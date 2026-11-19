package com.slg.game;

import com.google.protobuf.MessageLite;
import com.slg.core.auth.TokenService;
import com.slg.game.application.commit.*;
import com.slg.game.application.execution.OperationLimits;
import com.slg.game.application.march.MarchApplication;
import com.slg.game.application.march.MarchModule;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.*;
import com.slg.game.infrastructure.mysql.codec.*;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.WireStatus;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 通过实际GAME/Session RPC适配入口直达独立MySQL库，不启动生产进程或混用旧RocksDB。 */
@Timeout(45)
class MySqlPlayerRpcIT extends AuthoritySchemaFixture {
    @Test void authenticatedCreateReadAndReceiptUseAuthorityWhileUnmigratedGameplayIsRejected() throws Exception {
        try (var runtime = new Runtime(UnaryOperator.identity())) {
            Context bound = runtime.login("7");
            var missing = play(runtime, bound, GameplayRoutes.GET_PLAYER, "missing", GetPlayerReq.getDefaultInstance());
            assertEquals("PLAYER_NOT_FOUND", missing.getError().getCode());
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_journal"));
            var request = CreatePlayerReq.newBuilder().setName(" 城主 ").build();
            var created = CreatePlayerRsp.parseFrom(play(runtime, bound, GameplayRoutes.CREATE_PLAYER, "create", request).getBody());
            assertEquals("城主", created.getPlayer().getName()); assertEquals(3000, created.getPlayer().getTroopsOrThrow("INFANTRY"));
            assertEquals(0, created.getPlayer().getWoundedCount()); assertEquals(0, created.getPlayer().getDeadCount());
            assertEquals(created, CreatePlayerRsp.parseFrom(play(runtime, bound, GameplayRoutes.CREATE_PLAYER, "create", request).getBody()));
            assertEquals(created.getPlayer(), GetPlayerRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_PLAYER, "get", GetPlayerReq.getDefaultInstance()).getBody()).getPlayer());
            var receipt = GetReceiptRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_RECEIPT, "receipt",
                    GetReceiptReq.newBuilder().setTargetRequestId("create").build()).getBody());
            assertEquals(1, receipt.getJournalSeq()); assertEquals(GameplayRoutes.CREATE_PLAYER, receipt.getReceipt().getMsgId());
            assertEquals(created, receipt.getReceipt().getCreated());
            var foreign = GetReceiptRsp.parseFrom(play(runtime, runtime.login("8"), GameplayRoutes.GET_RECEIPT, "private",
                    GetReceiptReq.newBuilder().setTargetRequestId("create").build()).getBody());
            assertFalse(foreign.hasReceipt());
            var unsupported = play(runtime, bound, GameplayRoutes.GET_BATTLE, "not-migrated", GetBattleReq.getDefaultInstance());
            assertEquals(WireStatus.NOT_ACCEPTED, WireStatus.fromReply(unsupported));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_player"));
        }
    }

    @Test void marchRpcKeepsOriginalReceiptsAndArmyReservationAcrossStartQueryAndRecall() throws Exception {
        try (var runtime = new Runtime(UnaryOperator.identity())) {
            Context bound = runtime.login("7");
            play(runtime, bound, GameplayRoutes.CREATE_PLAYER, "create", CreatePlayerReq.newBuilder().setName("marcher").build());
            var order = StartMarchReq.newBuilder().setTargetX(650).setTargetY(650)
                    .addTroops(TroopGroup.newBuilder().setType(com.slg.rpc.v1.TroopType.TROOP_TYPE_INFANTRY).setCount(100)).build();
            var started = StartMarchRsp.parseFrom(play(runtime, bound, GameplayRoutes.START_MARCH, "start", order).getBody());
            String marchId = started.getMarch().getMarchId();
            assertEquals(com.slg.rpc.v1.MarchState.MARCH_STATE_OUTBOUND, started.getMarch().getState());
            var lookup = GetMarchReq.newBuilder().setMarchId(marchId).build();
            assertEquals(started.getMarch(), GetMarchRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_MARCH, "outbound", lookup).getBody()).getMarch());

            // 独立业务时钟确保召回仍在途中；数据库租约与会话验证继续使用真实各自时钟。
            runtime.marchClock.millis.addAndGet(1000);
            var recall = RecallMarchReq.newBuilder().setMarchId(marchId).build();
            var recalled = RecallMarchRsp.parseFrom(play(runtime, bound, GameplayRoutes.RECALL_MARCH, "recall", recall).getBody());
            assertEquals(com.slg.rpc.v1.MarchState.MARCH_STATE_RETURNING, recalled.getMarch().getState());
            assertEquals(recalled.getMarch(), GetMarchRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_MARCH, "returning", lookup).getBody()).getMarch());
            var receipt = GetReceiptRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_RECEIPT, "receipt-recall",
                    GetReceiptReq.newBuilder().setTargetRequestId("recall").build()).getBody());
            assertEquals(GameplayRoutes.RECALL_MARCH, receipt.getReceipt().getMsgId());
            assertEquals(recalled, receipt.getReceipt().getRecalled());
            assertEquals(recalled, RecallMarchRsp.parseFrom(play(runtime, bound, GameplayRoutes.RECALL_MARCH, "recall", recall).getBody()));
            // 回放不能把原 OUTBOUND 响应替换成当前 RETURNING，也不能再次扣兵或创建新任务。
            assertEquals(started, StartMarchRsp.parseFrom(play(runtime, bound, GameplayRoutes.START_MARCH, "start", order).getBody()));
            var original = GetReceiptRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_RECEIPT, "receipt-start",
                    GetReceiptReq.newBuilder().setTargetRequestId("start").build()).getBody());
            assertEquals(GameplayRoutes.START_MARCH, original.getReceipt().getMsgId());
            assertEquals(started, original.getReceipt().getStarted());
            var player = GetPlayerRsp.parseFrom(play(runtime, bound, GameplayRoutes.GET_PLAYER, "army", GetPlayerReq.getDefaultInstance()).getBody()).getPlayer();
            assertEquals(2900, player.getTroopsOrThrow("INFANTRY"));
            assertEquals(List.of(marchId), player.getActiveMarchIdsList());
            assertEquals(marchId, player.getHeroes(0).getAssignedMarch());
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_march"));
        }
    }

    @Test void sessionReplacementWaitsForStartedCommitAndPublicationEvenAfterNetworkCancellation() throws Exception {
        var committed = new CountDownLatch(1); var allowReply = new CountDownLatch(1);
        UnaryOperator<RealmTransaction> delayed = actual -> new RealmTransaction() {
            @Override public CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException {
                var result = actual.commit(writer, decision); committed.countDown();
                try { if (!allowReply.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Test gate timeout"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                return result;
            }
            @Override public CommitAttempt resolveAfterFence(WriterToken previous, WriterToken replacement, RequestKey request, String digest)
                    throws StorageAccessException { return actual.resolveAfterFence(previous, replacement, request, digest); }
        };
        try (var runtime = new Runtime(delayed)) {
            try {
                Context old = runtime.login("7");
                var createReply = new Reply<PlayReply>();
                try (var cancelled = old.withCancellation()) {
                    cancelled.run(() -> runtime.game.play(request(GameplayRoutes.CREATE_PLAYER, "create", CreatePlayerReq.newBuilder().setName("first").build()), createReply));
                    assertTrue(committed.await(5, TimeUnit.SECONDS)); cancelled.cancel(null);
                    var replacement = runtime.beginLogin("7");
                    Thread.sleep(100); assertFalse(replacement.reply.value.isDone(), "Open cannot pass an unreturned committed operation");
                    allowReply.countDown();
                    var binding = replacement.reply.value.get(10, TimeUnit.SECONDS);
                    Context next = replacement.context.withValue(RpcMetadata.SESSION, binding.getKey());
                    assertFalse(createReply.value.isDone(), "Cancelled RPC must not receive a late success");
                    assertEquals("SESSION_REPLACED", play(runtime, old, GameplayRoutes.CREATE_PLAYER, "old", CreatePlayerReq.newBuilder().setName("old").build()).getError().getCode());
                    var receipt = GetReceiptRsp.parseFrom(play(runtime, next, GameplayRoutes.GET_RECEIPT, "lookup",
                            GetReceiptReq.newBuilder().setTargetRequestId("create").build()).getBody());
                    assertEquals("first", receipt.getReceipt().getCreated().getPlayer().getName());
                    assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
                }
            } finally { allowReply.countDown(); }
        }
    }

    private static PlayRequest request(int type, String id, MessageLite body) {
        return PlayRequest.newBuilder().setMsgId(type).setRequestId(id).setBody(body.toByteString()).build();
    }
    private static PlayReply play(Runtime runtime, Context context, int type, String id, MessageLite body) throws Exception {
        var result = new Reply<PlayReply>(); context.run(() -> runtime.game.play(request(type, id, body), result));
        return result.value.get(10, TimeUnit.SECONDS);
    }
    private static final class Reply<T> implements StreamObserver<T> {
        final CompletableFuture<T> value = new CompletableFuture<>();
        @Override public void onNext(T item) { value.complete(item); }
        @Override public void onError(Throwable failure) { value.completeExceptionally(failure); }
        @Override public void onCompleted() { }
    }
    private record Login(Context context, Reply<SessionBinding> reply) { }

    private static final class MarchClock extends Clock {
        final AtomicLong millis = new AtomicLong(1_000_000L);
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(instant(), zone); }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis()); }
        @Override public long millis() { return millis.get(); }
    }

    private final class Runtime implements AutoCloseable {
        final MarchClock marchClock = new MarchClock();
        final SessionDirectory sessionsDirectory = new SessionDirectory(Clock.systemUTC(), 32);
        final TokenService tokens = new TokenService("mysql-player-rpc-test-secret-at-least-32", Clock.systemUTC());
        final MySqlRealmStorage storage = new MySqlRealmStorage(pool, 5, Duration.ofSeconds(30));
        final BoundedReadExecutor reads;
        final BoundedRealmCommitService commits;
        final PlayerApplication app;
        final GameRpcService game;
        final SessionRpcService controls;
        Runtime(UnaryOperator<RealmTransaction> transactions) throws Exception {
            var writer = storage.ownership().acquire(identity, UUID.randomUUID());
            var codec = new PlayerPayloadJsonCodec(); var sizer = new MySqlCommitPayloadSizer(new PersistenceCodec());
            var budget = new ReadBudget(1000, 1_000_000, Duration.ofSeconds(5));
            reads = new BoundedReadExecutor(new ReadExecutionLimits(2, 32, 100_000, 2, 32, Duration.ofSeconds(10)));
            var directory = new PlayerDirectory(identity, PlayerSnapshotLoader.using(identity, storage.states(), reads, budget),
                    new PlayerCacheLimits(32, 64, 16_000_000, 1_000_000, Duration.ofMinutes(1)), codec::measureSnapshot);
            commits = new BoundedRealmCommitService(writer, transactions.apply(storage.transactions()), storage.ownership(), sizer,
                    new CommitQueueLimits(32, 16_000_000, 2, 100_000, 2, 2, Duration.ofSeconds(5), Duration.ofMillis(10), Duration.ofSeconds(5)));
            var marchCodec = new MarchPayloadJsonCodec();
            var module = new MarchModule(identity, storage.states(), reads, budget, marchCodec, marchClock);
            app = new PlayerApplication(identity, directory, storage.receipts(), reads, budget, codec,
                    PlayerSessionAdapter.access(sessionsDirectory, () -> true), commits, sizer,
                    new OperationLimits(32, 32_000_000, 64, 2, 2, 2, 2, 100_000, Duration.ofSeconds(5)),
                    PlayerSessionAdapter.controlHandlers(sessionsDirectory, () -> true), List.of(module));
            game = new GameRpcService(tokens, app, () -> true, 5000, sessionsDirectory,
                    new MarchApplication(app, identity, marchCodec));
            controls = new SessionRpcService(tokens, app, sessionsDirectory, () -> true, 5000);
        }
        Login beginLogin(String account) throws Exception {
            Context context = Context.current().withValue(RpcMetadata.TOKEN, tokens.issue(account, Duration.ofHours(1)));
            var prepared = new Reply<SessionPreparation>();
            context.run(() -> controls.prepare(SessionPrepareRequest.getDefaultInstance(), prepared));
            var preparation = prepared.value.get(5, TimeUnit.SECONDS);
            var request = SessionOpenRequest.newBuilder().setGameInstance(preparation.getGameInstance())
                    .setExpectedGeneration(preparation.getObservedGeneration()).setTicketId(preparation.getTicketId())
                    .setAttemptId(UUID.randomUUID().toString()).build();
            var opened = new Reply<SessionBinding>(); context.run(() -> controls.open(request, opened));
            return new Login(context, opened);
        }
        Context login(String account) throws Exception {
            var pending = beginLogin(account);
            return pending.context.withValue(RpcMetadata.SESSION, pending.reply.value.get(10, TimeUnit.SECONDS).getKey());
        }
        @Override public void close() throws Exception {
            app.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            commits.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            reads.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            storage.ownership().release(commits.snapshot().writer()); sessionsDirectory.close();
        }
    }
}

package com.slg.game;

import com.google.protobuf.MessageLite;
import com.slg.core.auth.TokenService;
import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.*;
import com.slg.game.application.execution.OperationLimits;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import com.slg.game.infrastructure.mysql.codec.PlayerPayloadJsonCodec;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 有界应用/RPC 夹具：真实账号资格、发布、会话和编解码，仅权威端口使用内存替身。
 * 不证明 SQL 原子性、owner 屏障或进程恢复；这些边界由保留的真实 MySQL IT 验证。
 */
final class PlayerApplicationFixture implements AutoCloseable {
    final RealmIdentity realm = new RealmIdentity(1, UUID.randomUUID());
    final TokenService tokens = new TokenService("player-rpc-test-secret-at-least-32-bytes", Clock.systemUTC());
    final SessionDirectory sessions = new SessionDirectory(Clock.systemUTC(), 16);
    final String token = tokens.issue("101", Duration.ofMinutes(5));
    Context context;
    final FakeCommits commits = new FakeCommits(realm);
    final BoundedReadExecutor reads = new BoundedReadExecutor(new ReadExecutionLimits(2, 16, 64_000, 2, 14, Duration.ofSeconds(3)));
    final PlayerApplication application;
    final GameRpcService gameplay;
    final SessionRpcService controls;
    PlayerApplicationFixture() {
        var codec = new PlayerPayloadJsonCodec();
        var directory = new PlayerDirectory(realm, account -> CompletableFuture.completedFuture(commits.snapshot(account)),
                new PlayerCacheLimits(16, 32, 16 * 1024 * 1024, 1024 * 1024, Duration.ofMinutes(1)), codec::measureSnapshot);
        application = new PlayerApplication(realm, directory, (identity, key, budget) -> {
            var receipt = commits.receipts.get(key);
            return receipt == null ? new RealmReceiptReader.Observation.NotObserved() : new RealmReceiptReader.Observation.Found(receipt);
        }, reads, new ReadBudget(1000, 2 * 1024 * 1024, Duration.ofSeconds(2)), codec,
                PlayerSessionAdapter.access(sessions, () -> true), commits, ignored -> 4096,
                new OperationLimits(32, 64 * 1024 * 1024, 32, 2, 2, 2, 2, 4096, Duration.ofSeconds(5)),
                PlayerSessionAdapter.controlHandlers(sessions, () -> true));
        gameplay = new GameRpcService(tokens, application, () -> true, 5000, sessions);
        controls = new SessionRpcService(tokens, application, sessions, () -> true, 5000);
        var claims = tokens.verify(token);
        var initial = sessions.open(claims, SessionDirectoryTest.request(sessions, claims));
        context = Context.current().withValues(RpcMetadata.TOKEN, token, RpcMetadata.SESSION, initial.getKey());
    }
    Reply<PlayReply> play(int type, String id, MessageLite body) { return play(context, type, id, body); }
    Reply<PlayReply> play(Context active, int type, String id, MessageLite body) {
        var reply = new Reply<PlayReply>();
        var request = PlayRequest.newBuilder().setMsgId(type).setRequestId(id).setBody(body.toByteString()).build();
        active.run(() -> gameplay.play(request, reply)); return reply;
    }
    GetReceiptRsp receipt(String id, String target) throws Exception {
        var response = play(GameplayRoutes.GET_RECEIPT, id, GetReceiptReq.newBuilder().setTargetRequestId(target).build()).get();
        assertFalse(response.hasError(), () -> response.getError().toString());
        return GetReceiptRsp.parseFrom(response.getBody());
    }
    Reply<SessionBinding> open() {
        var reply = new Reply<SessionBinding>();
        var request = SessionDirectoryTest.request(sessions, tokens.verify(token));
        context.run(() -> controls.open(request, reply)); return reply;
    }
    @Override public void close() throws Exception {
        commits.automatic = true;
        for (var pending : commits.all) pending.settlement.complete(new CommitResult.NotCommitted(CommitResult.Reason.ROLLED_BACK));
        try { application.drain().toCompletableFuture().get(3, TimeUnit.SECONDS); }
        finally { reads.close(); sessions.close(); }
    }

    static final class FakeCommits implements CommitCoordinator {
        final RealmIdentity realm;
        final WriterToken writer;
        final ConcurrentMap<RequestKey, CommitReceipt> receipts = new ConcurrentHashMap<>();
        final Map<Long, PlayerSnapshot> snapshots = new ConcurrentHashMap<>();
        final BlockingQueue<Pending> submitted = new LinkedBlockingQueue<>(32);
        final List<Pending> all = new CopyOnWriteArrayList<>();
        final AtomicInteger submissions = new AtomicInteger();
        final AtomicLong sequence = new AtomicLong();
        volatile boolean automatic = true;
        FakeCommits(RealmIdentity realm) { this.realm = realm; writer = new WriterToken(realm, UUID.randomUUID(), 1); }
        PlayerSnapshot snapshot(long account) { return snapshots.getOrDefault(account, PlayerSnapshot.absent(realm, account, sequence.get())); }
        @Override public Optional<WriterToken> currentWriter() { return Optional.of(writer); }
        @Override public PublicationBarrier isolatePublication() { return () -> { }; }
        @Override public CompletionStage<Void> drain() { return CompletableFuture.completedFuture(null); }
        @Override public synchronized CommitHandle submit(CommitRequest request) {
            if (all.size() >= 32) throw new AssertionError("Fixture commit capacity exceeded");
            submissions.incrementAndGet();
            var pending = new Pending(request); all.add(pending); if (!submitted.offer(pending)) throw new AssertionError("Fixture commit capacity exceeded");
            if (automatic) pending.commit();
            return pending;
        }
        Pending next() throws Exception { return Objects.requireNonNull(submitted.poll(3, TimeUnit.SECONDS), "Missing commit submission"); }
        final class Pending implements CommitHandle {
            final CommitRequest request;
            final CompletableFuture<CommitResult> settlement = new CompletableFuture<>();
            Pending(CommitRequest request) { this.request = request; }
            @Override public RequestKey requestKey() { return request.decision().request(); }
            @Override public CommitPhase phase() { return settlement.isDone() ? CommitPhase.SETTLED : CommitPhase.EXECUTING; }
            @Override public CompletionStage<CommitResult> settlement() { return settlement.minimalCompletionStage(); }
            @Override public boolean cancelBeforeStart() { return false; }
            synchronized void commit() {
                if (settlement.isDone()) return;
                var decision = request.decision();
                var previous = receipts.get(decision.request());
                if (previous != null) {
                    settlement.complete(previous.digest().equals(decision.digest()) ? new CommitResult.Committed(previous, false)
                            : new CommitResult.NotCommitted(CommitResult.Reason.REQUEST_CONFLICT));
                    return;
                }
                long seq = sequence.incrementAndGet();
                long account = Long.parseLong(decision.request().scope().substring("account:".length()));
                var before = snapshot(account);
                var profile = before.profile();
                var resources = new EnumMap<ResourceType, VersionedState>(ResourceType.class); resources.putAll(before.resources());
                var troops = new EnumMap<TroopType, VersionedState>(TroopType.class); troops.putAll(before.troops());
                var heroes = new HashMap<>(before.heroes());
                for (var change : decision.changes()) {
                    var value = new VersionedState(change.expectedVersion() + 1, change.afterState());
                    switch (change.afterState()) {
                        case PlayerState state -> profile = Optional.of(value);
                        case ResourceState state -> resources.put(state.resourceType(), value);
                        case com.slg.game.domain.persistence.TroopState state -> troops.put(state.troopType(), value);
                        case HeroState state -> heroes.put(state.heroId(), value);
                        default -> throw new AssertionError("Unexpected creation state");
                    }
                }
                snapshots.put(account, new PlayerSnapshot(realm, account, seq, profile, resources, troops, heroes, before.activeMarches()));
                var receipt = new CommitReceipt(decision.request(), seq, decision.digest(), decision.resultJson());
                receipts.put(receipt.request(), receipt);
                settlement.complete(new CommitResult.Committed(receipt, true));
            }
        }
    }

    static final class Reply<T> implements StreamObserver<T> {
        final CompletableFuture<T> value = new CompletableFuture<>();
        @Override public void onNext(T response) { value.complete(response); }
        @Override public void onError(Throwable failure) { value.completeExceptionally(failure); }
        @Override public void onCompleted() { }
        T get() throws Exception { return value.get(3, TimeUnit.SECONDS); }
    }
}

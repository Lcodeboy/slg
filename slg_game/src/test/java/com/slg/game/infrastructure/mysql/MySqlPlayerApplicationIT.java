package com.slg.game.infrastructure.mysql;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.*;
import com.slg.game.application.execution.OperationLimits;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.*;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.*;
import com.slg.game.infrastructure.mysql.codec.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 真MySQL验证新用例到事务、缓存和回执的完整路径；不启动生产组合根或使用开发库。 */
@Timeout(45)
class MySqlPlayerApplicationIT extends AuthoritySchemaFixture {
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test void createsSixRootsAtomicallyAndReadsWithoutGrowingJournal() throws Exception {
        try (var runtime = new Runtime()) {
            var committed = committed(runtime.app.createPlayer(actor(7), "create", "  城主  ", WAIT));
            assertTrue(committed.applied()); assertEquals(1, committed.receipt().sequence());
            var original = runtime.app.outcome(committed.receipt()).player();
            assertEquals("城主", original.profile().name()); assertEquals("1_7", original.profile().playerId());
            assertEquals(3000, original.troops().get(TroopType.INFANTRY).available());
            assertEquals(Map.of(ResourceType.WOOD, 0L), original.resources());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_player"));
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_player_troop"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_player_resource"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_player_hero"));
            assertEquals(original, result(runtime.app.getPlayer(actor(7), "get", WAIT)));
            var receipt = assertInstanceOf(RealmReceiptReader.Observation.Found.class,
                    result(runtime.app.getReceipt(actor(7), "get-receipt", "create", WAIT)));
            assertEquals(committed.receipt(), receipt.receipt());
            assertEquals(1, scalar("SELECT high_seq FROM realm_commit_cursor WHERE realm_id=1"));
        }
    }

    @Test void sameRequestReplaysOriginalResponseAndChangedInputConflicts() throws Exception {
        try (var runtime = new Runtime()) {
            var original = committed(runtime.app.createPlayer(actor(7), "create", "first", WAIT));
            var replay = committed(runtime.app.createPlayer(actor(7), "create", "first", WAIT));
            assertFalse(replay.applied()); assertEquals(original.receipt(), replay.receipt());
            var conflict = assertInstanceOf(CommitResult.NotCommitted.class,
                    result(runtime.app.createPlayer(actor(7), "create", "second", WAIT)));
            assertEquals(CommitResult.Reason.REQUEST_CONFLICT, conflict.reason());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
        }
    }

    @Test void persistedRejectionsStayRejectedAfterLaterCreationAndReceiptScopeIsPrivate() throws Exception {
        try (var runtime = new Runtime()) {
            var invalid = committed(runtime.app.createPlayer(actor(7), "bad", "   ", WAIT));
            assertEquals("INVALID_ARGUMENT", runtime.app.outcome(invalid.receipt()).errorCode());
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_player"));
            committed(runtime.app.createPlayer(actor(7), "good", "first", WAIT));
            assertEquals(invalid.receipt(), committed(runtime.app.createPlayer(actor(7), "bad", "   ", WAIT)).receipt());
            var exists = committed(runtime.app.createPlayer(actor(7), "again", "second", WAIT));
            assertEquals("PLAYER_EXISTS", runtime.app.outcome(exists.receipt()).errorCode());
            assertInstanceOf(RealmReceiptReader.Observation.NotObserved.class,
                    result(runtime.app.getReceipt(actor(8), "private", "good", WAIT)));
            assertEquals(3, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_player"));
        }
    }

    @Test void duplicateConcurrentCreationSharesAccountOrderAndDoesNotIssueInitialAssetsTwice() throws Exception {
        try (var runtime = new Runtime()) {
            var first = runtime.app.createPlayer(actor(7), "same", "城主", WAIT);
            var duplicate = runtime.app.createPlayer(actor(7), "same", "城主", WAIT);
            var queried = runtime.app.getPlayer(actor(7), "query", WAIT);
            assertEquals(committed(first).receipt(), committed(duplicate).receipt());
            assertEquals(3000, result(queried).troops().get(TroopType.INFANTRY).available());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
        }
    }

    @Test void freshRuntimeColdLoadsExistingPlayerAndRetainsItsFoundingIdentity() throws Exception {
        try (var runtime = new Runtime()) {
            var founder = new RealmIdentity(2, UUID.randomUUID());
            List<ObjectChange> roots = PlayerCreation.initialStates(founder, 7, "迁入角色").stream().map(state -> {
                ObjectState adjusted = state instanceof PlayerState player
                        ? new PlayerState(player.playerId(), player.accountId(), 2, 1, player.name()) : state;
                return new ObjectChange(adjusted.key(), 0, adjusted);
            }).toList();
            seed(runtime, roots);
            var player = result(runtime.app.getPlayer(actor(7), "read", WAIT));
            assertEquals("2_7", player.profile().playerId()); assertEquals(1, player.profile().currentRealmId());
            assertEquals("PLAYER_EXISTS", runtime.app.outcome(committed(runtime.app.createPlayer(actor(7), "exists", "new", WAIT)).receipt()).errorCode());
        }
        try (var restarted = new Runtime()) {
            assertEquals("2_7", result(restarted.app.getPlayer(actor(7), "reload", WAIT)).profile().playerId());
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_journal"));
        }
    }

    @Test void orphanStateAndMissingRequiredRowsFailInsteadOfRefillingAssets() throws Exception {
        try (var runtime = new Runtime()) {
            var orphan = new ResourceState("1_7", ResourceType.WOOD, 999);
            seed(runtime, List.of(new ObjectChange(orphan.key(), 0, orphan)));
            var failure = assertThrows(ExecutionException.class, () -> result(runtime.app.getPlayer(actor(7), "broken", WAIT)));
            assertFalse(rootCause(failure) instanceof GameException, "Bad data must not be reported as an absent character");
            assertInstanceOf(CommitResult.NotCommitted.class, result(runtime.app.createPlayer(actor(7), "no-refill", "name", WAIT)));
            assertEquals(999, scalar("SELECT amount FROM game_player_resource WHERE player_id='1_7'"));
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_player"));
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
        }
    }

    @Test void coldLoadFailureDoesNotCreateARejectionReceiptOrPublishPartialPlayer() throws Exception {
        try (var runtime = new Runtime(UnaryOperator.identity(), ignored -> account -> CompletableFuture.failedFuture(
                new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "Injected unavailable player read")), null)) {
            assertInstanceOf(CommitResult.NotCommitted.class, result(runtime.app.createPlayer(actor(7), "unavailable", "name", WAIT)));
            assertThrows(ExecutionException.class, () -> result(runtime.app.getPlayer(actor(7), "query", WAIT)));
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(0, scalar("SELECT COUNT(*) FROM game_player"));
        }
    }

    @Test void unknownCommitRetainsCachePinDespiteExternalCancellationAndDrain() throws Exception {
        var resolving = new CountDownLatch(1); var allowResolution = new CountDownLatch(1);
        UnaryOperator<RealmTransaction> uncertain = actual -> new RealmTransaction() {
            @Override public CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException {
                actual.commit(writer, decision); return new CommitAttempt.Uncertain();
            }
            @Override public CommitAttempt resolveAfterFence(WriterToken previous, WriterToken replacement, RequestKey request, String digest)
                    throws StorageAccessException {
                resolving.countDown(); await(allowResolution);
                return actual.resolveAfterFence(previous, replacement, request, digest);
            }
        };
        try (var runtime = new Runtime(uncertain, UnaryOperator.identity(), null)) {
            var create = runtime.app.createPlayer(actor(7), "unknown", "name", WAIT);
            assertTrue(resolving.await(5, TimeUnit.SECONDS));
            assertTrue(create.result().toCompletableFuture().cancel(false));
            assertEquals(1, runtime.directory.statistics().pins()); assertEquals(0, runtime.directory.evictIdle());
            var draining = runtime.app.drain().toCompletableFuture(); assertFalse(draining.isDone());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_player"));
            allowResolution.countDown();
            assertFalse(committed(create).applied()); draining.get(10, TimeUnit.SECONDS);
            assertEquals(0, runtime.directory.statistics().pins());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
        } finally { allowResolution.countDown(); }
    }

    @Test void publicationAndReloadFailureKeepKnownCommitIsolatedUntilExplicitRepair() throws Exception {
        var block = new AtomicBoolean(true); var codec = new PlayerPayloadJsonCodec();
        ToLongFunction<PlayerSnapshot> failPublishedState = snapshot -> {
            if (block.get() && snapshot.profile().isPresent()) throw new IllegalStateException("Injected publication budget failure");
            return codec.measureSnapshot(snapshot);
        };
        try (var runtime = new Runtime(UnaryOperator.identity(), UnaryOperator.identity(), failPublishedState)) {
            var create = runtime.app.createPlayer(actor(7), "repair", "name", WAIT);
            await(() -> runtime.app.stats().isolated() == 1);
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(1, runtime.directory.statistics().pins()); assertFalse(create.result().toCompletableFuture().isDone());
            assertEquals(CommitQueueSnapshot.Mode.ISOLATED, runtime.commits.snapshot().mode());
            block.set(false); assertTrue(runtime.app.retryRepair(7, "repair"));
            assertTrue(committed(create).applied());
            assertEquals("name", result(runtime.app.getPlayer(actor(7), "after-repair", WAIT)).profile().name());
            assertEquals(1, scalar("SELECT COUNT(*) FROM game_journal"));
        } finally { block.set(false); }
    }

    @Test void corruptedReceiptCannotReplayOrExposeAnotherAccountOrRealm() throws Exception {
        try (var runtime = new Runtime()) {
            var own = committed(runtime.app.createPlayer(actor(7), "create", "owner", WAIT)).receipt();
            var foreign = committed(runtime.app.createPlayer(actor(8), "create", "private-name", WAIT)).receipt();
            // 仅改回执原响应，保留正确 key/digest/sequence；验证内部合法的 JSON 仍须与查询身份一致。
            assertEquals(1, execute("UPDATE game_receipt SET result_json=? WHERE realm_id=1 AND scope_id=? AND request_id=?",
                    foreign.resultJson().getBytes(java.nio.charset.StandardCharsets.UTF_8), "account:7", "create"));
            var queryFailure = assertThrows(ExecutionException.class,
                    () -> result(runtime.app.getReceipt(actor(7), "receipt", "create", WAIT)));
            var invalid = assertInstanceOf(IllegalArgumentException.class, rootCause(queryFailure));
            assertFalse(invalid.getMessage().contains("private-name"));
            var replay = assertInstanceOf(CommitResult.NotCommitted.class,
                    result(runtime.app.createPlayer(actor(7), "create", "owner", WAIT)));
            assertEquals(CommitResult.Reason.INVALID_DECISION, replay.reason());
            assertThrows(IllegalArgumentException.class, () -> runtime.app.outcome(
                    new CommitReceipt(own.request(), own.sequence(), own.digest(), foreign.resultJson())));
            assertThrows(IllegalArgumentException.class, () -> runtime.app.outcome(
                    new CommitReceipt(new RequestKey("account:07", "create"), own.sequence(), own.digest(), own.resultJson())));

            var codec = new PlayerPayloadJsonCodec();
            var otherRealm = PlayerDetails.created(PlayerCreation.initialStates(new RealmIdentity(2, UUID.randomUUID()), 7, "owner"));
            assertThrows(IllegalArgumentException.class, () -> runtime.app.outcome(new CommitReceipt(
                    own.request(), own.sequence(), own.digest(), codec.encodeOutcome(PlayerOutcome.created(otherRealm)))));
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_journal"));
            assertEquals(2, scalar("SELECT COUNT(*) FROM game_player"));
        }
    }

    private void seed(Runtime runtime, List<ObjectChange> changes) throws Exception {
        var decision = new CommitDecision(new RequestKey("fixture", "seed"), "seed-digest", "FIXTURE", "{}", "{}", changes, Map.of());
        var attempt = assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, runtime.storage.transactions().commit(runtime.writer, decision));
        assertInstanceOf(CommitResult.Committed.class, attempt.result());
    }
    private static PlayerAccess actor(long account) {
        return new PlayerAccess(account, 1, Long.MAX_VALUE, "00000000-0000-0000-0000-000000000001", 1,
                "00000000-0000-0000-0000-000000000002");
    }
    private static <T> T result(PlayerCall<T> call) throws Exception { return call.result().toCompletableFuture().get(10, TimeUnit.SECONDS); }
    private static CommitResult.Committed committed(PlayerCall<CommitResult> call) throws Exception {
        return assertInstanceOf(CommitResult.Committed.class, result(call));
    }
    private static Throwable rootCause(Throwable failure) { while (failure.getCause() != null) failure = failure.getCause(); return failure; }
    private static void await(CountDownLatch latch) throws StorageAccessException {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Test gate timeout"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "Test interrupted"); }
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for player lifecycle");
    }

    private final class Runtime implements AutoCloseable {
        final MySqlRealmStorage storage = new MySqlRealmStorage(pool, 5, Duration.ofSeconds(30));
        final WriterToken writer;
        final BoundedReadExecutor reads;
        final BoundedRealmCommitService commits;
        final PlayerDirectory directory;
        final PlayerApplication app;
        Runtime() throws Exception { this(UnaryOperator.identity(), UnaryOperator.identity(), null); }
        Runtime(UnaryOperator<RealmTransaction> transactions, UnaryOperator<PlayerSnapshotLoader> loaders,
                ToLongFunction<PlayerSnapshot> meter) throws Exception {
            writer = storage.ownership().acquire(identity, UUID.randomUUID());
            var codec = new PlayerPayloadJsonCodec(); var sizer = new MySqlCommitPayloadSizer(new PersistenceCodec());
            var budget = new ReadBudget(1000, 1_000_000, Duration.ofSeconds(5));
            reads = new BoundedReadExecutor(new ReadExecutionLimits(2, 32, 100_000, 2, 32, Duration.ofSeconds(10)));
            directory = new PlayerDirectory(identity, loaders.apply(PlayerSnapshotLoader.using(identity, storage.states(), reads, budget)),
                    new PlayerCacheLimits(32, 64, 16_000_000, 1_000_000, Duration.ofMillis(1)), meter == null ? codec::measureSnapshot : meter);
            commits = new BoundedRealmCommitService(writer, transactions.apply(storage.transactions()), storage.ownership(), sizer,
                    new CommitQueueLimits(32, 16_000_000, 2, 100_000, 2, 2, WAIT, Duration.ofMillis(10), Duration.ofSeconds(5)));
            app = new PlayerApplication(identity, directory, storage.receipts(), reads, budget, codec, ignored -> true, commits, sizer,
                    new OperationLimits(32, 32_000_000, 64, 2, 2, 2, 2, 100_000, WAIT), Map.of());
        }
        @Override public void close() throws Exception {
            app.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            commits.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            reads.drain().toCompletableFuture().get(10, TimeUnit.SECONDS);
            storage.ownership().release(commits.snapshot().writer());
        }
    }
}

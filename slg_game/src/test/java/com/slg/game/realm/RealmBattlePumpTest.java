package com.slg.game.realm;

import com.slg.rpc.v1.BattleReportState;
import com.slg.rpc.v1.MarchMission;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.core.battle.ExpeditionBattleEngine;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmVolume;
import io.grpc.Status;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class RealmBattlePumpTest {
    @TempDir Path path;
    private final ExpeditionBattleEngine engine = new ExpeditionBattleEngine();

    private record Dispatch(BattleInput input, String hash, String rules, CompletableFuture<BattleResult> reply) {}

    @Test void heldRpcLeavesMailboxFreeAndRetriesTheOriginalSnapshotWithinCapacity() throws Exception {
        var id = UUID.randomUUID();
        RealmVolume.initialize(path, 1, id);
        try (var volume = RealmVolume.open(path, 1, id)) {
            var journal = volume.journal();
            RealmMarchService game = prepareTwoNpcArrivals(journal);
            long before = journal.highSequence();
            var calls = new LinkedBlockingQueue<Dispatch>();
            try (var players = new ActivePlayers(game::loadPlayer, 2, 1, 16, 4, 16, Duration.ofMinutes(1));
                 var pump = new RealmBattlePump(journal, players, game, (input, hash, rules) -> {
                     var reply = new CompletableFuture<BattleResult>();
                     calls.add(new Dispatch(input, hash, rules, reply));
                     return reply;
                 }, 1)) {
                pump.start();
                Dispatch first = next(calls);
                String account = PlayerIdentity.accountOf(first.input().attacker().ownerId());
                String battleId = first.input().battleId();
                // 在另一场战斗也待派发时，等待多个轮询周期，确认只占一个在途名额。
                assertNull(calls.poll(350, TimeUnit.MILLISECONDS));
                assertEquals(1, pump.metrics().get("inFlight"));
                var read = players.submit(account, deadline(), active ->
                        game.getBattle(active, "read-pending", battleById(battleId)));
                var pending = read.get(3, TimeUnit.SECONDS);
                assertEquals(BattleReportState.REPORT_PENDING, pending.getState());

                first.reply().completeExceptionally(Status.DEADLINE_EXCEEDED.asRuntimeException());
                // 游标继续扫描另一玩家，失败的记录保留在待办索引，绕回后再派原快照。
                Dispatch other = next(calls);
                assertNotEquals(battleId, other.input().battleId());
                other.reply().complete(engine.simulate(other.input()));
                Dispatch retry = next(calls);
                assertEquals(first.input(), retry.input());
                assertEquals(first.hash(), retry.hash());
                assertEquals(first.rules(), retry.rules());

                // 已返回的战果也要等同账号的在途命令释放邮箱，不能直接由派发线程结算。
                var entered = new CountDownLatch(1);
                var release = new CountDownLatch(1);
                var preceding = players.submit(account, deadline(), active -> {
                    entered.countDown();
                    return release.await(5, TimeUnit.SECONDS);
                });
                try {
                    assertTrue(entered.await(3, TimeUnit.SECONDS));
                    retry.reply().complete(engine.simulate(retry.input()));
                    await(() -> players.metrics().get("outstanding").equals(2));
                    assertEquals("PENDING", Json.MAPPER.readTree(journal.read("battle:" + battleId).json())
                            .path("state").asText());
                } finally { release.countDown(); }
                assertTrue(preceding.get(3, TimeUnit.SECONDS));
                await(() -> pump.metrics().get("settled").equals(2L));
                assertEquals(3L, pump.metrics().get("submitted"));
                assertEquals(1L, pump.metrics().get("failedAttempts"));
                assertEquals(before + 2, journal.highSequence());
                assertTrue(journal.pendingBattles(null, 4).isEmpty());
                for (var call : List.of(first, other)) {
                    var job = Json.MAPPER.readTree(journal.read("battle:" + call.input().battleId()).json());
                    assertEquals("SETTLED", job.path("state").asText());
                    assertEquals(engine.simulate(call.input()), Json.MAPPER.treeToValue(job.path("result"), BattleResult.class));
                }
            }
        }
    }

    private static RealmMarchService prepareTwoNpcArrivals(RealmJournal journal) throws Exception {
        var dispatch = serviceAt(journal, 1000);
        for (String player : List.of("101", "102")) {
            var active = dispatch.loadPlayer(player);
            dispatch.createPlayer(active, "create", named(player));
            var attack = gather(120).toBuilder().setMission(MarchMission.MARCH_MISSION_ATTACK_NPC)
                    .setTargetX(120).setTargetY(120).build();
            dispatch.startMarch(active, "march", attack);
        }
        var arrivals = journal.dueTasks(Long.MAX_VALUE, null, 4);
        assertEquals(2, arrivals.size());
        long lastArrival = arrivals.getLast().dueAt();
        var game = serviceAt(journal, lastArrival);
        for (var arrival : arrivals) assertTrue(game.applyDue(game.loadPlayer(PlayerIdentity.accountOf(arrival.playerId())), arrival));
        assertEquals(2, journal.pendingBattles(null, 4).size());
        return game;
    }

    private static RealmMarchService serviceAt(RealmJournal journal, long millis) throws java.io.IOException {
        return new RealmMarchService(journal, Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC));
    }

    private static Dispatch next(BlockingQueue<Dispatch> calls) throws InterruptedException {
        Dispatch call = calls.poll(5, TimeUnit.SECONDS);
        assertNotNull(call, "Timed out waiting for battle dispatch");
        return call;
    }

    private static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(3); }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long end = deadline();
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for battle completion");
    }
}

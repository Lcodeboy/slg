package com.slg.game.realm;

import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.MarchMission;
import com.slg.rpc.v1.MarchView;
import com.slg.rpc.v1.PlayerView;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.DueTask;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class RealmArrivalTest {
    private static final int REALM = 9;
    private static final String OWNER = "101";
    @TempDir Path path;
    private final UUID volumeId = UUID.randomUUID();
    private final GameClock clock = new GameClock();

    @Test void duplicateWakesAdvanceEachStageOnceAndCargoIsDeliveredOnlyOnReturn() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = directory(service, 32, 16, 4)) {
                seed(players, service, OWNER);
                var march = depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150);
                var arrival = singleScheduled(journal);
                String marchId = march.getMarchId();
                assertEquals(march.getArriveAt(), arrival.dueAt());
                assertEquals(marchId, arrival.marchId());
                assertTrue(journal.dueTasks(clock.millis(), null, 16).isEmpty());
                assertFalse(wake(players, service, arrival).get(5, TimeUnit.SECONDS), "too early");
                assertEquals(2, journal.highSequence());

                clock.at(arrival.dueAt());
                DueTask stale = new DueTask(arrival.taskId(), arrival.playerId(), marchId, arrival.type(),
                        arrival.generation() + 1, arrival.dueAt());
                assertFalse(wake(players, service, stale).get(5, TimeUnit.SECONDS));
                onceOnly(players, service, arrival, journal);
                assertEquals("GATHERING", state(journal, marchId));
                DueTask gather = singleScheduled(journal);
                assertEquals(2, gather.generation());
                assertEquals(arrival.dueAt() + 3_000L, gather.dueAt());
                assertEquals(0, wood(player(players, service, OWNER)));

                clock.at(gather.dueAt());
                onceOnly(players, service, gather, journal);
                assertEquals("RETURNING", state(journal, marchId));
                assertEquals(600, record(journal, "march:" + marchId).at("/cargo/WOOD").asLong());
                assertEquals(0, wood(player(players, service, OWNER)));
                DueTask returning = singleScheduled(journal);
                assertEquals(3, returning.generation());
                assertEquals(gather.dueAt() + arrival.dueAt() - march.getDepartAt(), returning.dueAt());

                clock.at(returning.dueAt());
                onceOnly(players, service, returning, journal);
                assertTrue(journal.dueTasks(Long.MAX_VALUE, null, 16).isEmpty());
                assertReturned(player(players, service, OWNER), 600);
                assertEquals("COMPLETED", state(journal, marchId));
                assertEquals(march, depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150));
                assertEquals(5, journal.highSequence());
            }
        }
    }

    @Test void restartWhileOfflineCatchesUpAllStagesFromTheirScheduledTimes() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        MarchView original;
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var service = new RealmMarchService(volume.journal(), clock);
            try (var players = directory(service, 8, 4, 1)) {
                seed(players, service, OWNER);
                original = depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150);
                seed(players, service, "102"); // 始发玩家离开驻留目录，行军和任务仍在原卷中。
                assertTrue(((Number) players.metrics().get("evictions")).longValue() > 0);
            }
        }

        // GAME 停止期间同时跨过到达、采集完成与返城。重启后不开客户端连接，不推进游戏时钟。
        clock.at(original.getArriveAt() + 60_000L);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            var loads = new AtomicInteger();
            try (var players = new ActivePlayers(id -> {
                if (id.equals(OWNER)) loads.incrementAndGet();
                return service.loadPlayer(id);
            }, 1, 1, 16, 8, 1, Duration.ofMinutes(1));
                 var pump = new RealmTaskPump(journal, players, service, clock, 2)) {
                drive(pump, () -> "COMPLETED".equals(state(journal, original.getMarchId())));
                assertEquals(1, loads.get(), "timer should activate the offline player without client traffic");
                assertEquals(6, journal.highSequence()); // 两个建角，一次出征，三个时间阶段
                assertTrue(journal.dueTasks(clock.millis(), null, 16).isEmpty());
                assertReturned(player(players, service, OWNER), 600);
                assertEquals(original, depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150));
                assertEquals(6, journal.highSequence());
            }
        }
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            assertTrue(journal.dueTasks(clock.millis(), null, 16).isEmpty());
            assertEquals(6, journal.highSequence());
            var game = new RealmMarchService(journal, clock);
            assertReturned(game.getPlayer(game.loadPlayer(OWNER), "read", GetPlayerReq.getDefaultInstance()).getPlayer(), 600);
        }
    }

    @Test void recallSupersedesAnOldArrivalAndCanOnlyReturnTheTroopsOnce() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = directory(service, 16, 8, 4)) {
                seed(players, service, OWNER);
                var march = depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150);
                DueTask obsolete = singleScheduled(journal);
                clock.at(march.getDepartAt() + 1_000);
                var recall = recallOrder(obsolete.marchId());
                var response = command(players, OWNER, p -> service.recallMarch(p, "recall-1", recall));
                assertEquals("RETURNING", state(journal, obsolete.marchId()));
                DueTask returning = singleScheduled(journal);
                assertNotEquals(obsolete.taskId(), returning.taskId());
                clock.at(Math.max(obsolete.dueAt(), returning.dueAt()));
                assertFalse(wake(players, service, obsolete).get(5, TimeUnit.SECONDS));
                onceOnly(players, service, returning, journal);
                assertEquals(response, command(players, OWNER, p -> service.recallMarch(p, "recall-1", recall)));
                assertEquals("COMPLETED", state(journal, obsolete.marchId()));
                assertReturned(player(players, service, OWNER), 0);
                assertTrue(journal.dueTasks(clock.millis(), null, 16).isEmpty());
            }
        }
    }

    @Test void npcArrivalPersistsOneBattlePreparationAndDoesNotReleaseAttackingTroops() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = directory(service, 16, 8, 4)) {
                seed(players, service, OWNER);
                MarchView original = depart(players, service, OWNER, MarchMission.MARCH_MISSION_ATTACK_NPC, 120);
                DueTask arrival = singleScheduled(journal);
                clock.at(arrival.dueAt());
                // 已过到达时点但定时线程尚未投递：召回不能绕过应先发生的战斗准备。
                var recall = recallOrder(arrival.marchId());
                assertEquals("INVALID_MARCH_STATE", command(players, OWNER,
                        p -> rejection(() -> service.recallMarch(p, "late-recall", recall))));
                assertEquals("BATTLE_PENDING", state(journal, arrival.marchId()));
                var battle = record(journal, "battle:" + arrival.marchId());
                assertEquals("PENDING", battle.path("state").asText());
                assertEquals(arrival.marchId(), battle.path("battleId").asText());
                assertEquals(REALM + "_" + OWNER, battle.at("/input/attacker/ownerId").asText());
                assertEquals(120L, battle.at("/input/attacker/groups/0/count").asLong());
                assertEquals("npc", battle.at("/input/defender/ownerId").asText());
                assertTrue(battle.at("/input/seed").isIntegralNumber());
                assertTrue(battle.path("inputHash").asText().matches("[0-9a-f]{64}"));
                assertFalse(battle.path("rulesVersion").asText().isBlank());
                long committed = journal.highSequence();
                assertFalse(wake(players, service, arrival).get(5, TimeUnit.SECONDS));
                assertEquals("INVALID_MARCH_STATE", command(players, OWNER,
                        p -> rejection(() -> service.recallMarch(p, "late-recall", recall))));
                assertEquals(committed, journal.highSequence());
                assertEquals(battle, record(journal, "battle:" + arrival.marchId()));
                assertTrue(journal.dueTasks(clock.millis(), null, 16).isEmpty());
                var player = player(players, service, OWNER);
                assertEquals(2880, player.getTroopsOrThrow("INFANTRY"));
                assertEquals(original.getMarchId(), player.getHeroes(0).getAssignedMarch());
            }
        }
    }

    @Test void oneSaturatedOwnerCannotHideAnotherOwnersDueTask() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = new ActivePlayers(service::loadPlayer, 2, 2, 16, 1, 4, Duration.ofMinutes(1));
                 var pump = new RealmTaskPump(journal, players, service, clock, 1)) {
                seed(players, service, OWNER);
                String first = depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150).getMarchId();
                seed(players, service, "102");
                var second = depart(players, service, "102", MarchMission.MARCH_MISSION_GATHER, 650);
                clock.at(second.getArriveAt());
                var occupied = new CountDownLatch(1);
                var release = new CountDownLatch(1);
                var holding = players.submit(OWNER, deadline(), player -> {
                    occupied.countDown();
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                    return null;
                });
                try {
                    assertTrue(occupied.await(5, TimeUnit.SECONDS));
                    drive(pump, () -> "GATHERING".equals(state(journal, second.getMarchId())));
                    assertEquals("OUTBOUND", state(journal, first), "the earlier owner is still busy");
                } finally {
                    release.countDown();
                }
                holding.get(5, TimeUnit.SECONDS);
                drive(pump, () -> !"OUTBOUND".equals(state(journal, first)));
            }
        }
    }

    @Test void aCurrentTaskWithImpossibleMarchStateIsQuarantinedWithoutTouchingAssets() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = directory(service, 8, 4, 4)) {
                seed(players, service, OWNER);
                depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150);
                DueTask arrival = singleScheduled(journal);
                String key = "march:" + arrival.marchId();
                var before = journal.read(key);
                var impossible = (ObjectNode) Json.MAPPER.readTree(before.json());
                impossible.put("state", "COMPLETED");
                // 构造仍为当前 PENDING、却指向非法状态的持久数据。重试不能反复堵住到期索引头。
                journal.commit(new RealmJournal.Decision("fixture", "inconsistent-stage", "test", "TEST",
                        "{}", "{}", List.of(new RealmJournal.Change(key, before.version(), impossible.toString()))));
                var playerBefore = player(players, service, OWNER);
                clock.at(arrival.dueAt());
                wake(players, service, arrival).get(5, TimeUnit.SECONDS);
                assertEquals("BLOCKED", record(journal, "task:" + arrival.taskId()).path("state").asText());
                assertTrue(journal.dueTasks(Long.MAX_VALUE, null, 16).isEmpty());
                assertEquals(playerBefore, player(players, service, OWNER));
                long quarantined = journal.highSequence();
                assertFalse(wake(players, service, arrival).get(5, TimeUnit.SECONDS));
                assertEquals(quarantined, journal.highSequence());
            }
        }
    }

    @Test void shutdownDuringOfflineActivationLeavesUnexecutedTaskOnTheVolume() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        MarchView original;
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var initial = directory(service, 8, 4, 1)) {
                seed(initial, service, OWNER);
                original = depart(initial, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150);
            }
            clock.at(original.getArriveAt());
            var loading = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            try (var players = new ActivePlayers(id -> {
                loading.countDown();
                assertTrue(release.await(10, TimeUnit.SECONDS));
                return service.loadPlayer(id);
            }, 1, 1, 8, 4, 1, Duration.ofMinutes(1));
                 var pump = new RealmTaskPump(journal, players, service, clock, 1);
                 var closer = Executors.newSingleThreadExecutor()) {
                try {
                    assertEquals(1, pump.poll());
                    assertTrue(loading.await(5, TimeUnit.SECONDS));
                    pump.close();
                    assertEquals(0, pump.poll());
                    var closed = closer.submit(players::close);
                    long deadline = deadline();
                    while (!Boolean.TRUE.equals(players.metrics().get("closed")) && System.nanoTime() < deadline) {
                        TimeUnit.MILLISECONDS.sleep(5);
                    }
                    assertEquals(true, players.metrics().get("closed"));
                    assertFalse(closed.isDone());
                    release.countDown();
                    closed.get(5, TimeUnit.SECONDS);
                } finally {
                    release.countDown();
                }
                assertEquals(2, journal.highSequence());
                assertEquals("PENDING", record(journal, "task:" + original.getTaskId()).path("state").asText());
            }
        }
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = directory(service, 8, 4, 1);
                 var pump = new RealmTaskPump(journal, players, service, clock, 1)) {
                drive(pump, () -> "GATHERING".equals(state(journal, original.getMarchId())));
                assertEquals(3, journal.highSequence());
            }
        }
    }

    @Test void newArrivalsCannotKeepADeferredOwnerBehindAMovingScanHorizon() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            try (var players = new ActivePlayers(service::loadPlayer, 2, 2, 16, 1, 4, Duration.ofMinutes(1));
                 var pump = new RealmTaskPump(journal, players, service, clock, 1)) {
                long start = clock.millis();
                seed(players, service, OWNER);
                var a = depart(players, service, OWNER, MarchMission.MARCH_MISSION_GATHER, 150);
                for (int n = 1; n <= 2; n++) {
                    clock.at(start + n * 1000);
                    String owner = Integer.toString(200 + n);
                    seed(players, service, owner);
                    depart(players, service, owner, MarchMission.MARCH_MISSION_GATHER, 150);
                }
                long due = a.getArriveAt();
                clock.at(due);
                var holding = new CountDownLatch(1);
                var release = new CountDownLatch(1);
                var busy = players.submit(OWNER, deadline(), player -> {
                    holding.countDown();
                    return release.await(5, TimeUnit.SECONDS);
                });
                try {
                    assertTrue(holding.await(5, TimeUnit.SECONDS));
                    pump.poll(); // 第一个到期任务暂缓；后面两个玩家还没到达。
                } finally { release.countDown(); }
                assertTrue(busy.get(5, TimeUnit.SECONDS));
                // 后两次翻页时各多一个新到期玩家。第一个玩家仍应在下一轮首先重试。
                clock.at(due + 1000); pump.poll();
                clock.at(due + 2000); pump.poll();
                String marchId = a.getMarchId();
                long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                // 验证这个受限玩家最终获准执行到达；哪个玩家先拿矿是另一条规则。
                while ("OUTBOUND".equals(state(journal, marchId)) && System.nanoTime() < timeout)
                    TimeUnit.MILLISECONDS.sleep(5);
                assertEquals("DONE", record(journal, "task:" + a.getTaskId()).path("state").asText());
            }
        }
    }

    @Test void coldArrivalsAtOneMineCannotTakeAllBackgroundDispatchSlots() throws Exception {
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock);
            MarchView unrelated;
            try (var initial = directory(service, 16, 4, 4)) {
                for (String owner : List.of("301", "302", "401")) seed(initial, service, owner);
                depart(initial, service, "301", MarchMission.MARCH_MISSION_GATHER, 150);
                depart(initial, service, "302", MarchMission.MARCH_MISSION_GATHER, 150);
                unrelated = depart(initial, service, "401", MarchMission.MARCH_MISSION_GATHER, 650);
            }
            clock.at(unrelated.getArriveAt());
            var hotLoading = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            try (var players = new ActivePlayers(id -> {
                if (id.equals("301") || id.equals("302")) {
                    hotLoading.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return service.loadPlayer(id);
            }, 1, 2, 8, 4, 4, Duration.ofMinutes(1));
                 var pump = new RealmTaskPump(journal, players, service, clock, 2)) {
                try {
                    // 首批含两条同矿到达，慢加载只占一个在途名额；次页的不同矿无需等它结束。
                    assertEquals(1, pump.poll());
                    assertTrue(hotLoading.await(5, TimeUnit.SECONDS));
                    drive(pump, () -> "GATHERING".equals(state(journal, unrelated.getMarchId())));
                    assertTrue(((Number) pump.metrics().get("deferred")).longValue() > 0);
                } finally { release.countDown(); }
            }
        }
    }

    private static void onceOnly(ActivePlayers players, RealmMarchService service, DueTask task,
                                 RealmJournal journal) throws Exception {
        long before = journal.highSequence();
        List<CompletableFuture<Boolean>> deliveries = new ArrayList<>();
        for (int i = 0; i < 6; i++) deliveries.add(wake(players, service, task));
        int committed = 0;
        for (var future : deliveries) if (future.get(5, TimeUnit.SECONDS)) committed++;
        assertEquals(1, committed);
        assertEquals(before + 1, journal.highSequence());
    }

    private static ActivePlayers directory(RealmMarchService service, int capacity, int perPlayer, int residents) {
        return new ActivePlayers(service::loadPlayer, 2, 1, capacity, perPlayer, residents, Duration.ofMinutes(1));
    }

    private static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(5); }

    private static CompletableFuture<Boolean> wake(ActivePlayers players, RealmMarchService service, DueTask task) {
        return players.submit(PlayerIdentity.accountOf(task.playerId()), deadline(), player -> service.applyDue(player, task));
    }

    private static <T> T command(ActivePlayers players, String account, ActivePlayers.Work<T> work) throws Exception {
        return players.submit(account, deadline(), work).get(5, TimeUnit.SECONDS);
    }

    private static PlayerView player(ActivePlayers players, RealmMarchService service, String account) throws Exception {
        return command(players, account, p -> service.getPlayer(p, "read", GetPlayerReq.getDefaultInstance()).getPlayer());
    }

    private static void seed(ActivePlayers players, RealmMarchService service, String account) throws Exception {
        command(players, account, p -> service.createPlayer(p, "create", named(account)));
    }

    private static MarchView depart(ActivePlayers players, RealmMarchService service, String account,
                                    MarchMission mission, int target) throws Exception {
        var order = gather(120).toBuilder().setTargetX(target).setTargetY(target).setMission(mission).build();
        return command(players, account, p -> service.startMarch(p, "depart", order).getMarch());
    }

    private static long wood(PlayerView player) { return player.getResourcesOrThrow("WOOD"); }

    private static void assertReturned(PlayerView player, long wood) {
        assertEquals(3000, player.getTroopsOrThrow("INFANTRY"));
        assertFalse(player.getHeroes(0).hasAssignedMarch());
        assertTrue(player.getActiveMarchIdsList().isEmpty());
        assertEquals(wood, wood(player));
    }

    private static DueTask singleScheduled(RealmJournal journal) throws Exception {
        var tasks = journal.dueTasks(Long.MAX_VALUE, null, 16);
        assertEquals(1, tasks.size());
        return tasks.getFirst();
    }

    private static JsonNode record(RealmJournal j, String key) throws Exception {
        var value = j.read(key);
        assertNotNull(value, "Missing " + key);
        return Json.MAPPER.readTree(value.json());
    }

    private static String state(RealmJournal journal, String marchId) throws Exception {
        return record(journal, "march:" + marchId).path("state").asText();
    }

    @FunctionalInterface private interface Reached { boolean test() throws Exception; }
    private static void drive(RealmTaskPump pump, Reached reached) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!reached.test()) {
            if (System.nanoTime() >= deadline) fail("Due tasks failed to advance before timeout");
            pump.poll();
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }

    private static final class GameClock extends Clock {
        private volatile long now = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
        void at(long millis) { now = millis; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(instant(), zone); }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
        @Override public long millis() { return now; }
    }
}

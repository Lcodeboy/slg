package com.slg.game.realm;

import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopType;
import com.slg.rpc.v1.WriteReceipt;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class RealmMarchRecoveryTest {
    private static final int REALM = 11;
    private static final String OWNER = "101";
    private static final Clock DEPARTURE = Clock.fixed(Instant.parse("2026-01-20T08:00:00Z"), ZoneOffset.UTC);
    @TempDir Path directory;

    @Test void aCrashBeforeSubmissionLeavesAllTroopsAvailableAndRetryCanSubmit() throws Exception {
        verifyCrash("before");
    }

    @Test void aCrashAfterCommitBeforeReplyReturnsTheOriginalMarchWithoutChargingTwice() throws Exception {
        verifyCrash("after");
    }

    @Test void reorderedPayloadCanReplayButAnotherPlayerCannotReadTheMarch() throws Exception {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, REALM, id);
        try (var volume = RealmVolume.open(directory, REALM, id)) {
            var service = new RealmMarchService(volume.journal(), DEPARTURE);
            var alice = service.loadPlayer(OWNER);
            var bob = service.loadPlayer("102");
            service.createPlayer(alice, "create", named("Alice"));
            service.createPlayer(bob, "create", named("Bob"));
            var first = gather(120);
            var lancers = troop(TroopType.TROOP_TYPE_LANCER, 5);
            var ordered = first.toBuilder().addTroops(lancers).build();
            var reordered = first.toBuilder().clearTroops().addTroops(lancers).addTroops(first.getTroops(0)).build();
            var march = service.startMarch(alice, "ordered", ordered).getMarch();
            assertEquals(march, service.startMarch(alice, "ordered", reordered).getMarch());
            var request = marchById(march.getMarchId());
            assertEquals(march, service.getMarch(alice, "lookup", request).getMarch());
            assertEquals("MARCH_NOT_FOUND", rejection(() -> service.getMarch(bob, "lookup", request)));
            assertEquals(3L, volume.journal().highSequence());
            assertNull(StartMarchReq.getDescriptor().findFieldByName("player_id"));
            assertEquals(2880L, service.getPlayer(alice, "read", GetPlayerReq.getDefaultInstance())
                    .getPlayer().getTroopsOrThrow("INFANTRY"));
            assertEquals(3000L, service.getPlayer(bob, "read", GetPlayerReq.getDefaultInstance())
                    .getPlayer().getTroopsOrThrow("INFANTRY"));
        }
    }

    @Test void rejectedMarchKeepsItsOriginalReceiptWhenThePlayerLaterSpendsTroops() throws Exception {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, REALM, id);
        try (var volume = RealmVolume.open(directory, REALM, id)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, DEPARTURE);
            var active = service.loadPlayer(OWNER);
            service.createPlayer(active, "create", named("Alice"));
            var invalid = gather(3001);
            var denied = rejection(() -> service.startMarch(active, "denied", invalid));
            assertEquals("INSUFFICIENT_TROOPS", denied);
            assertEquals("COMMAND_REJECTED", journal.entry(2).factType());
            assertEquals(3000L, service.getPlayer(active, "read", GetPlayerReq.getDefaultInstance())
                    .getPlayer().getTroopsOrThrow("INFANTRY"));
            service.startMarch(active, "march-1", gather(120));
            assertEquals(denied, rejection(() -> service.startMarch(active, "denied", invalid)));
            assertEquals(3L, journal.highSequence());
        }
    }

    private void verifyCrash(String boundary) throws Exception {
        Path path = Files.createDirectory(directory.resolve("realm"));
        UUID volumeId = UUID.randomUUID();
        RealmVolume.initialize(path, REALM, volumeId);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var service = new RealmMarchService(volume.journal(), DEPARTURE);
            service.createPlayer(service.loadPlayer(OWNER), "create", named("Alice"));
            assertEquals(1, volume.journal().highSequence());
        }

        killWriter(path, volumeId, boundary);
        try (var volume = RealmVolume.open(path, REALM, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, Clock.offset(DEPARTURE, Duration.ofHours(1)));
            boolean committed = boundary.equals("after");
            assertEquals(committed ? 2L : 1L, journal.highSequence());
            // 恢复后客户端只知道写请求 ID。先只读查询，再决定是否重发，才能区分出征真正提交的时刻。
            var active = service.loadPlayer(OWNER);
            var receipt = service.getReceipt(active, "inspect", receiptFor("march-1"));
            assertEquals(committed, receipt.hasReceipt());
            if (committed) {
                assertEquals(2L, receipt.getJournalSeq());
                assertEquals(stored(journal.entry(2).resultJson()), receipt.getReceipt());
            }
            assertEquals(committed ? 2L : 1L, journal.highSequence());
            var beforeRetry = service.getPlayer(active, "read", GetPlayerReq.getDefaultInstance()).getPlayer();
            assertEquals(committed ? 2880 : 3000, beforeRetry.getTroopsOrThrow("INFANTRY"));
            var hero = beforeRetry.getHeroes(0);
            assertEquals(committed, hero.hasAssignedMarch());
            if (committed) assertEquals("MARCH_STARTED", journal.entry(2).factType());
            else assertNull(journal.entry(2));

            var response = service.startMarch(active, "march-1", gather(120));
            var departure = response.getMarch();
            String marchId = departure.getMarchId();
            assertFalse(marchId.isBlank());
            if (committed) {
                assertEquals(hero.getAssignedMarch(), marchId);
                assertEquals(DEPARTURE.millis(), departure.getDepartAt());
            }
            assertEquals(response, service.startMarch(active, "march-1", gather(120)));
            assertEquals(2L, journal.highSequence());
            var logged = journal.entry(2);
            assertEquals(response, stored(logged.resultJson()).getStarted());
            var changedKeys = logged.changes().stream().map(change -> change.key()).toList();
            assertTrue(changedKeys.contains("player:" + REALM + "_" + OWNER));
            assertTrue(changedKeys.contains("march:" + marchId));
            assertTrue(changedKeys.contains("task:" + departure.getTaskId()));
            assertNotNull(journal.read("march:" + marchId));
            var task = Json.MAPPER.readTree(journal.read("task:" + departure.getTaskId()).json());
            assertEquals("PENDING", task.get("state").textValue());
            assertEquals("MARCH_ARRIVE", task.get("type").textValue());
            assertEquals(marchId, task.get("marchId").textValue());
            assertEquals(departure.getArriveAt(), task.get("dueAt").longValue());
            var player = service.getPlayer(active, "read", GetPlayerReq.getDefaultInstance()).getPlayer();
            assertEquals(2880, player.getTroopsOrThrow("INFANTRY"));
            assertEquals(marchId, player.getHeroes(0).getAssignedMarch());
            assertEquals("REQUEST_CONFLICT", rejection(() -> service.startMarch(active, "march-1", gather(119))));
            assertEquals(2L, journal.highSequence());
        }
    }

    private void killWriter(Path path, UUID volumeId, String boundary) throws Exception {
        Path signal = directory.resolve("safe-to-kill");
        Path output = directory.resolve("child.log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx96m", "-cp", classpath, CrashPoint.class.getName(), path.toString(), volumeId.toString(),
                boundary, signal.toString()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (child.isAlive() && !Files.exists(signal) && System.nanoTime() < deadline) {
                Thread.sleep(15);
            }
            assertTrue(child.isAlive() && Files.exists(signal), "child failed before boundary: " + Files.readString(output));
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), "crashed JVM did not terminate");
        }
    }

    /** 模拟 GAME 的提交到发包之间的窗口。信号只通知测试进程强杀，不对客户端交付结果。 */
    public static final class CrashPoint {
        public static void main(String[] args) throws Exception {
            try (var volume = RealmVolume.open(Path.of(args[0]), REALM, UUID.fromString(args[1]))) {
                if (args[2].equals("after")) {
                    // 没有主动关闭卷、flush 或通过其他渠道保存业务结果。
                    var game = new RealmMarchService(volume.journal(), DEPARTURE);
                    game.startMarch(game.loadPlayer(OWNER), "march-1", gather(120));
                }
                Files.createFile(Path.of(args[3]));
                System.in.read();
            }
        }
    }

    private static WriteReceipt stored(String json) throws java.io.IOException {
        return WriteReceipt.parseFrom(Json.MAPPER.readValue(json, byte[].class));
    }
}

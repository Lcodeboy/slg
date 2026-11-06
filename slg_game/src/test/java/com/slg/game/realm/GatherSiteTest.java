package com.slg.game.realm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

/** 以可控时钟走真实建角、行军、任务与召回；争用顺序在这些用例中显式指定。 */
class GatherSiteTest {
    @TempDir Path directory;
    private final UUID volumeId = UUID.randomUUID();
    private RealmVolume volume;
    private RealmMarchService game;
    private RealmJournal journal;
    private final TestClock clock = new TestClock();

    @BeforeEach void open() throws Exception {
        RealmVolume.initialize(directory, 1, volumeId);
        volume = RealmVolume.open(directory, 1, volumeId);
        journal = volume.journal();
        game = new RealmMarchService(journal, clock);
    }

    @AfterEach void close() { if (volume != null) volume.close(); }

    @Test void recallReleasesTheClaimOnceAndItsOldTimerCannotReleaseTheNextOwnersClaim() throws Exception {
        var a = depart("101"); var b = depart("102"); var c = depart("103");
        clock.now = a.dueAt();
        assertTrue(game.applyDue(game.loadPlayer("101"), a));
        var oldGather = pending("101", "GATHER_DONE");
        var recallA = recallOrder(a.marchId());
        game.recallMarch(game.loadPlayer("101"), "recall", recallA);
        assertEquals(2, site().version());
        assertFalse(site().occupied());
        assertTrue(game.applyDue(game.loadPlayer("102"), b));
        assertTrue(game.applyDue(game.loadPlayer("103"), c));
        assertEquals("MARCH_TARGET_OCCUPIED", journal.entry(journal.highSequence()).factType());
        assertEquals("RETURNING", state("march:" + c.marchId()).path("state").asText());
        assertTrue(state("march:" + c.marchId()).path("cargo").isEmpty());
        assertEquals("MARCH_RETURN", pending("103", "MARCH_RETURN").type());
        long high = journal.highSequence();
        clock.now = oldGather.dueAt();
        assertFalse(game.applyDue(game.loadPlayer("101"), oldGather));
        game.recallMarch(game.loadPlayer("101"), "recall", recallA);
        assertEquals(high, journal.highSequence());
        assertEquals(3, site().version());
        assertEquals(b.marchId(), site().site().occupantMarchId());
    }

    @Test void overdueRecallSettlesGatheringBeforeTheRecallDecision() throws Exception {
        var a = depart("101"); var b = depart("102");
        clock.now = a.dueAt();
        assertTrue(game.applyDue(game.loadPlayer("101"), a));
        var gather = pending("101", "GATHER_DONE");
        clock.now = gather.dueAt();
        var request = recallOrder(a.marchId());
        var denied = rejection(() -> game.recallMarch(game.loadPlayer("101"), "recall", request));
        assertEquals("INVALID_MARCH_STATE", denied);
        assertFalse(site().occupied());
        assertEquals(600, state("march:" + a.marchId()).at("/cargo/WOOD").asLong());
        assertTrue(game.applyDue(game.loadPlayer("102"), b));
        long high = journal.highSequence();
        assertEquals(denied, rejection(() -> game.recallMarch(game.loadPlayer("101"), "recall", request)));
        assertEquals(high, journal.highSequence());
        assertEquals(b.marchId(), site().site().occupantMarchId());
    }

    @Test void inconsistentClaimGenerationBlocksTheGatherTaskAndPreservesTheClaim() throws Exception {
        var a = depart("101");
        clock.now = a.dueAt();
        assertTrue(game.applyDue(game.loadPlayer("101"), a));
        var gather = pending("101", "GATHER_DONE");
        // 只在隔离卷注入占用代次分歧。即便角色与行军 ID 都对，任务也不能交付或释放这份预留。
        var other = new GatherSite(150, 150, a.marchId(), a.playerId(), gather.generation() + 1);
        journal.commit(new RealmJournal.Decision("test", "bad-generation", "digest", "TEST", "{}", "{}",
                List.of(new RealmJournal.Change(GatherSite.key(150,150), site().version(), Json.write(other)))));
        assertThrows(java.io.IOException.class, () -> new RealmMarchService(journal, clock),
                "Restart must not serve new arrivals from a conflicting active reservation");
        clock.now = gather.dueAt();
        assertTrue(game.applyDue(game.loadPlayer("101"), gather));
        assertEquals("BLOCKED", state("task:" + gather.taskId()).path("state").asText());
        assertEquals("GATHERING", state("march:" + a.marchId()).path("state").asText());
        assertEquals(other, site().site());
    }

    @Test void restartRejectsAGatherWithNoClaimAndCannotCreditItsCargo() throws Exception {
        var a = depart("101");
        injectUnverifiableGather(a, GatherSite.RULES_VERSION, false);
        assertCannotSettle(a);
        assertEquals(0, site().version());
    }

    @Test void restartRejectsUnsupportedRulesEvenIfTheClaimLooksValid() throws Exception {
        var a = depart("101");
        injectUnverifiableGather(a, "unsupported-rules", true);
        assertCannotSettle(a);
        assertEquals(a.marchId(), site().site().occupantMarchId());
    }

    private void assertCannotSettle(RealmJournal.DueTask a) throws Exception {
        assertThrows(java.io.IOException.class, () -> new RealmMarchService(journal, clock));
        var gather = pending("101", "GATHER_DONE");
        clock.now = gather.dueAt();
        assertTrue(game.applyDue(game.loadPlayer("101"), gather));
        assertEquals("BLOCKED", state("task:" + gather.taskId()).path("state").asText());
        assertEquals("GATHERING", state("march:" + a.marchId()).path("state").asText());
        assertEquals(0, state("player:" + a.playerId()).at("/resources/WOOD").longValue());
    }

    /** 在隔离卷的到达边界注入坏快照。任务与行军的 ID、版本和时间保持一致，只破坏本用例指定的一项。 */
    private void injectUnverifiableGather(RealmJournal.DueTask arrival, String rules, boolean withClaim) throws Exception {
        var id = arrival.marchId();
        var march = Json.MAPPER.readValue(journal.read("march:" + id).json(), MarchTasks.March.class);
        var outbound = Json.MAPPER.readValue(journal.read("task:" + arrival.taskId()).json(), MarchTasks.Task.class);
        var collecting = march.gather(arrival.dueAt());
        var gather = MarchTasks.nextTask(collecting, "GATHER_DONE", 2);
        ObjectNode row = Json.MAPPER.valueToTree(collecting);
        row.put("rulesVersion", rules);
        var changes = new ArrayList<>(List.of(
                new RealmJournal.Change("task:" + outbound.taskId(), 1, Json.write(outbound.finish("DONE", null))),
                new RealmJournal.Change("march:" + id, 1, Json.write(row)),
                new RealmJournal.Change("task:" + gather.taskId(), 0, Json.write(gather))));
        if (withClaim) changes.add(site().claim(collecting, gather.generation()));
        journal.commit(new RealmJournal.Decision("test", "inject", "digest", "TEST", "{}", "{}", changes));
    }

    private RealmJournal.DueTask depart(String owner) throws Exception {
        var player = game.loadPlayer(owner);
        game.createPlayer(player, "create", named(owner));
        game.startMarch(player, "depart", gather(120));
        return pending(owner, "MARCH_ARRIVE");
    }

    private RealmJournal.DueTask pending(String owner, String type) throws Exception {
        return journal.dueTasks(Long.MAX_VALUE, null, 32).stream()
                .filter(t -> ("1_" + owner).equals(t.playerId()) && type.equals(t.type())).findFirst().orElseThrow();
    }

    private GatherSite.Stored site() throws Exception { return GatherSite.read(journal, 150, 150); }
    private JsonNode state(String key) throws Exception { return Json.MAPPER.readTree(journal.read(key).json()); }

    private static final class TestClock extends Clock {
        long now = 1_000_000L;
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return Clock.fixed(instant(), zone); }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
    }
}

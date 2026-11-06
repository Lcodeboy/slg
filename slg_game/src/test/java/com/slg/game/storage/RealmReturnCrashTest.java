package com.slg.game.storage;

import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.StartMarchRsp;
import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.protocol.Json;
import com.slg.game.realm.ActivePlayer;
import com.slg.game.realm.ActivePlayers;
import com.slg.game.realm.RealmMarchService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class RealmReturnCrashTest {
    private static final String OWNER = "101";
    private static final Clock START = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private static final Clock LATE = Clock.offset(START, Duration.ofHours(1));
    @TempDir Path directory;

    @Test void aReturnWhoseBatchWasWrittenButNotAcknowledgedCannotCreditThePlayerAgain() throws Exception {
        UUID volumeId = UUID.randomUUID();
        RealmVolume.initialize(directory, 1, volumeId);
        StartMarchRsp original;
        RealmJournal.DueTask returning;
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            var journal = volume.journal();
            var initial = new RealmMarchService(journal, START);
            var active = initial.loadPlayer(OWNER);
            initial.createPlayer(active, "create", named(OWNER));
            original = initial.startMarch(active, "depart", gather(120));
            var catchup = new RealmMarchService(journal, LATE);
            var player = catchup.loadPlayer(OWNER);
            assertTrue(catchup.applyDue(player, onlyDue(journal)));
            assertTrue(catchup.applyDue(player, onlyDue(journal)));
            returning = onlyDue(journal);
            assertEquals("MARCH_RETURN", returning.type());
            assertEquals(4, journal.highSequence());
            var state = catchup.getPlayer(player, "read", GetPlayerReq.getDefaultInstance()).getPlayer();
            assertEquals(2880, state.getTroopsOrThrow("INFANTRY"));
            assertEquals(0, state.getResourcesOrThrow("WOOD"));
            assertEquals(returning.marchId(), state.getHeroes(0).getAssignedMarch());
        }

        try (var options = new Options().setCreateIfMissing(false);
             var db = RocksDB.open(options, directory.toString())) {
            var lostAck = new RealmJournal(db, (writes, batch) -> {
                db.write(writes, batch);
                throw new RocksDBException("return batch written, acknowledgement lost");
            });
            lostAck.recover();
            var service = new RealmMarchService(lostAck, LATE);
            var active = new AtomicReference<ActivePlayer>();
            try (var players = new ActivePlayers(service::loadPlayer, 1, 1, 8, 4, 2, Duration.ofMinutes(1))) {
                var pending = players.submit(OWNER, deadline(), player -> {
                    active.set(player);
                    return service.applyDue(player, returning);
                });
                var failure = assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
                assertInstanceOf(IOException.class, failure.getCause());
                assertFalse(active.get().valid(), "the unpublished pre-return snapshot must not be served");
                assertThrows(IOException.class, lostAck::highSequence);
            }
        }

        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, LATE);
            var active = service.loadPlayer(OWNER);
            assertEquals(5, journal.highSequence());
            assertEquals("MARCH_COMPLETED", journal.entry(5).factType());
            assertEquals("DONE", row(journal, "task:" + returning.taskId()).path("state").asText());
            assertEquals("COMPLETED", row(journal, "march:" + returning.marchId()).path("state").asText());
            assertTrue(journal.dueTasks(LATE.millis(), null, 8).isEmpty());
            assertFalse(service.applyDue(active, returning));
            assertEquals(original, service.startMarch(active, "depart", gather(120)));
            var after = service.getPlayer(active, "read", GetPlayerReq.getDefaultInstance()).getPlayer();
            assertEquals(3000, after.getTroopsOrThrow("INFANTRY"));
            assertEquals(600, after.getResourcesOrThrow("WOOD"));
            assertFalse(after.getHeroes(0).hasAssignedMarch());
            assertTrue(after.getActiveMarchIdsList().isEmpty());
            assertEquals(5, journal.highSequence());
        }
    }

    private static JsonNode row(RealmJournal journal, String key) throws IOException {
        return Json.MAPPER.readTree(journal.read(key).json());
    }

    private static RealmJournal.DueTask onlyDue(RealmJournal journal) throws IOException {
        var due = journal.dueTasks(LATE.millis(), null, 8);
        assertEquals(1, due.size());
        return due.getFirst();
    }

    private static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(5); }
}

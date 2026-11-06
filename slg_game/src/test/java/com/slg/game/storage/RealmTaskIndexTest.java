package com.slg.game.storage;

import com.slg.core.protocol.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RealmTaskIndexTest {
    @TempDir Path directory;
    final UUID volumeId = UUID.randomUUID();

    @Test void pagesUseTimeThenIdAndFinishingOrMovingATaskRemovesItsOldKey() throws Exception {
        RealmVolume.initialize(directory, 1, volumeId);
        try (RealmVolume volume = RealmVolume.open(directory, 1, volumeId)) {
            var journal = volume.journal();
            var add = decide("start", List.of(task("z", 0, 10, "PENDING"), task("a", 0, 10, "PENDING"),
                    task("late", 0, 30, "PENDING"), task("first", 0, 3, "PENDING")));
            assertEquals(journal.commit(add), journal.commit(add));
            var page = journal.dueTasks(10, null, 2);
            assertEquals(List.of("first", "a"), ids(page));
            var cursor = page.getLast();
            journal.commit(decide("advance", List.of(task("a", 1, 10, "DONE"), task("first", 1, 20, "PENDING"))));
            // 即使上一页末项刚刚被删除，排在它后面的相同到期时间的任务也不能漏掉。
            assertEquals(List.of("z"), ids(journal.dueTasks(10, cursor, 2)));
            assertEquals(List.of("z", "first", "late"), ids(journal.dueTasks(30, null, 3)));
            assertTrue(journal.dueTasks(9, null, 1).isEmpty());
            journal.commit(decide("stop", List.of(task("z", 1, 10, "CANCELLED"), task("first", 2, 20, "BLOCKED"))));
            assertEquals(List.of("late"), ids(journal.dueTasks(30, null, 1)));

            var invalid = new RealmJournal.Change("task:wrong", 0, task("another", 0, 1, "PENDING").json());
            assertThrows(IllegalArgumentException.class, () -> journal.commit(decide("bad", List.of(invalid))));
            assertEquals(3L, journal.highSequence());
            assertNull(journal.read("task:wrong"));
        }
        try (RealmVolume volume = RealmVolume.open(directory, 1, volumeId)) {
            assertEquals(List.of("late"), ids(volume.journal().dueTasks(30, null, 5)));
        }
    }

    @Test void reopeningRetainsTheCompletePendingTaskIndexAndOriginalReceipt() throws Exception {
        RealmVolume.initialize(directory, 1, volumeId);
        final int count = 70;
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            var changes = new ArrayList<RealmJournal.Change>();
            for (int i = 0; i < count; i++)
                changes.add(task("t%02d".formatted(i), 0, 500, i == 5 ? "DONE" : "PENDING"));
            volume.journal().commit(decide("initial-tasks", changes));
        }
        try (var reopened = RealmVolume.open(directory, 1, volumeId)) {
            var journal = reopened.journal();
            var first = journal.dueTasks(500, null, 64);
            var second = journal.dueTasks(500, first.getLast(), 64);
            assertEquals(64, first.size());
            assertFalse(ids(first).contains("t05"));
            assertEquals(List.of("t65", "t66", "t67", "t68", "t69"), ids(second));
            assertEquals(1L, journal.highSequence());
            assertEquals(1L, journal.lookup("timer-test", "initial-tasks", "digest").sequence());
        }
    }

    @Test void missingOrMismatchedIndexedTaskIsRejected() throws Exception {
        RealmVolume.initialize(directory, 1, volumeId);
        try (var opts = new Options().setCreateIfMissing(false);
             var db = RocksDB.open(opts, directory.toString())) {
            db.put(indexKey("ghost", 5), new byte[0]);
        }
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            assertThrows(IOException.class, () -> volume.journal().dueTasks(5, null, 10));
        }
    }

    private static List<String> ids(List<RealmJournal.DueTask> tasks) {
        return tasks.stream().map(RealmJournal.DueTask::taskId).toList();
    }

    private static RealmJournal.Change task(String id, long version, long due, String state) {
        return new RealmJournal.Change("task:" + id, version, Json.write(Map.of("taskId", id, "playerId", "A",
                "marchId", "m1", "type", "MARCH_ARRIVE", "state", state, "generation", 1, "dueAt", due)));
    }

    private static RealmJournal.Decision decide(String id, List<RealmJournal.Change> changes) {
        return new RealmJournal.Decision("timer-test", id, "digest", "TIMER_TEST", "{}", "{}", changes);
    }

    private static byte[] indexKey(String id, long due) {
        return ByteBuffer.allocate(9 + id.length()).put((byte)'D').putLong(due).put(bytes(id)).array();
    }

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
}

package com.slg.game.storage;

import com.slg.core.protocol.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class RealmBattleIndexTest {
    @TempDir Path directory;
    final UUID volumeId = UUID.randomUUID();

    @Test void reopensPendingJobsFromTheCommittedIndexAndRetiresThemWithTheirResult() throws Exception {
        RealmVolume.initialize(directory, 1, volumeId);
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            volume.journal().commit(write("add", job("a", 0, "PENDING"), job("b", 0, "SETTLED"), job("c", 0, "PENDING")));
        }
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            var journal = volume.journal();
            var first = journal.pendingBattles(null, 1).getFirst();
            assertEquals("a", first.battleId());
            assertEquals("p", first.playerId());
            assertEquals(journal.read("battle:a"), first.record());
            assertThrows(RealmJournal.Stale.class, () -> journal.commit(write("stale", job("a", 0, "SETTLED"))));
            assertEquals(List.of("a", "c"), ids(journal, null));
            journal.commit(write("settle", job("a", 1, "SETTLED")));
            assertEquals(List.of("c"), ids(journal, "a")); // 游标指向的索引已经删除。
            assertEquals(2L, journal.highSequence());
        }
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            assertEquals(List.of("c"), ids(volume.journal(), null));
            assertNotNull(volume.journal().read("battle:a"));
        }
    }

    @Test void orphanedIndexEntryCannotBeDispatched() throws Exception {
        RealmVolume.initialize(directory, 1, volumeId);
        try (var opts = new Options(); var db = RocksDB.open(opts, directory.toString())) {
            db.put(bytes("Bghost"), new byte[0]);
        }
        try (var volume = RealmVolume.open(directory, 1, volumeId)) {
            assertThrows(IOException.class, () -> volume.journal().pendingBattles(null, 1));
        }
    }

    private static List<String> ids(RealmJournal journal, String after) throws IOException {
        return journal.pendingBattles(after, 4).stream().map(RealmJournal.PendingBattle::battleId).toList();
    }

    private static RealmJournal.Decision write(String requestId, RealmJournal.Change... changes) {
        return new RealmJournal.Decision("system", requestId, "digest", "BATTLE_INDEX_TEST", "{}", "{}", List.of(changes));
    }

    private static RealmJournal.Change job(String id, long version, String state) {
        return new RealmJournal.Change("battle:" + id, version, Json.write(Map.of("battleId", id, "marchId", id,
                "playerId", "p", "inputHash", "a".repeat(64), "rulesVersion", "v1", "state", state)));
    }

    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
}

package com.slg.game.storage;

import com.slg.rpc.v1.GetPlayerReq;
import com.slg.game.realm.RealmMarchService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class ActivePlayerCommitFailureTest {
    @TempDir Path directory;
    private static final Clock ORIGINAL = Clock.fixed(Instant.parse("2026-01-20T08:00:00Z"), ZoneOffset.UTC);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void uncertainMarchInvalidatesMemoryAndResolvesFromTheOriginalVolume(boolean actuallyCommitted) throws Exception {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, 1, id);
        var read = GetPlayerReq.getDefaultInstance();
        var march = gather(120);
        try (Options options = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(options, directory.toString())) {
            var writes = new AtomicInteger();
            var journal = new RealmJournal(db, (writeOptions, batch) -> {
                if (writes.incrementAndGet() == 1) {
                    db.write(writeOptions, batch); // 建角完成。
                    return;
                }
                if (actuallyCommitted) db.write(writeOptions, batch);
                throw new RocksDBException("unknown outcome");
            });
            journal.recover();
            var service = new RealmMarchService(journal, ORIGINAL);
            var player = service.loadPlayer("101");
            service.createPlayer(player, "create", named("Alice"));
            assertEquals(3000L, service.getPlayer(player, "read", read).getPlayer().getTroopsOrThrow("INFANTRY"));
            assertThrows(IOException.class, () -> service.startMarch(player, "march", march));
            assertFalse(player.valid());
            assertThrows(IOException.class, () -> service.getPlayer(player, "read", read));
        }
        try (var recovered = RealmVolume.open(directory, 1, id)) {
            var later = Clock.offset(ORIGINAL, Duration.ofHours(1));
            var service = new RealmMarchService(recovered.journal(), later);
            var player = service.loadPlayer("101");
            var before = service.getPlayer(player, "read", read).getPlayer();
            assertEquals(actuallyCommitted ? 2880 : 3000, before.getTroopsOrThrow("INFANTRY"));
            assertEquals(actuallyCommitted ? 2 : 1, recovered.journal().highSequence());
            var replay = service.startMarch(player, "march", march).getMarch();
            assertEquals(actuallyCommitted ? ORIGINAL.millis() : later.millis(), replay.getDepartAt());
            assertEquals(replay, service.startMarch(player, "march", march).getMarch());
            var after = service.getPlayer(player, "read", read).getPlayer();
            assertEquals(2880L, after.getTroopsOrThrow("INFANTRY"));
            assertEquals(replay.getMarchId(), after.getHeroes(0).getAssignedMarch());
            assertEquals(2L, recovered.journal().highSequence());
        }
    }

}

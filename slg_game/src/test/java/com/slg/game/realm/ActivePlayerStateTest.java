package com.slg.game.realm;

import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.HeroSelection;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class ActivePlayerStateTest {
    @TempDir Path directory;
    private static final String ACCOUNT = "101";

    @Test void replayingOlderReceiptsCannotPublishAnOlderSnapshot() throws Exception {
        try (var volume = create()) {
            var service = new RealmMarchService(volume.journal(), Clock.systemUTC());
            var loads = new AtomicInteger();
            try (var players = directory(service, loads)) {
                var created = named("Alice");
                var firstMarch = withoutHero(120);
                var secondMarch = withoutHero(80);
                var original = run(players, p -> service.createPlayer(p, "create", created));
                var first = run(players, p -> service.startMarch(p, "first", firstMarch));
                run(players, p -> service.startMarch(p, "second", secondMarch));
                var latest = run(players, p -> service.getPlayer(p, "read", GetPlayerReq.getDefaultInstance()).getPlayer());
                assertEquals(2800, latest.getTroopsOrThrow("INFANTRY"));
                assertEquals(3L, latest.getVersion());
                assertEquals(original, run(players, p -> service.createPlayer(p, "create", created)));
                assertEquals(first, run(players, p -> service.startMarch(p, "first", firstMarch)));
                assertEquals(latest, run(players, p -> service.getPlayer(p, "read", GetPlayerReq.getDefaultInstance()).getPlayer()));
                var denied = run(players, p -> rejection(() -> service.startMarch(p, "too-many", withoutHero(2900))));
                assertEquals("INSUFFICIENT_TROOPS", denied);
                assertEquals(latest, run(players, p -> service.getPlayer(p, "read", GetPlayerReq.getDefaultInstance()).getPlayer()));
                assertEquals(4L, volume.journal().highSequence());
                assertEquals(1, loads.get(), "all requests should reuse the active player");
            }
        }
    }

    @Test void staleCommitDoesNotPublishItsDraftAndTheNextRequestReloadsTheWinner() throws Exception {
        try (var volume = create()) {
            var service = new RealmMarchService(volume.journal(), Clock.systemUTC());
            var loads = new AtomicInteger();
            try (var players = directory(service, loads)) {
                run(players, p -> service.createPlayer(p, "create", named("Alice")));
                var staleRequest = withoutHero(80);
                // 模拟不经过该目录的另一个已提交写者，使活动快照的预期版本失效。
                service.startMarch(service.loadPlayer(ACCOUNT), "winner", withoutHero(120));
                var rejected = run(players, p -> rejection(() -> service.startMarch(p, "retry", staleRequest)));
                assertEquals("STATE_CHANGED", rejected);
                assertEquals(2L, volume.journal().highSequence());
                var reloaded = run(players, p -> service.getPlayer(p, "read", GetPlayerReq.getDefaultInstance()).getPlayer());
                assertEquals(2880L, reloaded.getTroopsOrThrow("INFANTRY"));
                assertEquals(2L, reloaded.getVersion());
                assertEquals(2, loads.get(), "a stale snapshot must leave the directory before the next read");
                run(players, p -> service.startMarch(p, "retry", staleRequest));
                assertEquals(2800L, run(players, p -> service.getPlayer(p, "read", GetPlayerReq.getDefaultInstance()).getPlayer()).getTroopsOrThrow("INFANTRY"));
            }
        }
    }

    @Test void cachedReadsDoNotPretendToBeAuthoritativeAfterTheVolumeCloses() throws Exception {
        try (var volume = create()) {
            var service = new RealmMarchService(volume.journal(), Clock.systemUTC());
            var player = service.loadPlayer(ACCOUNT);
            service.createPlayer(player, "create", named("Alice"));
            volume.close();
            assertThrows(IOException.class, () -> service.getPlayer(player, "read", GetPlayerReq.getDefaultInstance()));
            assertFalse(player.valid());
        }
    }

    private RealmVolume create() throws Exception {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, 1, id);
        return RealmVolume.open(directory, 1, id);
    }

    private static ActivePlayers directory(RealmMarchService service, AtomicInteger loads) {
        return new ActivePlayers(id -> {
            loads.incrementAndGet();
            return service.loadPlayer(id);
        }, 1, 1, 8, 4, 4, Duration.ofMinutes(5));
    }

    private static <T> T run(ActivePlayers players, ActivePlayers.Work<T> action)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        return players.submit(ACCOUNT, deadline, action).get(5, TimeUnit.SECONDS);
    }

    private static StartMarchReq withoutHero(long troops) {
        return gather(troops).toBuilder().setHeroes(HeroSelection.getDefaultInstance()).build();
    }
}

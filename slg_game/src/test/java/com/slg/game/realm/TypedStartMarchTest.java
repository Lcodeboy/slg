package com.slg.game.realm;

import com.slg.rpc.v1.HeroSelection;
import com.slg.rpc.v1.MarchMission;
import com.slg.rpc.v1.MarchState;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.StartMarchRsp;
import com.slg.rpc.v1.TroopGroup;
import com.slg.rpc.v1.TroopType;
import com.slg.game.domain.GameException;
import com.slg.game.storage.RealmVolume;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class TypedStartMarchTest {
    private static final int REALM = 17;
    private static final String ACCOUNT = "101";
    private static final Clock DEPARTURE = Clock.fixed(Instant.parse("2026-01-20T08:00:00Z"), ZoneOffset.UTC);
    private static final TroopGroup INFANTRY = troop(TroopType.TROOP_TYPE_INFANTRY, 120);
    private static final TroopGroup LANCER = troop(TroopType.TROOP_TYPE_LANCER, 5);
    @TempDir Path directory;

    @Test void canonicalTroopOrderReplaysTheCommittedDepartureAfterReopeningTheVolume() throws IOException {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, REALM, id);
        var march = order().addTroops(LANCER).addTroops(INFANTRY).build();
        StartMarchRsp first;
        try (var volume = RealmVolume.open(directory, REALM, id)) {
            var game = new RealmMarchService(volume.journal(), DEPARTURE);
            var player = create(game);
            first = game.startMarch(player, "march", march);
            assertEquals(first, game.startMarch(player, "march", march));
            assertEquals("17_101", first.getMarch().getPlayerId());
            assertEquals(MarchState.MARCH_STATE_OUTBOUND, first.getMarch().getState());
            assertEquals(MarchMission.MARCH_MISSION_GATHER, first.getMarch().getMission());
            assertEquals(DEPARTURE.millis(), first.getMarch().getDepartAt());
            assertTrue(first.getMarch().getArriveAt() > first.getMarch().getDepartAt());
            assertEquals(List.of(INFANTRY, LANCER), first.getMarch().getTroopsList());
            assertEquals(List.of("sergey"), first.getMarch().getHeroIdsList());
            assertNotNull(volume.journal().read("task:" + first.getMarch().getTaskId()));
            assertEquals(2L, volume.journal().highSequence());
        }
        try (var volume = RealmVolume.open(directory, REALM, id)) {
            var game = new RealmMarchService(volume.journal(), Clock.offset(DEPARTURE, Duration.ofHours(1)));
            var player = game.loadPlayer(ACCOUNT);
            var reordered = order().addTroops(INFANTRY).addTroops(LANCER).build();
            assertArrayEquals(first.toByteArray(), game.startMarch(player, "march", reordered).toByteArray());
            assertEquals(2880L, player.snapshot().player().troops().get("INFANTRY"));
            assertEquals(2L, volume.journal().highSequence());
            // 首次未指定英雄，已经占用了默认英雄。显式空英雄列表以新 ID 出征，不能再次占用该英雄。
            var withoutHero = march.toBuilder().setHeroes(HeroSelection.getDefaultInstance()).build();
            assertEquals("REQUEST_CONFLICT", rejected(game, player, "march", withoutHero));
            var second = game.startMarch(player, "no-hero", withoutHero);
            assertEquals(List.of(), second.getMarch().getHeroIdsList());
            assertEquals(first.getMarch().getMarchId(), player.snapshot().player().heroes().getFirst().assignedMarch());
            assertEquals(2760L, player.snapshot().player().troops().get("INFANTRY"));
            assertEquals(3L, volume.journal().highSequence());
        }
    }

    @Test void malformedSelectionsAreDurableRejectionsAndTransientPressureCanRetryTheSameId() throws IOException {
        UUID id = UUID.randomUUID();
        RealmVolume.initialize(directory, REALM, id);
        var allowed = new AtomicBoolean(true);
        try (var volume = RealmVolume.open(directory, REALM, id)) {
            var journal = volume.journal();
            var game = new RealmMarchService(journal, DEPARTURE, high -> allowed.get());
            var player = create(game);
            var valid = order().addTroops(INFANTRY).build();
            var duplicated = valid.toBuilder().addTroops(INFANTRY).build();
            assertEquals("INVALID_ARGUMENT", rejected(game, player, "bad", duplicated));
            assertEquals("INVALID_ARGUMENT", rejected(game, player, "bad", duplicated));
            assertEquals("REQUEST_CONFLICT", rejected(game, player, "bad", valid));
            assertEquals("COMMAND_REJECTED", journal.entry(2).factType());
            assertTrue(player.snapshot().player().activeMarchIds().isEmpty());
            assertEquals(3000L, player.snapshot().player().troops().get("INFANTRY"));
            allowed.set(false);
            assertEquals("PROJECTION_BACKLOG", rejected(game, player, "retry", valid));
            assertNull(journal.findReceipt(player.playerId(), "retry"));
            assertEquals(2L, journal.highSequence());
            allowed.set(true);
            var confirmed = game.startMarch(player, "retry", valid);
            allowed.set(false);
            assertEquals(confirmed, game.startMarch(player, "retry", valid));
            assertEquals(3L, journal.highSequence());
            assertEquals(2880L, player.snapshot().player().troops().get("INFANTRY"));
            var receipt = game.getReceipt(player, "inspect", receiptFor("retry"));
            assertEquals(confirmed, receipt.getReceipt().getStarted());
        }
    }

    private static TroopGroup troop(TroopType type, long count) {
        return TroopGroup.newBuilder().setType(type).setCount(count).build();
    }

    private static StartMarchReq.Builder order() { return StartMarchReq.newBuilder().setTargetX(150).setTargetY(150); }

    private static ActivePlayer create(RealmMarchService game) throws IOException {
        var player = game.loadPlayer(ACCOUNT);
        game.createPlayer(player, "create", named("Alice"));
        return player;
    }

    private static String rejected(RealmMarchService game, ActivePlayer player, String id, StartMarchReq request) {
        return assertThrows(GameException.class, () -> game.startMarch(player, id, request)).code();
    }
}

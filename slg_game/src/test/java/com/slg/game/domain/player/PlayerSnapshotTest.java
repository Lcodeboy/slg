package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.slg.game.domain.player.PlayerTestData.*;
import static org.junit.jupiter.api.Assertions.*;

class PlayerSnapshotTest {
    @Test void missingPlayerKeepsOnlyItsCandidateKeyAndAbsenceVersion() {
        var absent = PlayerSnapshot.absent(REALM, ACCOUNT, 0);
        assertEquals(PLAYER, absent.playerId());
        assertTrue(absent.profileState().isEmpty());
        assertTrue(absent.ownedStates().isEmpty());
        assertEquals(Map.of(new ObjectKey.Player(PLAYER), 0L), absent.readVersions());
        assertThrows(UnsupportedOperationException.class, () -> absent.readVersions().clear());
    }

    @Test void existingPlayerUsesItsFoundingIdAndKeepsEveryRootVersionSeparate() {
        var identity = new RealmIdentity(20, UUID.randomUUID());
        var rows = new ArrayList<>(PlayerCreation.initialStates(REALM, ACCOUNT, "Alice"));
        rows.set(0, new PlayerState(PLAYER, ACCOUNT, 17, 20, "Alice"));
        var snapshot = snapshot(rows, identity, 7);
        assertEquals(PLAYER, snapshot.playerId());
        assertEquals(20, snapshot.profileState().orElseThrow().currentRealmId());
        assertEquals(7L, snapshot.readVersions().get(new ObjectKey.Player(PLAYER)));
        assertEquals(11L, snapshot.readVersions().get(new ObjectKey.Resource(PLAYER, ResourceType.WOOD)));
        assertEquals(21L, snapshot.readVersions().get(new ObjectKey.Troop(PLAYER, TroopType.INFANTRY)));
        assertEquals(31L, snapshot.readVersions().get(new ObjectKey.Hero(PLAYER, "sergey")));
        var keys = snapshot.ownedStates().stream().map(row -> row.state().key()).toList();
        assertEquals(keys.stream().sorted(ObjectKey.ORDER).toList(), keys);
        assertThrows(UnsupportedOperationException.class, () -> snapshot.ownedStates().clear());
    }

    @Test void mutableInputMapsCannotChangePublishedSnapshot() {
        var initial = snapshot();
        var resources = new HashMap<>(initial.resources());
        var troops = new HashMap<>(initial.troops());
        var heroes = new HashMap<>(initial.heroes());
        var snapshot = copy(initial, resources, troops, heroes, Map.of());
        resources.clear(); troops.clear(); heroes.clear();
        assertEquals(6, snapshot.ownedStates().size());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.resources().clear());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.troops().clear());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.heroes().clear());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.activeMarches().clear());
    }

    @Test void requiredRowsCannotBeRecreatedFromDefaultsDuringColdLoad() {
        var initial = snapshot();
        assertThrows(IllegalArgumentException.class, () -> copy(initial, Map.of(), initial.troops(), initial.heroes(), Map.of()));
        for (TroopType type : TroopType.values()) {
            var troops = new HashMap<>(initial.troops()); troops.remove(type);
            assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), troops, initial.heroes(), Map.of()));
        }
        assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(), Map.of(), Map.of()));
    }

    @Test void optionalResourcesStayAbsentInsteadOfBecomingFakeVersionedZeroRows() {
        var initial = snapshot();
        assertFalse(initial.resources().containsKey(ResourceType.FOOD));
        assertFalse(initial.readVersions().containsKey(new ObjectKey.Resource(PLAYER, ResourceType.FOOD)));
        var resources = new HashMap<>(initial.resources());
        resources.put(ResourceType.GEMS, new VersionedState(4, new ResourceState(PLAYER, ResourceType.GEMS, 0)));
        var changed = copy(initial, resources, initial.troops(), initial.heroes(), Map.of());
        assertEquals(4L, changed.readVersions().get(new ObjectKey.Resource(PLAYER, ResourceType.GEMS)));
        assertFalse(changed.resources().containsKey(ResourceType.FOOD));
    }

    @Test void accountRealmStateKindAndMapKeysMustAllAgree() {
        var initial = snapshot();
        assertThrows(IllegalArgumentException.class, () -> new PlayerSnapshot(REALM, ACCOUNT + 1, 100,
                initial.profile(), initial.resources(), initial.troops(), initial.heroes(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new PlayerSnapshot(new RealmIdentity(20, UUID.randomUUID()), ACCOUNT, 100,
                initial.profile(), initial.resources(), initial.troops(), initial.heroes(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new PlayerSnapshot(REALM, ACCOUNT, 100,
                Optional.of(initial.resources().get(ResourceType.WOOD)), initial.resources(), initial.troops(), initial.heroes(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> copy(initial,
                Map.of(ResourceType.WOOD, new VersionedState(1, new ResourceState("17_1", ResourceType.WOOD, 0))),
                initial.troops(), initial.heroes(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> copy(initial,
                Map.of(ResourceType.WOOD, new VersionedState(1, new ResourceState(PLAYER, ResourceType.FOOD, 0))),
                initial.troops(), initial.heroes(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(),
                Map.of("sergey", new VersionedState(1, new HeroState(PLAYER, "other", null))), Map.of()));
    }

    @Test void aMissingProfileCannotHideOrphanAssets() {
        var initial = snapshot();
        assertThrows(IllegalArgumentException.class, () -> new PlayerSnapshot(REALM, ACCOUNT, 100, Optional.empty(),
                initial.resources(), initial.troops(), initial.heroes(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new PlayerSnapshot(REALM, ACCOUNT, -1, Optional.empty(),
                Map.of(), Map.of(), Map.of(), Map.of()));
    }

    @Test void activeMarchAndHeroOccupancyMustDescribeTheSameReservation() {
        var initial = snapshot();
        var march = march("march", PLAYER, MarchState.Status.OUTBOUND, List.of("sergey"));
        var heroes = Map.of("sergey", new VersionedState(2, new HeroState(PLAYER, "sergey", "march")));
        var marches = Map.of("march", new VersionedState(3, march));
        var valid = copy(initial, initial.resources(), initial.troops(), heroes, marches);
        assertEquals(3L, valid.readVersions().get(march.key()));
        assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(), heroes, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(), initial.heroes(), marches));
        var unknownHero = march("march", PLAYER, MarchState.Status.OUTBOUND, List.of("unknown"));
        assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(), heroes,
                Map.of("march", new VersionedState(3, unknownHero))));
        var repeated = new HashMap<>(marches);
        repeated.put("second", new VersionedState(1, march("second", PLAYER, MarchState.Status.OUTBOUND, List.of("sergey"))));
        assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(), heroes, repeated));
    }

    @Test void completedForeignOrMiskeyedMarchesCannotAppearInTheActiveAggregate() {
        var initial = snapshot();
        for (MarchState invalid : List.of(march("m", PLAYER, MarchState.Status.COMPLETED, List.of()),
                march("m", "17_1", MarchState.Status.OUTBOUND, List.of()), march("another", PLAYER, MarchState.Status.OUTBOUND, List.of())))
            assertThrows(IllegalArgumentException.class, () -> copy(initial, initial.resources(), initial.troops(), initial.heroes(),
                    Map.of("m", new VersionedState(1, invalid))));
    }
}

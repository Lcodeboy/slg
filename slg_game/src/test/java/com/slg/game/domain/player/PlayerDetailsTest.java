package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.slg.game.domain.player.PlayerTestData.*;
import static org.junit.jupiter.api.Assertions.*;

class PlayerDetailsTest {
    @Test void creationResponseKeepsVersionOneAndContainsNoUnallocatedSequenceOrDataset() {
        var details = PlayerDetails.created(PlayerCreation.initialStates(REALM, ACCOUNT, "Alice"));
        assertEquals(1, details.version());
        assertEquals(Map.of(ResourceType.WOOD, 0L), details.resources());
        assertEquals(3000, details.troops().get(TroopType.INFANTRY).available());
        assertEquals(List.of(new HeroState(PLAYER, "sergey", null)), details.heroes());
        assertTrue(details.activeMarchIds().isEmpty());
        assertEquals(Set.of("version", "profile", "resources", "troops", "heroes", "activeMarchIds"),
                new HashSet<>(Arrays.stream(PlayerDetails.class.getRecordComponents()).map(java.lang.reflect.RecordComponent::getName).toList()));
    }

    @Test void currentDetailsUseProfileVersionRatherThanWatermarkOrAssetVersions() {
        var snapshot = snapshot();
        var current = PlayerDetails.from(snapshot);
        assertEquals(7, current.version());
        assertNotEquals(snapshot.highSequence(), current.version());
        assertNotEquals(snapshot.resources().get(ResourceType.WOOD).version(), current.version());
        var original = PlayerDetails.created(PlayerCreation.initialStates(REALM, ACCOUNT, "Alice"));
        assertEquals(1, original.version());
    }

    @Test void missingRoleRemainsBusinessNotFoundInsteadOfEmptySuccessDetails() {
        var error = assertThrows(GameException.class, () -> PlayerDetails.from(PlayerSnapshot.absent(REALM, ACCOUNT, 0)));
        assertEquals("PLAYER_NOT_FOUND", error.code());
    }

    @Test void detailsDefensivelyCopyAllCollections() {
        var original = PlayerDetails.from(snapshot());
        var resources = new HashMap<>(original.resources());
        var troops = new HashMap<>(original.troops());
        var heroes = new ArrayList<>(original.heroes());
        var marches = new ArrayList<String>();
        var copy = new PlayerDetails(original.version(), original.profile(), resources, troops, heroes, marches);
        resources.clear(); troops.clear(); heroes.clear(); marches.add("later");
        assertEquals(original, copy);
        assertThrows(UnsupportedOperationException.class, () -> copy.resources().clear());
        assertThrows(UnsupportedOperationException.class, () -> copy.troops().clear());
        assertThrows(UnsupportedOperationException.class, () -> copy.heroes().clear());
        assertThrows(UnsupportedOperationException.class, () -> copy.activeMarchIds().clear());
    }

    @Test void detailsRejectForeignTroopsHeroesDuplicateIdsAndDanglingAssignments() {
        var details = PlayerDetails.from(snapshot());
        var troops = new HashMap<>(details.troops());
        troops.put(TroopType.INFANTRY, new TroopState("17_1", TroopType.INFANTRY, 3000, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new PlayerDetails(1, details.profile(), details.resources(), troops, details.heroes(), List.of()));
        for (List<HeroState> invalid : List.of(List.of(new HeroState("17_1", "sergey", null)),
                List.of(new HeroState(PLAYER, "sergey", null), new HeroState(PLAYER, "sergey", null)),
                List.of(new HeroState(PLAYER, "sergey", "missing"))))
            assertThrows(IllegalArgumentException.class, () -> new PlayerDetails(1, details.profile(), details.resources(), details.troops(), invalid, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new PlayerDetails(1, details.profile(), details.resources(), details.troops(), details.heroes(), List.of("m", "m")));
        assertThrows(IllegalArgumentException.class, () -> new PlayerDetails(1, details.profile(), Map.of(ResourceType.WOOD, -1L), details.troops(), details.heroes(), List.of()));
    }

    @Test void creationProjectionChecksRootCountDuplicatesKindsAndResourceOwnershipBeforeFlattening() {
        var rows = PlayerCreation.initialStates(REALM, ACCOUNT, "Alice");
        assertThrows(IllegalArgumentException.class, () -> PlayerDetails.created(rows.subList(0, 5)));
        var duplicate = new ArrayList<>(rows); duplicate.set(2, rows.get(1));
        assertThrows(IllegalArgumentException.class, () -> PlayerDetails.created(duplicate));
        var foreign = new ArrayList<>(rows); foreign.set(1, new ResourceState("17_1", ResourceType.WOOD, 0));
        assertThrows(IllegalArgumentException.class, () -> PlayerDetails.created(foreign));
        var wrongKind = new ArrayList<>(rows); wrongKind.set(5, march("m", PLAYER, MarchState.Status.COMPLETED, List.of()));
        assertThrows(IllegalArgumentException.class, () -> PlayerDetails.created(wrongKind));
    }
}

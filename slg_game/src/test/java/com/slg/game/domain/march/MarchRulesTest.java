package com.slg.game.domain.march;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerCreation;
import com.slg.game.domain.player.PlayerSnapshot;
import org.junit.jupiter.api.Test;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MarchRulesTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.fromString("66a5245b-03ef-43b6-8e47-20921c382f73"));
    private static final String MARCH = MarchIdentity.start(1, 7, "start-1");

    @Test void startKeepsTypedReservationsAndRejectsWithoutPartialChanges() {
        var before = initial();
        var started = MarchRules.start(before, MARCH, order(null, null), 1000);
        assertNull(started.errorCode()); assertEquals(4, started.changes().size());
        assertEquals(2880, changed(started, new ObjectKey.Troop("1_7", TroopType.INFANTRY), TroopState.class).available());
        assertEquals(MARCH, changed(started, new ObjectKey.Hero("1_7", "sergey"), HeroState.class).assignedMarchId());
        assertEquals(4000, started.march().arriveAt());
        assertEquals(1, changed(started, new ObjectKey.Task(MarchIdentity.arrival(MARCH)), TaskState.class).generation());
        assertTrue(started.changes().stream().noneMatch(change -> change.afterState() instanceof GatherSiteState || change.afterState() instanceof PlayerState));
        var duplicate = new StartMarchOrder(null, 150, 150, null,
                List.of(new StartMarchOrder.TroopSelection(1, 120), new StartMarchOrder.TroopSelection(1, 0)), List.of());
        assertEquals("INVALID_ARGUMENT", MarchRules.start(before, MARCH, duplicate, 1000).errorCode());
        assertTrue(MarchRules.start(before, MARCH, duplicate, 1000).changes().isEmpty());
        var withoutHero = MarchRules.start(before, MARCH, order(null, List.of()), 1000);
        assertEquals(List.of(), withoutHero.march().heroIds());
        assertEquals(MARCH, MarchIdentity.start(1, 7, "start-1"));
        assertNotEquals(MARCH, MarchIdentity.start(2, 7, "start-1"));
    }

    @Test void earlyRecallCancelsOriginalTaskWithoutReturningTroopsOrHeroes() {
        var started = started(); var before = apply(initial(), started);
        var recalled = MarchRules.recall(before, marchRow(before), tasks(started), Optional.empty(), 2000);
        assertNull(recalled.errorCode()); assertEquals(MarchState.Status.RETURNING, recalled.march().status());
        assertEquals(50d, recalled.march().fromX()); assertEquals(5000, recalled.march().arriveAt());
        assertEquals(TaskState.Status.CANCELLED, changed(recalled, new ObjectKey.Task(MarchIdentity.arrival(MARCH)), TaskState.class).status());
        assertEquals(2, changed(recalled, new ObjectKey.Task(MarchIdentity.returning(MARCH)), TaskState.class).generation());
        assertTrue(recalled.changes().stream().noneMatch(change -> change.afterState() instanceof TroopState || change.afterState() instanceof HeroState));
        assertEquals(1, recalled.changes().stream().filter(change -> change.afterState() instanceof MarchState).findFirst().orElseThrow().expectedVersion());
    }

    @Test void catchUpFoldsArrivalGatherAndReturnIntoOneVersionPerRoot() {
        var started = started(); var before = apply(initial(), started);
        var shortlyAfterArrival = MarchRules.recall(before, marchRow(before), tasks(started), Optional.empty(), 4500);
        assertNull(shortlyAfterArrival.errorCode());
        assertEquals(List.of("MARCH_GATHER_STARTED", "MARCH_RECALLED"), shortlyAfterArrival.phases());
        assertEquals(TaskState.Status.CANCELLED, changed(shortlyAfterArrival, new ObjectKey.Task(MarchIdentity.gather(MARCH)), TaskState.class).status());
        assertNull(changed(shortlyAfterArrival, new ObjectKey.GatherSite(150, 150), GatherSiteState.class).occupantMarchId());

        var late = MarchRules.recall(before, marchRow(before), tasks(started), Optional.empty(), 12000);
        assertEquals("INVALID_MARCH_STATE", late.errorCode()); assertNull(late.march());
        assertEquals(List.of("MARCH_GATHER_STARTED", "MARCH_GATHER_FINISHED", "MARCH_COMPLETED"), late.phases());
        assertEquals(MarchState.Status.COMPLETED, changed(late, new ObjectKey.March(MARCH), MarchState.class).status());
        assertEquals(3000, changed(late, new ObjectKey.Troop("1_7", TroopType.INFANTRY), TroopState.class).available());
        assertEquals(600, changed(late, new ObjectKey.Resource("1_7", ResourceType.WOOD), ResourceState.class).amount());
        assertNull(changed(late, new ObjectKey.Hero("1_7", "sergey"), HeroState.class).assignedMarchId());
        assertEquals(late.changes().size(), late.changes().stream().map(ObjectChange::key).distinct().count());
        assertEquals(2, changed(late, new ObjectKey.Task(MarchIdentity.returning(MARCH)), TaskState.class).generation());
        assertEquals(TaskState.Status.DONE, changed(late, new ObjectKey.Task(MarchIdentity.returning(MARCH)), TaskState.class).status());
        assertTrue(apply(before, late).activeMarches().isEmpty());
    }

    @Test void occupiedTargetReturnsEmptyAndCorruptTaskCannotReleaseOrReward() {
        var started = started(); var before = apply(initial(), started);
        var otherSite = new VersionedState(8, new GatherSiteState(150, 150, "1_8", "other-march", 3));
        var occupied = MarchRules.recall(before, marchRow(before), tasks(started), Optional.of(otherSite), 4500);
        assertEquals("INVALID_MARCH_STATE", occupied.errorCode());
        assertEquals(List.of("MARCH_TARGET_OCCUPIED"), occupied.phases());
        assertTrue(occupied.changes().stream().noneMatch(change -> change.afterState() instanceof GatherSiteState));
        assertEquals(Map.of(), changed(occupied, new ObjectKey.March(MARCH), MarchState.class).cargo());
        var arrival = (TaskState) tasks(started).get(MarchIdentity.arrival(MARCH)).state();
        var bad = new TaskState(arrival.taskId(), arrival.playerId(), arrival.marchId(), arrival.type(), arrival.status(), 9, arrival.dueAt(), null);
        assertThrows(MarchDataException.class, () -> MarchRules.recall(before, marchRow(before),
                Map.of(bad.taskId(), new VersionedState(1, bad)), Optional.empty(), 12000));
    }

    @Test void npcCatchUpFixesBattleInputAndRejectsRecallWithoutDispatching() {
        var start = MarchRules.start(initial(), MARCH, new StartMarchOrder(2, 120, 120, 1,
                List.of(new StartMarchOrder.TroopSelection(1, 120)), null), 1000);
        var before = apply(initial(), start);
        var arrived = MarchRules.recall(before, marchRow(before), tasks(start), Optional.empty(), 4500);
        assertEquals("INVALID_MARCH_STATE", arrived.errorCode());
        assertEquals(List.of("MARCH_BATTLE_PREPARED"), arrived.phases());
        assertEquals(MarchState.Status.BATTLE_PENDING, changed(arrived, new ObjectKey.March(MARCH), MarchState.class).status());
        assertNotNull(arrived.pendingBattle()); assertEquals(2, arrived.pendingBattle().marchVersion());
        assertEquals(MARCH, arrived.pendingBattle().input().battleId());
        assertEquals(120, arrived.pendingBattle().input().attacker().groups().getFirst().count());
        assertEquals("MARCH_NOT_FOUND", MarchRules.recall(before, null, Map.of(), Optional.empty(), 4500).errorCode());
    }

    private static StartMarchOrder order(Integer mission, List<String> heroes) {
        return new StartMarchOrder(mission, 150, 150, null, List.of(new StartMarchOrder.TroopSelection(1, 120)), heroes);
    }
    private static MarchTransition started() { return MarchRules.start(initial(), MARCH, order(null, null), 1000); }
    private static VersionedState marchRow(PlayerSnapshot snapshot) { return snapshot.activeMarches().get(MARCH); }
    private static Map<String, VersionedState> tasks(MarchTransition result) {
        var tasks = new HashMap<String, VersionedState>();
        result.changes().forEach(change -> { if (change.afterState() instanceof TaskState task)
            tasks.put(task.taskId(), new VersionedState(change.expectedVersion() + 1, task)); });
        return tasks;
    }
    private static PlayerSnapshot initial() {
        var rows = PlayerCreation.initialStates(REALM, 7, "Alice").stream()
                .map(state -> new ObjectChange(state.key(), 0, state)).toList();
        return apply(PlayerSnapshot.absent(REALM, 7, 0), rows);
    }
    private static PlayerSnapshot apply(PlayerSnapshot before, MarchTransition result) {
        return apply(before, result.changes());
    }
    private static PlayerSnapshot apply(PlayerSnapshot before, List<ObjectChange> changes) {
        var profile = before.profile();
        var resources = new EnumMap<ResourceType, VersionedState>(ResourceType.class); resources.putAll(before.resources());
        var troops = new EnumMap<TroopType, VersionedState>(TroopType.class); troops.putAll(before.troops());
        var heroes = new HashMap<>(before.heroes()); var marches = new HashMap<>(before.activeMarches());
        for (var change : changes) {
            var row = new VersionedState(change.expectedVersion() + 1, change.afterState());
            switch (change.afterState()) {
                case PlayerState ignored -> profile = Optional.of(row);
                case ResourceState state -> resources.put(state.resourceType(), row);
                case TroopState state -> troops.put(state.troopType(), row);
                case HeroState state -> heroes.put(state.heroId(), row);
                case MarchState state -> { if (state.status() == MarchState.Status.COMPLETED) marches.remove(state.marchId()); else marches.put(state.marchId(), row); }
                default -> { }
            }
        }
        return new PlayerSnapshot(before.identity(), before.accountId(), before.highSequence() + 1, profile, resources, troops, heroes, marches);
    }
    private static <T extends ObjectState> T changed(MarchTransition result, ObjectKey key, Class<T> type) {
        return type.cast(result.changes().stream().filter(change -> change.key().equals(key)).findFirst().orElseThrow().afterState());
    }
}

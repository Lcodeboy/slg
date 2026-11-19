package com.slg.game.domain.march;

import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 只覆盖后台与既有召回不同的边界：单阶段、旧候选/坏任务区分、结算与返城的资产边界。 */
class BackgroundMarchRulesTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.fromString("dfb45f3d-cb1f-4da9-b1ec-8e1e46f31c2f"));
    private static final String ID = MarchIdentity.start(1, 7, "background");

    @Test void overdueWorkAdvancesOneStageAndDeliversOnceAfterPersistentReturn() {
        var started = start(1, 120); var player = apply(initial(), started.changes());
        var tasks = taskRows(started.changes());
        var arrival = tasks.get(MarchIdentity.arrival(ID));
        var first = MarchRules.advance(player, arrival, march(player), tasks, Optional.empty(), 99_000);
        assertEquals(DueTransition.Status.APPLIED, first.status());
        assertEquals(List.of("MARCH_GATHER_STARTED"), first.phases());
        var gathering = changed(first.changes(), new ObjectKey.March(ID), MarchState.class);
        assertEquals(MarchState.Status.GATHERING, gathering.status()); assertEquals(4000, gathering.departAt());
        assertEquals(7000, gathering.arriveAt(), "Delayed scanning must not move the durable due time");
        player = apply(player, first.changes()); tasks = mergeTasks(tasks, first.changes());
        assertEquals(DueTransition.Status.SKIPPED, MarchRules.advance(player, arrival, march(player), tasks,
                Optional.empty(), 99_000).status());
        var site = row(first.changes(), new ObjectKey.GatherSite(150,150));
        var second = MarchRules.advance(player, tasks.get(MarchIdentity.gather(ID)), march(player), tasks, Optional.of(site), 99_000);
        assertEquals(List.of("MARCH_GATHER_FINISHED"), second.phases());
        assertTrue(second.changes().stream().noneMatch(change -> change.afterState() instanceof ResourceState));
        var returning = changed(second.changes(), new ObjectKey.March(ID), MarchState.class);
        assertEquals(600, returning.cargo().get(ResourceType.WOOD)); assertEquals(7000, returning.departAt());
        player = apply(player, second.changes()); tasks = mergeTasks(tasks, second.changes());
        var third = MarchRules.advance(player, tasks.get(MarchIdentity.returning(ID)), march(player), tasks, Optional.empty(), 99_000);
        var finished = apply(player, third.changes());
        assertEquals(3000, ((TroopState) finished.troops().get(TroopType.INFANTRY).state()).available());
        assertEquals(600, ((ResourceState) finished.resources().get(ResourceType.WOOD).state()).amount());
        assertTrue(finished.activeMarches().isEmpty());
    }

    @Test void staleCandidateSkipsWhileMatchingBrokenGenerationOnlyBlocksItsTask() {
        var started = start(1, 120); var player = apply(initial(), started.changes()); var tasks = taskRows(started.changes());
        var original = tasks.get(MarchIdentity.arrival(ID)); var task = (TaskState) original.state();
        assertEquals(DueTransition.Status.SKIPPED, MarchRules.advance(player, original, march(player), tasks, Optional.empty(), 3999).status());
        var badTask = new TaskState(task.taskId(), task.playerId(), task.marchId(), task.type(), task.status(), 99, task.dueAt(), null);
        var bad = new VersionedState(2, badTask); var current = Map.of(task.taskId(), bad);
        assertEquals(DueTransition.Status.SKIPPED, MarchRules.advance(player, original, march(player), current, Optional.empty(), 5000).status());
        var blocked = MarchRules.advance(player, bad, march(player), current, Optional.empty(), 5000);
        assertEquals(DueTransition.Status.BLOCKED, blocked.status()); assertEquals(1, blocked.changes().size());
        assertEquals(2, blocked.changes().getFirst().expectedVersion());
        var result = (TaskState) blocked.changes().getFirst().afterState();
        assertEquals(TaskState.Status.BLOCKED, result.status()); assertNotNull(result.reason());
        assertEquals(99, result.generation()); assertNull(blocked.pendingBattle());

        var npcStart = start(2,120); var npcPlayer = apply(initial(),npcStart.changes()); var npcTasks = taskRows(npcStart.changes());
        var collision = new VersionedState(4, new BattleState(ID,ID,"1_7",BattleState.Status.PENDING,List.of("sergey"),
                "codec-validated-hash",ExpeditionBattleEngine.RULES_VERSION,2,new JsonPayload(1,"{}"),null));
        var duplicateBattle = MarchRules.advance(npcPlayer,npcTasks.get(MarchIdentity.arrival(ID)),march(npcPlayer),
                npcTasks,Optional.empty(),Optional.of(collision),5000);
        assertEquals(DueTransition.Status.BLOCKED,duplicateBattle.status());
        assertEquals(1,duplicateBattle.changes().size()); assertNull(duplicateBattle.pendingBattle());
        assertEquals(4,duplicateBattle.readVersions().get(new ObjectKey.Battle(ID)));
        assertEquals(TaskState.Status.BLOCKED,((TaskState)duplicateBattle.changes().getFirst().afterState()).status());
    }

    @Test void settlementRecordsLossesButRetainsSurvivorsRewardsHeroesUntilReturn() {
        var started = start(2, 1000); var player = apply(initial(), started.changes()); var tasks = taskRows(started.changes());
        var arrived = MarchRules.advance(player, tasks.get(MarchIdentity.arrival(ID)), march(player), tasks, Optional.empty(), 4000);
        player = apply(player, arrived.changes());
        var prepared = arrived.pendingBattle(); assertNotNull(prepared);
        var battle = new BattleState(ID, ID, "1_7", BattleState.Status.PENDING, prepared.heroIds(), "codec-validated-hash",
                prepared.rulesVersion(), prepared.marchVersion(), new JsonPayload(1, "{}"), null);
        var battleRow = new VersionedState(1, battle); var marchRow = march(player);
        var result = new ExpeditionBattleEngine().simulate(prepared.input());
        var settled = NpcBattleRules.settle(player, battleRow, marchRow, Optional.empty(), prepared.input(), result, 4500);
        assertEquals(MarchState.Status.RETURNING, settled.returning().status()); assertEquals(4500, settled.returning().departAt());
        assertEquals(3, changed(settled.changes(), new ObjectKey.Task(MarchIdentity.returning(ID)), TaskState.class).generation());
        var after = apply(player, settled.changes()); var troop = (TroopState) after.troops().get(TroopType.INFANTRY).state();
        assertEquals(2000, troop.available(), "Survivors remain in the returning march");
        assertEquals(result.attacker().wounded(), troop.wounded()); assertEquals(result.attacker().dead(), troop.dead());
        assertEquals(ID, ((HeroState) after.heroes().get("sergey").state()).assignedMarchId());
        assertEquals(0, ((ResourceState) after.resources().get(ResourceType.WOOD).state()).amount());
        assertEquals(result.attacker().survivors(), settled.returning().troops().values().stream().mapToLong(Long::longValue).sum());
        assertEquals(Outcome.ATTACKER_WIN, result.outcome());
        assertEquals(Map.of(ResourceType.WOOD,500L,ResourceType.FOOD,500L,ResourceType.GEMS,10L), settled.returning().cargo());
        var wrongSeed = new BattleResult(result.battleId(), result.outcome(), result.rounds(), result.attacker(), result.defender(),
                result.rulesVersion(), result.seed() + 1);
        var fixedPlayer = player;
        assertThrows(MarchDataException.class, () -> NpcBattleRules.settle(fixedPlayer, battleRow, marchRow,
                Optional.empty(), prepared.input(), wrongSeed, 4500));
        assertThrows(MarchDataException.class, () -> NpcBattleRules.validateForDispatch(fixedPlayer, battleRow,
                new VersionedState(marchRow.version() + 1, marchRow.state()), prepared.input()));
    }

    private static MarchTransition start(int mission, long count) {
        int target = mission == 1 ? 150 : 120;
        return MarchRules.start(initial(), ID, new StartMarchOrder(mission,target,target,1,
                List.of(new StartMarchOrder.TroopSelection(1,count)),null),1000);
    }
    private static VersionedState march(PlayerSnapshot player) { return player.activeMarches().get(ID); }
    private static Map<String,VersionedState> taskRows(List<ObjectChange> changes) { return mergeTasks(Map.of(), changes); }
    private static Map<String,VersionedState> mergeTasks(Map<String,VersionedState> old,List<ObjectChange> changes) {
        var result = new HashMap<>(old);
        changes.forEach(change -> { if (change.afterState() instanceof TaskState task)
            result.put(task.taskId(),new VersionedState(change.expectedVersion()+1,task)); });
        return result;
    }
    private static VersionedState row(List<ObjectChange> changes,ObjectKey key) {
        var change=changes.stream().filter(value->value.key().equals(key)).findFirst().orElseThrow();
        return new VersionedState(change.expectedVersion()+1,change.afterState());
    }
    private static <T> T changed(List<ObjectChange> changes,ObjectKey key,Class<T> type) { return type.cast(row(changes,key).state()); }
    private static PlayerSnapshot initial() {
        return apply(PlayerSnapshot.absent(REALM,7,0),PlayerCreation.initialStates(REALM,7,"Alice").stream()
                .map(state->new ObjectChange(state.key(),0,state)).toList());
    }
    private static PlayerSnapshot apply(PlayerSnapshot before,List<ObjectChange> changes) {
        var profile=before.profile(); var resources=new EnumMap<ResourceType,VersionedState>(ResourceType.class); resources.putAll(before.resources());
        var troops=new EnumMap<TroopType,VersionedState>(TroopType.class); troops.putAll(before.troops());
        var heroes=new HashMap<>(before.heroes()); var marches=new HashMap<>(before.activeMarches());
        for(var change:changes) {
            var row=new VersionedState(change.expectedVersion()+1,change.afterState());
            switch(change.afterState()) {
                case PlayerState ignored -> profile=Optional.of(row);
                case ResourceState value -> resources.put(value.resourceType(),row);
                case TroopState value -> troops.put(value.troopType(),row);
                case HeroState value -> heroes.put(value.heroId(),row);
                case MarchState value -> { if(value.status()==MarchState.Status.COMPLETED) marches.remove(value.marchId()); else marches.put(value.marchId(),row); }
                default -> { }
            }
        }
        return new PlayerSnapshot(before.identity(),before.accountId(),before.highSequence()+1,profile,resources,troops,heroes,marches);
    }
}

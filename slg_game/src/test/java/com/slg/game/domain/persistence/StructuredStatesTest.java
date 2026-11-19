package com.slg.game.domain.persistence;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.slg.core.battle.TroopType.INFANTRY;
import static com.slg.core.economy.ResourceType.FOOD;
import static com.slg.core.economy.ResourceType.WOOD;
import static org.junit.jupiter.api.Assertions.*;

class StructuredStatesTest {
    private static final String PLAYER = "17_100086";

    @Test void compositeKeysDoNotCollapseWhenIdentifiersContainSeparators() {
        var first = new ObjectKey.Hero("a:b", "c");
        var second = new ObjectKey.Hero("a", "b:c");
        assertNotEquals(first, second);
        assertNotEquals(0, first.compareTo(second));
        assertEquals(2, Map.of(first, 1L, second, 2L).size());
        assertNotEquals(new ObjectKey.Hero(PLAYER, "WOOD"), new ObjectKey.Resource(PLAYER, WOOD));
    }

    @Test void lockOrderIsStableAcrossTypesCompositePartsAndNumericCoordinates() {
        var keys = new ArrayList<ObjectKey>(List.of(new ObjectKey.Task("task"), new ObjectKey.GatherSite(10, 1),
                new ObjectKey.GatherSite(2, 11), new ObjectKey.GatherSite(2, 3),
                new ObjectKey.Resource("B", FOOD), new ObjectKey.Resource("A", FOOD),
                new ObjectKey.Resource("A", WOOD), new ObjectKey.Troop("A", TroopType.MARKSMAN),
                new ObjectKey.Troop("A", INFANTRY), new ObjectKey.Player(PLAYER)));
        keys.sort(ObjectKey.ORDER);
        // WOOD 的 code 小于 FOOD；名称字典序相反，不能把整数主键顺序换成枚举名顺序。
        assertEquals(List.of(new ObjectKey.Player(PLAYER), new ObjectKey.Resource("A", WOOD),
                new ObjectKey.Resource("A", FOOD), new ObjectKey.Resource("B", FOOD),
                new ObjectKey.Troop("A", INFANTRY), new ObjectKey.Troop("A", TroopType.MARKSMAN),
                new ObjectKey.GatherSite(2, 3), new ObjectKey.GatherSite(2, 11), new ObjectKey.GatherSite(10, 1),
                new ObjectKey.Task("task")), keys);
        for (ObjectKey left : keys) for (ObjectKey right : keys) {
            assertEquals(-Integer.signum(right.compareTo(left)), Integer.signum(left.compareTo(right)));
            assertEquals(left.equals(right), left.compareTo(right) == 0);
        }
    }

    @Test void aChangeCannotTargetAnotherKeyOrAnotherKindOfRow() {
        var resource = new ResourceState(PLAYER, WOOD, 100);
        assertDoesNotThrow(() -> new ObjectChange(resource.key(), 4, resource));
        assertThrows(IllegalArgumentException.class,
                () -> new ObjectChange(new ObjectKey.Resource(PLAYER, FOOD), 4, resource));
        assertThrows(IllegalArgumentException.class,
                () -> new ObjectChange(new ObjectKey.Player(PLAYER), 4, resource));
        assertThrows(IllegalArgumentException.class,
                () -> new ObjectChange(new ObjectKey.Resource("18_100086", WOOD), 4, resource));
        // 编译期封闭的状态集合中不存在 JSON/任意 Object 兜底；辅助载荷不能成为资源后像。
        assertTrue(ObjectState.class.isSealed());
        assertFalse(ObjectState.class.isAssignableFrom(JsonPayload.class));
        assertTrue(Arrays.stream(ObjectState.class.getPermittedSubclasses()).allMatch(Class::isRecord));
    }

    @Test void profilesKeepPermanentIdentityAndOwnOnlyTheirProfileVersion() {
        var player = new PlayerState(PLAYER, 100086, 17, 18, "玩家");
        assertEquals(new ObjectKey.Player(PLAYER), player.key());
        assertEquals(18, player.currentRealmId());
        assertThrows(IllegalArgumentException.class, () -> new PlayerState("18_100086", 100086, 17, 18, "玩家"));
        assertThrows(IllegalArgumentException.class, () -> new PlayerState("17_0", 0, 17, 17, "玩家"));
        assertThrows(IllegalArgumentException.class, () -> new PlayerState(PLAYER, 100086, 17, 0, "玩家"));
    }

    @Test void balancesAndArmyRowsRejectNegativeAmountsAndPreserveLongPrecision() {
        assertEquals(Long.MAX_VALUE, new ResourceState(PLAYER, WOOD, Long.MAX_VALUE).amount());
        assertEquals(0, new ResourceState(PLAYER, WOOD, 0).amount());
        assertThrows(IllegalArgumentException.class, () -> new ResourceState(PLAYER, WOOD, -1));
        assertThrows(IllegalArgumentException.class, () -> new TroopState(PLAYER, INFANTRY, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new TroopState(PLAYER, INFANTRY, 0, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> new TroopState(PLAYER, INFANTRY, 0, 0, -1));
        assertNull(new HeroState(PLAYER, "hero", null).assignedMarchId());
        assertThrows(IllegalArgumentException.class, () -> new HeroState(PLAYER, "hero", " "));
    }

    @Test void marchCapturesAllTypedChildCollectionsBeforeEnqueue() {
        var troops = new HashMap<>(Map.of(INFANTRY, 120L));
        var heroes = new ArrayList<>(List.of("hero-2", "hero-1"));
        var cargo = new HashMap<>(Map.of(WOOD, 600L));
        var march = march(MarchState.Status.RETURNING, MarchState.Mission.GATHER, "return-task", troops, heroes, cargo);
        troops.clear(); heroes.clear(); cargo.clear();
        assertEquals(Map.of(INFANTRY, 120L), march.troops());
        assertEquals(List.of("hero-2", "hero-1"), march.heroIds());
        assertEquals(Map.of(WOOD, 600L), march.cargo());
        assertThrows(UnsupportedOperationException.class, () -> march.troops().put(INFANTRY, 1L));
        assertThrows(UnsupportedOperationException.class, () -> march.heroIds().clear());
        assertThrows(UnsupportedOperationException.class, () -> march.cargo().clear());
    }

    @Test void marchRejectsMalformedQuantitiesAndStateTaskCombinations() {
        assertThrows(IllegalArgumentException.class, () -> march(MarchState.Status.OUTBOUND,
                MarchState.Mission.GATHER, "task", Map.of(INFANTRY, -1L), List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> march(MarchState.Status.RETURNING,
                MarchState.Mission.GATHER, "task", Map.of(), List.of(), Map.of(WOOD, -1L)));
        assertThrows(IllegalArgumentException.class, () -> march(MarchState.Status.OUTBOUND,
                MarchState.Mission.GATHER, "task", Map.of(), List.of("hero", "hero"), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> march(MarchState.Status.OUTBOUND,
                MarchState.Mission.GATHER, null, Map.of(), List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> march(MarchState.Status.COMPLETED,
                MarchState.Mission.GATHER, "task", Map.of(), List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> march(MarchState.Status.GATHERING,
                MarchState.Mission.ATTACK_NPC, "task", Map.of(), List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new MarchState("march", PLAYER,
                MarchState.Status.OUTBOUND, MarchState.Mission.GATHER, Double.NaN, 0, 150, 150,
                1, 2, 1, Map.of(), List.of(), "task", "rules", Map.of()));
    }

    @Test void typedKeysAndAmountsRejectMissingTypesBeforeEnqueue() {
        assertEquals(new ObjectKey.Resource(PLAYER, WOOD), new ResourceState(PLAYER, WOOD, 10).key());
        assertEquals(new ObjectKey.Troop(PLAYER, INFANTRY), new TroopState(PLAYER, INFANTRY, 10, 0, 0).key());
        assertThrows(NullPointerException.class, () -> new ObjectKey.Resource(PLAYER, null));
        assertThrows(NullPointerException.class, () -> new ObjectKey.Troop(PLAYER, null));
        assertThrows(NullPointerException.class, () -> new ResourceState(PLAYER, null, 10));
        assertThrows(NullPointerException.class, () -> new TroopState(PLAYER, null, 10, 0, 0));
        var troops = new HashMap<TroopType, Long>();
        troops.put(null, 1L);
        assertThrows(NullPointerException.class, () -> march(MarchState.Status.OUTBOUND,
                MarchState.Mission.GATHER, "task", troops, List.of(), Map.of()));
        var cargo = new HashMap<ResourceType, Long>();
        cargo.put(WOOD, null);
        assertThrows(NullPointerException.class, () -> march(MarchState.Status.RETURNING,
                MarchState.Mission.GATHER, "task", Map.of(), List.of(), cargo));
    }

    @Test void gatherSiteRequiresAnEntireReservationOrExplicitEmptyState() {
        assertDoesNotThrow(() -> new GatherSiteState(150, 150, null, null, 0));
        assertDoesNotThrow(() -> new GatherSiteState(150, 150, PLAYER, "march", 2));
        assertThrows(IllegalArgumentException.class, () -> new GatherSiteState(150, 150, PLAYER, null, 2));
        assertThrows(IllegalArgumentException.class, () -> new GatherSiteState(150, 150, PLAYER, "march", 0));
        assertThrows(IllegalArgumentException.class, () -> new GatherSiteState(150, 150, null, null, 1));
    }

    @Test void taskTransitionsRetainIdentityAndBlockedTasksRetainFailureReason() {
        var done = new TaskState("task", PLAYER, "march", TaskState.Type.MARCH_RETURN,
                TaskState.Status.DONE, 3, 1000, null);
        assertEquals(new ObjectKey.Task("task"), done.key());
        assertThrows(IllegalArgumentException.class, () -> new TaskState("task", PLAYER, "march",
                TaskState.Type.MARCH_RETURN, TaskState.Status.BLOCKED, 3, 1000, null));
        assertThrows(IllegalArgumentException.class, () -> new TaskState("task", PLAYER, "march",
                TaskState.Type.MARCH_RETURN, TaskState.Status.PENDING, 0, 1000, null));
    }

    @Test void battlePayloadsAreVersionedAndOnlySettledStateCarriesAResult() {
        var heroes = new ArrayList<>(List.of("hero"));
        var input = new JsonPayload(1, "{\"fixed\":true}");
        var battle = new BattleState("battle", "march", PLAYER, BattleState.Status.PENDING,
                heroes, "hash", "rules", 3, input, null);
        heroes.clear();
        assertEquals(List.of("hero"), battle.heroIds());
        assertThrows(UnsupportedOperationException.class, () -> battle.heroIds().clear());
        assertThrows(IllegalArgumentException.class, () -> new BattleState("battle", "march", PLAYER,
                BattleState.Status.SETTLED, List.of(), "hash", "rules", 3, input, null));
        assertThrows(IllegalArgumentException.class, () -> new BattleState("battle", "march", PLAYER,
                BattleState.Status.PENDING, List.of(), "hash", "rules", 3, input, new JsonPayload(1, "{}")));
        assertDoesNotThrow(() -> new BattleState("battle", "march", PLAYER, BattleState.Status.SETTLED,
                List.of(), "hash", "rules", 3, input, new JsonPayload(1, "{}")));
    }

    private static MarchState march(MarchState.Status status, MarchState.Mission mission, String taskId,
                                     Map<TroopType, Long> troops, List<String> heroes, Map<ResourceType, Long> cargo) {
        return new MarchState("march", PLAYER, status, mission, 1.25, 2.5, 150, 150,
                1000, 2000, 1, troops, heroes, taskId, "rules", cargo);
    }
}

package com.slg.game.domain.persistence;

import com.slg.core.type.IntCodeEnum;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

class PersistentEnumCodesTest {
    @Test void objectKindCodesRemainStableAndRejectUnknownObjects() {
        assertCodes(ObjectKind.values(), Map.of(
                ObjectKind.PLAYER, 1, ObjectKind.RESOURCE, 2, ObjectKind.TROOP, 3, ObjectKind.HERO, 4,
                ObjectKind.MARCH, 5, ObjectKind.GATHER_SITE, 6, ObjectKind.TASK, 7, ObjectKind.BATTLE, 8),
                ObjectKind::fromCode);
    }

    @Test void objectKindsKeepTheirSeparateLockOrderContract() {
        var lockOrders = Map.of(ObjectKind.PLAYER, 10, ObjectKind.RESOURCE, 20, ObjectKind.TROOP, 30,
                ObjectKind.HERO, 40, ObjectKind.MARCH, 50, ObjectKind.GATHER_SITE, 60,
                ObjectKind.TASK, 70, ObjectKind.BATTLE, 80);
        lockOrders.forEach((kind, order) -> {
            assertEquals(order.intValue(), kind.lockOrder());
            assertNotEquals(kind.code(), kind.lockOrder());
        });
    }

    @Test void marchStatusCodesRemainStableAndRejectUnknownStages() {
        assertCodes(MarchState.Status.values(), Map.of(
                MarchState.Status.OUTBOUND, 1, MarchState.Status.GATHERING, 2,
                MarchState.Status.BATTLE_PENDING, 3, MarchState.Status.RETURNING, 4,
                MarchState.Status.COMPLETED, 5), MarchState.Status::fromCode);
    }

    @Test void marchMissionCodesRemainStableAndRejectUnknownMissions() {
        assertCodes(MarchState.Mission.values(), Map.of(
                MarchState.Mission.GATHER, 1, MarchState.Mission.ATTACK_NPC, 2), MarchState.Mission::fromCode);
    }

    @Test void taskTypeCodesRemainStableAndRejectUnknownExecutions() {
        assertCodes(TaskState.Type.values(), Map.of(
                TaskState.Type.MARCH_ARRIVE, 1, TaskState.Type.GATHER_DONE, 2, TaskState.Type.MARCH_RETURN, 3),
                TaskState.Type::fromCode);
    }

    @Test void taskStatusCodesRemainStableAndRejectUnknownStatuses() {
        assertCodes(TaskState.Status.values(), Map.of(
                TaskState.Status.PENDING, 1, TaskState.Status.DONE, 2,
                TaskState.Status.CANCELLED, 3, TaskState.Status.BLOCKED, 4), TaskState.Status::fromCode);
    }

    @Test void battleStatusCodesRemainStableAndRejectUnknownSettlementStates() {
        assertCodes(BattleState.Status.values(), Map.of(
                BattleState.Status.PENDING, 1, BattleState.Status.SETTLED, 2), BattleState.Status::fromCode);
    }

    private static <E extends Enum<E> & IntCodeEnum> void assertCodes(
            E[] values, Map<E, Integer> expectedCodes, IntFunction<E> fromCode) {
        // 显式常量表锁定已发布编码，不能用 ordinal 或 values() 的位置产生期望值。
        assertEquals(expectedCodes.size(), values.length);
        for (E value : values) {
            assertEquals(expectedCodes.get(value).intValue(), value.code());
            assertSame(value, fromCode.apply(value.code()));
        }
        for (int unknown : new int[]{Integer.MIN_VALUE, -1, 0, expectedCodes.size() + 1, Integer.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> fromCode.apply(unknown));
        }
    }
}

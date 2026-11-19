package com.slg.game.application.port;

import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.BattleState;
import com.slg.game.domain.persistence.JsonPayload;
import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.domain.persistence.ResourceState;
import com.slg.game.domain.persistence.TaskState;
import com.slg.game.domain.persistence.VersionedState;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StructuredReadContractsTest {
    @Test void snapshotEntryRejectsWrongRowIdentityAndWrongObjectKind() {
        var state = new VersionedState(3, new ResourceState("1_7", ResourceType.WOOD, 200));
        assertEquals(state, new RealmStateReader.StateEntry(new ObjectKey.Resource("1_7", ResourceType.WOOD), state).state());
        assertThrows(IllegalArgumentException.class,
                () -> new RealmStateReader.StateEntry(new ObjectKey.Resource("1_8", ResourceType.WOOD), state));
        assertThrows(IllegalArgumentException.class,
                () -> new RealmStateReader.StateEntry(new ObjectKey.Resource("1_7", ResourceType.FOOD), state));
        assertThrows(IllegalArgumentException.class,
                () -> new RealmStateReader.StateEntry(new ObjectKey.Player("1_7"), state));
    }

    @Test void dueDiscoveryKeepsTaskVersionSeparateFromMarchGenerationAndRejectsFinishedTasks() {
        var pending = task(TaskState.Status.PENDING);
        var candidate = new RealmWorkReader.DueTask(2, pending);
        assertEquals(2, candidate.version());
        assertEquals(7, candidate.task().generation());
        assertEquals(new RealmWorkReader.DueCursor(1234, "return-m1"), candidate.cursor());
        assertThrows(IllegalArgumentException.class, () -> new RealmWorkReader.DueTask(0, pending));
        for (var terminal : List.of(TaskState.Status.DONE, TaskState.Status.CANCELLED, TaskState.Status.BLOCKED))
            assertThrows(IllegalArgumentException.class, () -> new RealmWorkReader.DueTask(2, task(terminal)));
    }

    @Test void battleDiscoveryRequiresPendingTypedStateAndPositiveVersion() {
        var input = new JsonPayload(1, "{\"battleId\":\"b1\"}");
        var pending = new BattleState("b1", "m1", "1_7", BattleState.Status.PENDING,
                List.of("hero-1"), "input-hash", "rules-v1", 7, input, null);
        assertEquals(pending, new RealmWorkReader.PendingBattle(3, pending).battle());
        assertThrows(IllegalArgumentException.class, () -> new RealmWorkReader.PendingBattle(0, pending));
        var settled = new BattleState("b1", "m1", "1_7", BattleState.Status.SETTLED,
                pending.heroIds(), pending.inputHash(), pending.rulesVersion(), 7, input,
                new JsonPayload(1, "{\"winner\":\"attacker\"}"));
        assertThrows(IllegalArgumentException.class, () -> new RealmWorkReader.PendingBattle(4, settled));
    }

    private static TaskState task(TaskState.Status status) {
        return new TaskState("return-m1", "1_7", "m1", TaskState.Type.MARCH_RETURN,
                status, 7, 1234, status == TaskState.Status.BLOCKED ? "missing march" : null);
    }
}

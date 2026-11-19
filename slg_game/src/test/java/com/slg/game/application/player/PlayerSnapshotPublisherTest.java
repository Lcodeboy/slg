package com.slg.game.application.player;

import com.slg.core.battle.TroopType;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PlayerSnapshotPublisherTest {
    @Test void publishesWholePlayerMarchTransitionAndRejectsForeignOrStaleCachedRoots() {
        var realm = new RealmIdentity(1, UUID.randomUUID());
        var created = PlayerCreation.create(PlayerSnapshot.absent(realm, 7, 0), "Seven");
        var before = PlayerSnapshotPublisher.apply(PlayerSnapshot.absent(realm, 7, 0), decision(created), 1);
        var march = new MarchState("m1", "1_7", MarchState.Status.OUTBOUND, MarchState.Mission.GATHER,
                100, 100, 150, 150, 1000, 4000, 1, Map.of(TroopType.INFANTRY, 10L), List.of("sergey"),
                "arrive-m1", "gather-v1", Map.of());
        var task = new TaskState("arrive-m1", "1_7", "m1", TaskState.Type.MARCH_ARRIVE,
                TaskState.Status.PENDING, 1, 4000, null);
        var departed = PlayerSnapshotPublisher.apply(before, decision(List.of(
                change(1, new TroopState("1_7", TroopType.INFANTRY, 2990, 0, 0)),
                change(1, new HeroState("1_7", "sergey", "m1")), change(0, march), change(0, task))), 2);
        assertEquals(1, departed.profile().orElseThrow().version());
        assertEquals(2, departed.troops().get(TroopType.INFANTRY).version());
        assertEquals(1, departed.activeMarches().get("m1").version());

        var completed = new MarchState("m1", "1_7", MarchState.Status.COMPLETED, MarchState.Mission.GATHER,
                150, 150, 100, 100, 4000, 7000, 1, march.troops(), march.heroIds(), null, "gather-v1", Map.of());
        var returned = PlayerSnapshotPublisher.apply(departed, decision(List.of(
                change(2, new TroopState("1_7", TroopType.INFANTRY, 3000, 0, 0)),
                change(2, new HeroState("1_7", "sergey", null)), change(1, completed),
                change(8, new GatherSiteState(150, 150, null, null, 0)))), 3);
        assertTrue(returned.activeMarches().isEmpty());
        assertEquals(3, returned.troops().get(TroopType.INFANTRY).version());
        assertEquals(1, returned.profile().orElseThrow().version());
        assertThrows(IllegalArgumentException.class, () -> PlayerSnapshotPublisher.apply(before,
                decision(List.of(change(0, new TroopState("1_8", TroopType.INFANTRY, 1, 0, 0)))), 2));
        assertThrows(IllegalStateException.class, () -> PlayerSnapshotPublisher.apply(before,
                decision(List.of(change(9, new TroopState("1_7", TroopType.INFANTRY, 1, 0, 0)))), 2));
    }

    private static ObjectChange change(long version, ObjectState state) { return new ObjectChange(state.key(), version, state); }
    private static CommitDecision decision(List<ObjectChange> changes) {
        return new CommitDecision(new RequestKey("account:7", "test"), "digest", "TEST", "{}", "{}", changes, Map.of());
    }
}

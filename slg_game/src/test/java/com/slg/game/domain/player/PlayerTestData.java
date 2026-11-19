package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;

import java.util.*;

final class PlayerTestData {
    static final long ACCOUNT = 9_007_199_254_740_993L;
    static final RealmIdentity REALM = new RealmIdentity(17, UUID.fromString("00000000-0000-0000-0000-000000000017"));
    static final String PLAYER = "17_" + ACCOUNT;
    private PlayerTestData() {}

    static PlayerSnapshot snapshot() { return snapshot(PlayerCreation.initialStates(REALM, ACCOUNT, "Alice"), REALM, 7); }

    static PlayerSnapshot snapshot(List<ObjectState> states, RealmIdentity identity, long profileVersion) {
        VersionedState profile = null;
        var resources = new EnumMap<ResourceType, VersionedState>(ResourceType.class);
        var troops = new EnumMap<TroopType, VersionedState>(TroopType.class);
        var heroes = new HashMap<String, VersionedState>();
        var marches = new HashMap<String, VersionedState>();
        for (ObjectState state : states) switch (state) {
            case PlayerState value -> profile = new VersionedState(profileVersion, value);
            case ResourceState value -> resources.put(value.resourceType(), new VersionedState(11, value));
            case TroopState value -> troops.put(value.troopType(), new VersionedState(20 + value.troopType().code(), value));
            case HeroState value -> heroes.put(value.heroId(), new VersionedState(31, value));
            case MarchState value -> marches.put(value.marchId(), new VersionedState(41, value));
            default -> throw new AssertionError("Unexpected fixture row");
        }
        return new PlayerSnapshot(identity, ACCOUNT, 100, Optional.ofNullable(profile), resources, troops, heroes, marches);
    }

    static PlayerSnapshot copy(PlayerSnapshot source, Map<ResourceType, VersionedState> resources,
                                Map<TroopType, VersionedState> troops, Map<String, VersionedState> heroes,
                                Map<String, VersionedState> marches) {
        return new PlayerSnapshot(source.identity(), source.accountId(), source.highSequence(), source.profile(),
                resources, troops, heroes, marches);
    }

    static MarchState march(String id, String player, MarchState.Status status, List<String> heroes) {
        boolean timed = status == MarchState.Status.OUTBOUND || status == MarchState.Status.GATHERING || status == MarchState.Status.RETURNING;
        return new MarchState(id, player, status, status == MarchState.Status.BATTLE_PENDING ? MarchState.Mission.ATTACK_NPC : MarchState.Mission.GATHER,
                0, 0, 150, 150, 1, 1000, 1, Map.of(TroopType.INFANTRY, 100L), heroes,
                timed ? "task-" + id : null, "rules", Map.of());
    }
}

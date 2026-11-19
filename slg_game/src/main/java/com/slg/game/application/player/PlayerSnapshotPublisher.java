package com.slg.game.application.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.*;

/**
 * 将一次新提交的玩家后像装配为私有快照，最后由目录原子替换。
 * 只投影玩家目录实际缓存的根；任务、矿点与战斗仍由权威存储持有，不虚构其缓存版本。
 * 不能用于回执重放或 UNKNOWN 恢复，这两种情况必须重新读取权威状态。
 */
final class PlayerSnapshotPublisher {
    private PlayerSnapshotPublisher() { }

    static PlayerSnapshot apply(PlayerSnapshot before, CommitDecision decision, long sequence) {
        if (sequence <= before.highSequence())
            throw new IllegalStateException("Player publication is not newer than its snapshot");
        var profile = before.profile();
        var resources = new EnumMap<ResourceType, VersionedState>(ResourceType.class); resources.putAll(before.resources());
        var troops = new EnumMap<TroopType, VersionedState>(TroopType.class); troops.putAll(before.troops());
        var heroes = new TreeMap<>(before.heroes());
        var marches = new TreeMap<>(before.activeMarches());
        var priorVersions = before.readVersions();
        for (var change : decision.changes()) {
            String owner = cachedOwner(change.afterState());
            if (owner == null) continue;
            if (!before.playerId().equals(owner))
                throw new IllegalArgumentException("Player publication contains another player's cached root");
            long prior = priorVersions.getOrDefault(change.key(), 0L);
            if (prior != change.expectedVersion())
                throw new IllegalStateException("Player publication version differs from decision");
            var value = new VersionedState(Math.addExact(prior, 1), change.afterState());
            switch (change.afterState()) {
                case PlayerState ignored -> profile = Optional.of(value);
                case ResourceState state -> resources.put(state.resourceType(), value);
                case TroopState state -> troops.put(state.troopType(), value);
                case HeroState state -> heroes.put(state.heroId(), value);
                case MarchState state -> {
                    // 完成行军仍保留数据库根和版本，但不再占用玩家活动集合与行军槽位。
                    if (state.status() == MarchState.Status.COMPLETED) marches.remove(state.marchId());
                    else marches.put(state.marchId(), value);
                }
                default -> throw new IllegalStateException("Unexpected cached player root");
            }
        }
        // 全部后像组装后再校验英雄/活动行军的一致性，避免发布修改到一半的聚合。
        return new PlayerSnapshot(before.identity(), before.accountId(), sequence, profile, resources, troops, heroes, marches);
    }

    private static String cachedOwner(ObjectState state) {
        return switch (state) {
            case PlayerState value -> value.playerId();
            case ResourceState value -> value.playerId();
            case TroopState value -> value.playerId();
            case HeroState value -> value.playerId();
            case MarchState value -> value.playerId();
            default -> null;
        };
    }
}

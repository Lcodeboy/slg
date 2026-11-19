package com.slg.game.application.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmStateReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** 一次加载返回同视图的完整不可变聚合；不发放资产，不操作玩家邮箱或数据库写事务。 */
@FunctionalInterface
public interface PlayerSnapshotLoader {
    CompletionStage<PlayerSnapshot> load(long accountId);

    /** SQL 全部在独立冷读线程执行，准备线程只能等待结果，不能自己打开第二份快照拼接资产。 */
    static PlayerSnapshotLoader using(RealmIdentity identity, RealmStateReader reader,
                                       BoundedReadExecutor reads, ReadBudget budget) {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(reads, "reads");
        Objects.requireNonNull(budget, "budget");
        return accountId -> {
            if (accountId <= 0) throw new IllegalArgumentException("Account ID must be positive");
            return reads.readSnapshot(reader, identity, budget, snapshot -> {
                try {
                    if (!identity.equals(snapshot.identity())) throw new IllegalArgumentException("Snapshot identity differs");
                    long highSequence = snapshot.highSequence();
                    var profile = snapshot.findPlayerByAccount(accountId);
                    if (profile.isEmpty()) {
                        String candidate = PlayerIdentity.atCreation(identity.realmId(), accountId);
                        // 无角色不是“加载失败”的默认值。候选身份只要留下任何归属行就拒绝；包括
                        // 完成行军/任务/战斗，避免错误地建角并覆盖一段残缺历史。第一条即失败。
                        for (ObjectKind kind : ObjectKind.values()) if (kind != ObjectKind.PLAYER)
                            snapshot.scanOwned(kind, candidate, ignored -> {
                                throw new IllegalArgumentException("Missing player profile has orphan owned state");
                            });
                        return PlayerSnapshot.absent(identity, accountId, highSequence);
                    }
                    if (!(profile.orElseThrow().state() instanceof PlayerState player))
                        throw new IllegalArgumentException("Account lookup did not return player profile");
                    var resources = new HashMap<ResourceType, VersionedState>();
                    var troops = new HashMap<TroopType, VersionedState>();
                    var heroes = new HashMap<String, VersionedState>();
                    var marches = new HashMap<String, VersionedState>();
                    snapshot.scanOwned(ObjectKind.RESOURCE, player.playerId(), entry -> {
                        if (!(entry.state().state() instanceof ResourceState value)) throw new IllegalArgumentException("Unexpected resource row");
                        putUnique(resources, value.resourceType(), entry.state());
                    });
                    snapshot.scanOwned(ObjectKind.TROOP, player.playerId(), entry -> {
                        if (!(entry.state().state() instanceof TroopState value)) throw new IllegalArgumentException("Unexpected troop row");
                        putUnique(troops, value.troopType(), entry.state());
                    });
                    snapshot.scanOwned(ObjectKind.HERO, player.playerId(), entry -> {
                        if (!(entry.state().state() instanceof HeroState value)) throw new IllegalArgumentException("Unexpected hero row");
                        putUnique(heroes, value.heroId(), entry.state());
                    });
                    // 有角色只查活动行军索引，不把全部完成历史载入内存后过滤。
                    snapshot.scanActiveMarches(player.playerId(), entry -> {
                        if (!(entry.state().state() instanceof MarchState value)) throw new IllegalArgumentException("Unexpected march row");
                        putUnique(marches, value.marchId(), entry.state());
                    });
                    return new PlayerSnapshot(identity, accountId, highSequence, profile, resources, troops, heroes, marches);
                } catch (IllegalArgumentException | NullPointerException invalid) {
                    throw new StorageAccessException(StorageAccessException.Code.INVALID_DATA,
                            "Player snapshot is incomplete or inconsistent");
                }
            }).result();
        };
    }

    private static <K> void putUnique(Map<K, VersionedState> target, K key, VersionedState value) {
        if (target.putIfAbsent(key, value) != null) throw new IllegalArgumentException("Duplicate player state row");
    }
}

package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;

import java.util.*;

/**
 * 一份已完成读取的不可变玩家聚合，保留每类权威根行的独立版本；资料版本不是资产或集合版本。
 * 主行、资产及活动行军必须来自同一身份/水位的一致视图，构造校验只证明内容内部一致，不能
 * 证明调用者实际使用了同一数据库快照，也不授予玩家执行资格。缓存安装/替换须另受玩家资格保护。
 *
 * <p>缺角色用空 profile 表示，不伪造版本 0 的行；此时不得携带资产或行军。已有角色必须保留
 * 首版必需的 WOOD、三个兵种和 sergey 行，缺失按坏数据处理，不在冷加载时重新发放初始资产。
 * FOOD/GEMS 尚未获得时允许缺行，不能因此制造一个已持久化的零余额版本。</p>
 */
public record PlayerSnapshot(RealmIdentity identity, long accountId, long highSequence,
                             Optional<VersionedState> profile,
                             Map<ResourceType, VersionedState> resources,
                             Map<TroopType, VersionedState> troops,
                             Map<String, VersionedState> heroes,
                             Map<String, VersionedState> activeMarches) {
    public PlayerSnapshot {
        Objects.requireNonNull(identity, "identity");
        if (accountId <= 0 || highSequence < 0) throw new IllegalArgumentException("Invalid player snapshot identity or watermark");
        profile = Objects.requireNonNull(profile, "profile");
        resources = Map.copyOf(Objects.requireNonNull(resources, "resources"));
        troops = Map.copyOf(Objects.requireNonNull(troops, "troops"));
        heroes = Map.copyOf(Objects.requireNonNull(heroes, "heroes"));
        activeMarches = Map.copyOf(Objects.requireNonNull(activeMarches, "activeMarches"));

        if (profile.isEmpty()) {
            if (!resources.isEmpty() || !troops.isEmpty() || !heroes.isEmpty() || !activeMarches.isEmpty())
                throw new IllegalArgumentException("Missing player profile cannot own stored child state");
        } else {
            var player = state(profile.orElseThrow(), PlayerState.class);
            if (player.accountId() != accountId || player.currentRealmId() != identity.realmId())
                throw new IllegalArgumentException("Player snapshot belongs to another account or current realm");
            String playerId = player.playerId();
            for (var entry : resources.entrySet()) {
                var value = state(entry.getValue(), ResourceState.class);
                if (entry.getKey() != value.resourceType() || !playerId.equals(value.playerId()))
                    throw new IllegalArgumentException("Resource key or owner differs from player snapshot");
            }
            for (var entry : troops.entrySet()) {
                var value = state(entry.getValue(), TroopState.class);
                if (entry.getKey() != value.troopType() || !playerId.equals(value.playerId()))
                    throw new IllegalArgumentException("Troop key or owner differs from player snapshot");
            }
            for (var entry : heroes.entrySet()) {
                var value = state(entry.getValue(), HeroState.class);
                if (!entry.getKey().equals(value.heroId()) || !playerId.equals(value.playerId()))
                    throw new IllegalArgumentException("Hero key or owner differs from player snapshot");
            }
            for (var entry : activeMarches.entrySet()) {
                var value = state(entry.getValue(), MarchState.class);
                if (!entry.getKey().equals(value.marchId()) || !playerId.equals(value.playerId())
                        || value.status() == MarchState.Status.COMPLETED)
                    throw new IllegalArgumentException("Active march key, owner or state is invalid");
            }
            if (!resources.containsKey(ResourceType.WOOD) || !troops.keySet().containsAll(PlayerCreation.STARTER_TROOPS)
                    || !heroes.containsKey(PlayerCreation.STARTER_HERO))
                throw new IllegalArgumentException("Existing player is missing required initial rows");

            // 英雄占用与活动行军必须双向一致；不能只验证其中一个列表而让重复出征或悬空占用被激活。
            var assignedHeroes = new HashSet<String>();
            for (var row : activeMarches.values()) {
                var march = state(row, MarchState.class);
                for (String heroId : march.heroIds()) {
                    var heroRow = heroes.get(heroId);
                    if (heroRow == null || !assignedHeroes.add(heroId)
                            || !march.marchId().equals(state(heroRow, HeroState.class).assignedMarchId()))
                        throw new IllegalArgumentException("March hero reservation is missing, duplicated or inconsistent");
                }
            }
            for (var row : heroes.values()) {
                var hero = state(row, HeroState.class);
                if (hero.assignedMarchId() != null && !assignedHeroes.contains(hero.heroId()))
                    throw new IllegalArgumentException("Hero reservation has no matching active march");
            }
        }
    }

    public static PlayerSnapshot absent(RealmIdentity identity, long accountId, long highSequence) {
        return new PlayerSnapshot(identity, accountId, highSequence, Optional.empty(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    /** 缺角色时仅生成本区候选键；已有角色始终使用其持久化建角 ID，支持 founding/current 区不同。 */
    public String playerId() {
        return profileState().map(PlayerState::playerId).orElseGet(() -> PlayerIdentity.atCreation(identity.realmId(), accountId));
    }

    public Optional<PlayerState> profileState() { return profile.map(row -> state(row, PlayerState.class)); }

    /** 全部已读取根按统一键序返回；行军明细随行军根版本，不能拆成独立版本或再加一份可写 JSON。 */
    public List<VersionedState> ownedStates() {
        var rows = new ArrayList<VersionedState>();
        profile.ifPresent(rows::add);
        rows.addAll(resources.values()); rows.addAll(troops.values()); rows.addAll(heroes.values()); rows.addAll(activeMarches.values());
        rows.sort(Comparator.comparing(row -> row.state().key(), ObjectKey.ORDER));
        return List.copyOf(rows);
    }

    /**
     * 仅返回实际读取到的根版本；缺角色单独记录候选 Player=0。可选资源缺行若影响裁决，
     * 用例须显式加入该资源键的 0 依赖。未出现在 activeMarches 中不能证明某行军根不存在，
     * 它可能已经完成；本映射也不能证明集合无新增成员，仍需该玩家共同的稳定账号执行资格。
     */
    public Map<ObjectKey, Long> readVersions() {
        var versions = new LinkedHashMap<ObjectKey, Long>();
        if (profile.isEmpty()) versions.put(new ObjectKey.Player(playerId()), 0L);
        else for (VersionedState row : ownedStates()) {
            if (versions.putIfAbsent(row.state().key(), row.version()) != null)
                throw new IllegalStateException("Duplicate root key in player snapshot");
        }
        return Collections.unmodifiableMap(versions);
    }

    private static <T extends ObjectState> T state(VersionedState row, Class<T> expected) {
        if (!expected.isInstance(row.state())) throw new IllegalArgumentException("Unexpected player snapshot row type");
        return expected.cast(row.state());
    }
}

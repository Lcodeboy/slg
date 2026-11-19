package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;

import java.util.*;

/**
 * 客户端详情/原始建角响应的纯领域值，不包含尚未分配的提交序号或数据集信息。
 * version 仅表示资料行版本；资产、英雄和行军仍由权威快照中的各根版本保护。
 * 将它保存为原响应后，重放必须返回当时的详情，不能按当前快照或新配置重算响应。
 */
public record PlayerDetails(long version, PlayerState profile, Map<ResourceType, Long> resources,
                            Map<TroopType, TroopState> troops, List<HeroState> heroes, List<String> activeMarchIds) {
    public PlayerDetails {
        if (version <= 0) throw new IllegalArgumentException("Player profile version must be positive");
        Objects.requireNonNull(profile, "profile");
        resources = Map.copyOf(Objects.requireNonNull(resources, "resources"));
        troops = Map.copyOf(Objects.requireNonNull(troops, "troops"));
        heroes = List.copyOf(Objects.requireNonNull(heroes, "heroes"));
        activeMarchIds = List.copyOf(Objects.requireNonNull(activeMarchIds, "activeMarchIds"));
        for (long amount : resources.values()) if (amount < 0) throw new IllegalArgumentException("Negative player resource amount");
        for (var entry : troops.entrySet()) {
            if (entry.getKey() != entry.getValue().troopType() || !profile.playerId().equals(entry.getValue().playerId()))
                throw new IllegalArgumentException("Player troop details have another key or owner");
        }
        var marchIds = new HashSet<String>();
        for (String id : activeMarchIds) {
            new ObjectKey.March(id);
            if (!marchIds.add(id)) throw new IllegalArgumentException("Duplicate active march ID");
        }
        var heroIds = new HashSet<String>();
        for (HeroState hero : heroes) {
            if (!profile.playerId().equals(hero.playerId()) || !heroIds.add(hero.heroId()))
                throw new IllegalArgumentException("Player hero details have another owner or duplicate ID");
            if (hero.assignedMarchId() != null && !marchIds.contains(hero.assignedMarchId()))
                throw new IllegalArgumentException("Player hero details refer to an absent active march");
        }
        if (!resources.containsKey(ResourceType.WOOD) || !troops.keySet().containsAll(PlayerCreation.STARTER_TROOPS)
                || !heroIds.contains(PlayerCreation.STARTER_HERO))
            throw new IllegalArgumentException("Player details are missing required initial state");
    }

    public static PlayerDetails from(PlayerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        GameException.require(snapshot.profile().isPresent(), "PLAYER_NOT_FOUND", "Create a character first");
        var resources = new EnumMap<ResourceType, Long>(ResourceType.class);
        snapshot.resources().forEach((type, row) -> resources.put(type, ((ResourceState) row.state()).amount()));
        var troops = new EnumMap<TroopType, TroopState>(TroopType.class);
        snapshot.troops().forEach((type, row) -> troops.put(type, (TroopState) row.state()));
        var heroes = snapshot.heroes().values().stream().map(row -> (HeroState) row.state())
                .sorted(Comparator.comparing(HeroState::heroId)).toList();
        var marches = snapshot.activeMarches().keySet().stream().sorted().toList();
        return new PlayerDetails(snapshot.profile().orElseThrow().version(), snapshot.profileState().orElseThrow(),
                resources, troops, heroes, marches);
    }

    /**
     * 把未提交的六条建角后像映射成版本 1 的私有原响应。这里只固定响应，不代表数据库提交成功；
     * 用例必须等待整个创建事务成功及发布完成后才交付。重放不把这个版本改成当前资料版本。
     */
    public static PlayerDetails created(List<ObjectState> sixRows) {
        var rows = List.copyOf(Objects.requireNonNull(sixRows, "sixRows"));
        if (rows.size() != 6) throw new IllegalArgumentException("Initial player must contain six roots");
        PlayerState profile = null;
        var resources = new EnumMap<ResourceType, Long>(ResourceType.class);
        var troops = new EnumMap<TroopType, TroopState>(TroopType.class);
        var heroes = new ArrayList<HeroState>();
        for (ObjectState state : rows) {
            switch (state) {
                case PlayerState player -> {
                    if (profile != null) throw new IllegalArgumentException("Duplicate initial profile");
                    profile = player;
                }
                case ResourceState resource -> {
                    if (resources.putIfAbsent(resource.resourceType(), resource.amount()) != null)
                        throw new IllegalArgumentException("Duplicate initial resource");
                }
                case TroopState troop -> {
                    if (troops.putIfAbsent(troop.troopType(), troop) != null)
                        throw new IllegalArgumentException("Duplicate initial troop");
                }
                case HeroState hero -> heroes.add(hero);
                default -> throw new IllegalArgumentException("Unexpected initial player root type");
            }
        }
        if (profile == null) throw new IllegalArgumentException("Initial player profile is missing");
        // 数量 Map 不保留 ResourceState 的属主，转换前须单独验证，不能因投影而丢掉身份校验。
        for (ObjectState state : rows) if (state instanceof ResourceState resource
                && !profile.playerId().equals(resource.playerId()))
            throw new IllegalArgumentException("Initial resource belongs to another player");
        return new PlayerDetails(1, profile, resources, troops, heroes, List.of());
    }
}

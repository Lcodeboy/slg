package com.slg.core.battle;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 远征战斗中一方军队的不可变输入快照，既可表示玩家，也可表示 NPC。
 * 当前演示规则将英雄效果折算为攻防倍率，军队只保存聚合兵组，不创建逐个士兵实体。
 *
 * @param ownerId 军队所属玩家或 NPC 的标识
 * @param groups 兵组列表，最多 64 组，组标识在本军队内唯一，允许空军队
 * @param attackMultiplier 攻击倍率，有限且位于 [0.1, 10]
 * @param defenseMultiplier 防御倍率，有限且位于 [0.1, 10]
 */
public record Army(String ownerId, List<TroopGroup> groups,
                   double attackMultiplier, double defenseMultiplier) {
    /** 使用中性攻防倍率 1.0 创建军队快照。 */
    public Army(String ownerId, List<TroopGroup> groups) {
        this(ownerId, groups, 1.0, 1.0);
    }

    /** 复制兵组列表并校验容量、标识和倍率，隔离调用方后续对列表的修改。 */
    public Army {
        if (ownerId == null || ownerId.isBlank()) {
            throw new IllegalArgumentException("Army owner is required");
        }
        Objects.requireNonNull(groups, "groups");
        groups = List.copyOf(groups);
        if (groups.size() > 64) throw new IllegalArgumentException("At most 64 troop groups are allowed");
        Set<String> identifiers = new HashSet<>();
        for (TroopGroup group : groups) {
            if (!identifiers.add(group.groupId())) throw new IllegalArgumentException("Duplicate troop group identifier");
        }
        if (!Double.isFinite(attackMultiplier) || !Double.isFinite(defenseMultiplier)
                || attackMultiplier < 0.1 || attackMultiplier > 10
                || defenseMultiplier < 0.1 || defenseMultiplier > 10) {
            throw new IllegalArgumentException("Army multipliers must be finite and within 0.1..10");
        }
    }
}

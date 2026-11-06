package com.slg.core.battle;

import java.util.Objects;

/**
 * 按兵种与等级聚合的兵力快照，一组十亿名士兵也只占用一个兵组对象。
 * 战斗计算使用兵组总生命和数量，计算量不随士兵人数逐个增长。
 *
 * @param groupId 本军队内唯一的兵组标识
 * @param type 兵种，决定本演示版本的基础属性和克制关系
 * @param tier 兵种等级，范围为 1 到 10
 * @param count 参战人数，范围为 0 到 MAX_COUNT，允许空兵组
 */
public record TroopGroup(String groupId, TroopType type, int tier, long count) {
    /** 单兵组人数上限，同时约束总生命和伤害计算的数值范围。 */
    public static final long MAX_COUNT = 1_000_000_000L;

    /** 校验兵组标识、兵种、等级和人数，不读取或扣减玩家兵力库存。 */
    public TroopGroup {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group identifier is required");
        }
        Objects.requireNonNull(type, "type");
        if (tier < 1 || tier > 10 || count < 0 || count > MAX_COUNT) {
            throw new IllegalArgumentException("Tier must be 1..10 and count 0..1000000000");
        }
    }
}

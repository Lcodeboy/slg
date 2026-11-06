package com.slg.core.battle;

import java.util.Objects;

/**
 * 单个聚合兵组的战后数量，严格保持 initialCount = survivors + wounded + dead。
 * 所有数量均以名为单位，伤兵与存活可用兵力分别返回，避免重复回补库存。
 *
 * @param groupId 与参战快照一致的兵组标识
 * @param type 兵种
 * @param tier 兵种等级，范围为 1 到 10
 * @param initialCount 初始参战人数，范围为 0 到 {@link TroopGroup#MAX_COUNT}
 * @param survivors 存活且未计入伤兵的人数
 * @param wounded 受伤人数，是否能够入院由游戏领域处理
 * @param dead 死亡人数
 */
public record GroupResult(String groupId, TroopType type, int tier, long initialCount,
                          long survivors, long wounded, long dead) {
    /** 用逐项扣减校验非负数量与兵力守恒，避免直接求和造成溢出。 */
    public GroupResult {
        if (groupId == null || groupId.isBlank()) throw new IllegalArgumentException("Group identifier is required");
        Objects.requireNonNull(type, "type");
        if (tier < 1 || tier > 10 || initialCount < 0 || initialCount > TroopGroup.MAX_COUNT
                || survivors < 0 || wounded < 0 || dead < 0
                || survivors > initialCount || wounded > initialCount - survivors
                || dead != initialCount - survivors - wounded) {
            throw new IllegalArgumentException("Troop result must conserve its initial population");
        }
    }

    /** 返回受伤与死亡人数之和，即不能直接作为存活兵力返回的部分。 */
    public long losses() {
        return wounded + dead;
    }
}

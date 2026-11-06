package com.slg.core.battle;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 一方军队的战后数量快照，各兵组分别满足初始兵力等于存活、受伤与死亡之和。
 * 本对象提供结算数据，不直接修改玩家库存或医院容量。
 *
 * @param ownerId 军队所属玩家或 NPC 标识
 * @param groups 各兵组的不可变战损明细，最多 64 组且组标识唯一
 */
public record ArmyResult(String ownerId, List<GroupResult> groups) {
    /** 复制结果列表并校验归属标识、组数和组标识唯一性。 */
    public ArmyResult {
        if (ownerId == null || ownerId.isBlank()) throw new IllegalArgumentException("Army owner is required");
        Objects.requireNonNull(groups, "groups");
        groups = List.copyOf(groups);
        if (groups.size() > 64) throw new IllegalArgumentException("At most 64 troop groups are allowed");
        Set<String> identifiers = new HashSet<>();
        for (GroupResult group : groups) {
            if (!identifiers.add(group.groupId())) throw new IllegalArgumentException("Duplicate result identifier");
        }
    }

    /** 返回参战初始总兵力，单位为名。 */
    public long initialCount() { return groups.stream().mapToLong(GroupResult::initialCount).sum(); }
    /** 返回战后存活且未被归入伤兵的总兵力，单位为名。 */
    public long survivors() { return groups.stream().mapToLong(GroupResult::survivors).sum(); }
    /** 返回伤兵总数，是否入院以及何时恢复由外部结算流程决定。 */
    public long wounded() { return groups.stream().mapToLong(GroupResult::wounded).sum(); }
    /** 返回死亡总人数。 */
    public long dead() { return groups.stream().mapToLong(GroupResult::dead).sum(); }
    /** 返回伤兵与死亡人数之和，战损不等于死亡人数。 */
    public long losses() { return groups.stream().mapToLong(GroupResult::losses).sum(); }
}

package com.slg.core.battle;

import java.util.Objects;

/**
 * 远征战斗的完整只读输入，不依赖当前时间或外部库存。
 * 使用同一规则版本和相同输入、随机种子重算时，应得到一致的战斗结果。
 *
 * @param battleId 战斗唯一标识，用于关联任务、战报和外部幂等结算
 * @param attacker 攻击方军队快照
 * @param defender 防守方军队快照，可以属于 NPC
 * @param seed 随机数生成器种子，允许任意 long 值，重试时必须沿用
 * @param maxRounds 最大同步回合数，范围为 1 到 1000，限制单次计算量
 */
public record BattleInput(String battleId, Army attacker, Army defender, long seed, int maxRounds) {
    /** 校验战斗标识、双方快照和回合上限。 */
    public BattleInput {
        if (battleId == null || battleId.isBlank()) throw new IllegalArgumentException("Battle identifier is required");
        Objects.requireNonNull(attacker, "attacker");
        Objects.requireNonNull(defender, "defender");
        if (maxRounds < 1 || maxRounds > 1_000) {
            throw new IllegalArgumentException("Round limit must be within 1..1000");
        }
    }
}

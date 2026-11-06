package com.slg.core.battle;

import java.util.Objects;

/**
 * 纯计算生成的远征战报，包含可供外部持久化、校验和一次性结算的兵力结果。
 * 记录规则版本与随机种子以支持重放；战报本身不代表库存结算已经提交。
 *
 * @param battleId 对应输入的战斗标识
 * @param outcome 胜负结果，达到回合上限且双方仍存活时为平局
 * @param rounds 实际完成的同步回合数；开战时已有一方为空可为 0
 * @param attacker 攻击方各兵组的战后数量
 * @param defender 防守方各兵组的战后数量
 * @param rulesVersion 本次计算使用的规则版本
 * @param seed 本次计算使用的原始随机种子
 */
public record BattleResult(String battleId, Outcome outcome, int rounds,
                           ArmyResult attacker, ArmyResult defender, String rulesVersion, long seed) {
    /** 校验结果元数据及双方快照，不代替外部对原始参战兵力的核对。 */
    public BattleResult {
        if (battleId == null || battleId.isBlank() || rulesVersion == null || rulesVersion.isBlank()
                || rounds < 0 || rounds > 1_000) {
            throw new IllegalArgumentException("Invalid battle result metadata");
        }
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(attacker, "attacker");
        Objects.requireNonNull(defender, "defender");
    }
}

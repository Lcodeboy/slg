package com.slg.game.domain.march;

import com.slg.core.battle.BattleResult;
import com.slg.game.domain.persistence.BattleState;
import java.util.Objects;

/** 私有战报查询结果；原始战损来自已保存结果，PENDING 不伪造空结果或触发重新计算。 */
public record BattleReport(String battleId, String marchId, BattleState.Status status,
                           String rulesVersion, String inputHash, BattleResult result) {
    public BattleReport {
        Objects.requireNonNull(battleId); Objects.requireNonNull(marchId); Objects.requireNonNull(status);
        Objects.requireNonNull(rulesVersion); Objects.requireNonNull(inputHash);
        if ((status == BattleState.Status.SETTLED) != (result != null))
            throw new IllegalArgumentException("Only a settled battle report carries a result");
        if (result != null && (!battleId.equals(result.battleId()) || !rulesVersion.equals(result.rulesVersion())))
            throw new IllegalArgumentException("Battle report result differs from its identity");
    }
}

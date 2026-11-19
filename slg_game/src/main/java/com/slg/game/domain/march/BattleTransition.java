package com.slg.game.domain.march;

import com.slg.core.battle.BattleResult;
import com.slg.game.domain.persistence.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 结算草案：伤亡、幸存者行军和返城任务必须与应用编码后的 SETTLED 战报同事务提交。
 * 领域不编码 JSON；本结果还不是提交完成，也不允许把幸存者或奖励立即加回城内资产。
 */
public record BattleTransition(List<ObjectChange> changes, Map<ObjectKey, Long> readVersions,
                               MarchState returning, BattleResult result) {
    public BattleTransition {
        changes = List.copyOf(changes); readVersions = Map.copyOf(readVersions);
        Objects.requireNonNull(returning); Objects.requireNonNull(result);
        if (returning.status() != MarchState.Status.RETURNING)
            throw new IllegalArgumentException("Battle settlement must create a returning march");
    }
}

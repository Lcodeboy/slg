package com.slg.game.application.background;

import com.slg.core.battle.BattleInput;
import com.slg.game.application.port.RealmWorkReader;
import java.util.Objects;

/** 资格内验证后交给 Worker 的固定输入；计算在资格外进行，结果返回后仍须重新读取权威版本。 */
public record BattleDispatch(RealmWorkReader.PendingBattle candidate, BattleInput input) {
    public BattleDispatch {
        Objects.requireNonNull(candidate); Objects.requireNonNull(input);
        if (!candidate.battle().battleId().equals(input.battleId())
                || !candidate.battle().playerId().equals(input.attacker().ownerId()))
            throw new IllegalArgumentException("Dispatch input differs from the persisted battle");
    }
}

package com.slg.game.domain.march;

import com.slg.core.battle.BattleInput;
import java.util.List;
import java.util.Objects;

/**
 * NPC 到达时固定的纯计算输入。基础设施负责版本化编码与摘要，应用将得到的 BattleState
 * 与行军、原任务及回执同事务提交；这里不调用 Worker，也不在领域内保存 JSON 字符串。
 */
public record PendingBattle(String marchId, String playerId, List<String> heroIds, long marchVersion,
                            String rulesVersion, BattleInput input) {
    public PendingBattle {
        Objects.requireNonNull(marchId); Objects.requireNonNull(playerId); Objects.requireNonNull(rulesVersion);
        heroIds = List.copyOf(heroIds); Objects.requireNonNull(input);
        if (marchVersion <= 0 || !marchId.equals(input.battleId()) || !playerId.equals(input.attacker().ownerId()))
            throw new IllegalArgumentException("Pending battle identity differs from its input");
    }
}

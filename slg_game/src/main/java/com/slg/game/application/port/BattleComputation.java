package com.slg.game.application.port;

import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import java.util.concurrent.CompletionStage;

/**
 * 对固定输入作无状态远程计算。调用期间不持玩家资格；结果必须重新进入有界结算用例核验
 * 持久输入摘要、规则与版本。完成阶段只表示计算结果，不表示资产或战斗已提交。
 */
@FunctionalInterface
public interface BattleComputation {
    CompletionStage<BattleResult> compute(BattleInput input, String persistedHash, String rulesVersion);
}

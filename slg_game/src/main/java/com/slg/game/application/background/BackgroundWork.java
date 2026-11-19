package com.slg.game.application.background;

import com.slg.core.battle.BattleResult;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.player.PlayerCall;
import com.slg.game.application.port.RealmWorkReader;
import java.time.Duration;
import java.util.Optional;

/**
 * 后台发现器与用例执行的边界。发现器不获取玩家缓存或数据库写入口；候选必须交给统一资格
 * 下的用例重新验证，prepareBattle 完成后远程计算才可离开玩家资格执行。
 */
public interface BackgroundWork {
    PlayerCall<CommitResult> advance(RealmWorkReader.DueTask candidate, Duration wait);
    PlayerCall<Optional<BattleDispatch>> prepareBattle(RealmWorkReader.PendingBattle candidate, Duration wait);
    PlayerCall<CommitResult> settle(RealmWorkReader.PendingBattle candidate, BattleResult result, Duration wait);
}

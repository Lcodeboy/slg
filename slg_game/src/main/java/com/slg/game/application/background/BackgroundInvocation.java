package com.slg.game.application.background;

import com.slg.core.battle.BattleResult;
import com.slg.game.application.march.MarchPlan;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.util.Objects;

/** 持久候选及可复核提示的不可变命令；系统身份来自权威归属，不伪造玩家会话。 */
public record BackgroundInvocation(long accountId, VersionedState candidate, MarchPlan plan, BattleResult result) {
    public BackgroundInvocation {
        Objects.requireNonNull(candidate, "candidate");
        String player = switch (candidate.state()) {
            case TaskState task -> task.playerId();
            case BattleState battle -> battle.playerId();
            default -> throw new IllegalArgumentException("Only task/battle work can be scheduled");
        };
        if (accountId <= 0 || PlayerIdentity.accountOf(player) != accountId)
            throw new IllegalArgumentException("Background work belongs to another account");
        if (candidate.state() instanceof TaskState task) {
            if (result != null || (plan != null && !task.marchId().equals(plan.marchId())))
                throw new IllegalArgumentException("Invalid task work payload");
        } else if (plan != null) throw new IllegalArgumentException("Battle work must not carry a gather plan");
    }
}

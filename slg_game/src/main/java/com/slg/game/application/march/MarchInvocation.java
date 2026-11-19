package com.slg.game.application.march;

import com.slg.game.application.player.PlayerAccess;
import com.slg.game.domain.march.StartMarchOrder;
import java.util.Objects;

/** 有界队列输入；只携带不可变原请求与可复核规划，不捕获玩家缓存或存储连接。 */
public record MarchInvocation(PlayerAccess access, StartMarchOrder order, String marchId, MarchPlan plan) {
    public MarchInvocation {
        Objects.requireNonNull(access, "access");
        if (order == null && marchId == null) throw new IllegalArgumentException("Missing march input");
        if (plan != null && !plan.marchId().equals(marchId))
            throw new IllegalArgumentException("Plan belongs to another march");
    }
}

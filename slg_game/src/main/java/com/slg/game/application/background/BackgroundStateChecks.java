package com.slg.game.application.background;

import com.slg.game.application.execution.OperationStateChangedException;
import com.slg.game.application.player.PlayerOperationContext;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.Objects;

/** 权威世界根与已持资格的玩家缓存交界；不一致先失效，不能把残缺预留当成正常业务拒绝。 */
final class BackgroundStateChecks {
    private BackgroundStateChecks() { }
    static void playerMarch(PlayerOperationContext context, PlayerSnapshot before, long high,
                            String marchId, VersionedState current) {
        if (high < before.highSequence()) throw new IllegalStateException("Background snapshot precedes accepted player state");
        var cached = before.activeMarches().get(marchId);
        boolean ownedActive = current != null && current.state() instanceof MarchState march
                && march.playerId().equals(before.playerId()) && march.status() != MarchState.Status.COMPLETED;
        if (!Objects.equals(cached, ownedActive ? current : null)) {
            context.directory().invalidate(before.accountId());
            throw new OperationStateChangedException();
        }
    }
}

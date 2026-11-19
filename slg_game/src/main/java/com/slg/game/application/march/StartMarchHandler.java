package com.slg.game.application.march;

import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.player.*;
import com.slg.game.domain.march.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/** 派兵模板示例：资格内读取的玩家快照 → 纯规则 → 完整决定；此阶段不抢占目的矿点。 */
final class StartMarchHandler extends MarchWriteHandler {
    private final Clock clock;
    StartMarchHandler(PlayerOperationContext context, MarchPayloadCodec codec, Clock clock) {
        super(context, codec); this.clock = clock;
    }
    @Override protected String digest(MarchInvocation input) { return codec.startDigest(input.order()); }
    @Override protected CompletionStage<CommitDecision> decide(OperationCommand command, MarchInvocation input, PlayerSnapshot before) {
        if (input.order() == null || input.plan() != null || !MarchIdentity.start(context.realm().realmId(),
                input.access().accountId(), command.scope().request().requestId()).equals(input.marchId()))
            throw new IllegalArgumentException("Start input differs from predeclared identity");
        if (!command.scope().objects().containsAll(Set.of(new ObjectKey.March(input.marchId()),
                new ObjectKey.Task(MarchIdentity.arrival(input.marchId())))))
            throw new IllegalArgumentException("Start decision would escape its declared world scope");
        var transition = MarchRules.start(before, input.marchId(), input.order(), clock.millis());
        return CompletableFuture.completedFuture(decision(command, input, PlayerOutcome.START, transition, Map.of()));
    }
}

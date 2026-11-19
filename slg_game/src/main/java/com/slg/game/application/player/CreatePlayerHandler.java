package com.slg.game.application.player;

import com.slg.game.application.execution.OperationCommand;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** 建角用例：只组合纯规则与原响应，不重复实现提交、会话或缓存生命周期。 */
final class CreatePlayerHandler extends AbstractPlayerWriteHandler<PlayerInvocation> {
    CreatePlayerHandler(PlayerOperationContext context) { super(context); }
    @Override protected PlayerInvocation decode(OperationCommand command) { return context.codec().decodeInvocation(command.payload()); }
    @Override protected PlayerAccess access(PlayerInvocation input) { return input.access(); }
    @Override protected String digest(PlayerInvocation input) { return context.codec().createDigest(input.value()); }

    @Override protected CompletionStage<CommitDecision> decide(OperationCommand command, PlayerInvocation input,
                                                                PlayerSnapshot before) {
        List<ObjectChange> changes;
        PlayerOutcome outcome;
        try {
            changes = PlayerCreation.create(before, input.value());
            outcome = PlayerOutcome.created(PlayerDetails.created(changes.stream().map(ObjectChange::afterState).toList()));
        } catch (GameException rejected) {
            changes = List.of();
            outcome = PlayerOutcome.rejected(rejected.code(), rejected.getMessage());
        }
        // 建角只依赖资料存在性；六个初始根各自 expected=0，不把无关世界根误声明为读取依赖。
        var profile = new ObjectKey.Player(before.playerId());
        long version = before.profile().map(VersionedState::version).orElse(0L);
        return CompletableFuture.completedFuture(new CommitDecision(command.scope().request(), digest(input),
                outcome.player() == null ? "COMMAND_REJECTED" : "PLAYER_CREATED",
                context.codec().creationFact(input.access().accountId(), before.playerId(), outcome.errorCode()),
                context.codec().encodeOutcome(outcome), changes, Map.of(profile, version)));
    }
}

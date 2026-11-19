package com.slg.game.application.background;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.execution.*;
import com.slg.game.application.player.PlayerOperationContext;
import com.slg.game.application.port.RealmWorkReader;
import com.slg.game.domain.march.NpcBattleRules;
import com.slg.game.domain.persistence.*;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** 短资格内确认仍待计算及完整预留，返回固定输入后释放资格；远程 Worker 调用不在此 handler 内。 */
final class PrepareBattleHandler implements QualifiedQueryHandler {
    private final PlayerOperationContext context;
    private final BackgroundCodec codec;
    private final BackgroundSnapshotReader reader;
    PrepareBattleHandler(PlayerOperationContext context, BackgroundCodec codec, BackgroundSnapshotReader reader) {
        this.context = context; this.codec = codec; this.reader = reader;
    }
    @Override public CompletionStage<OperationValue> execute(OperationCommand command) {
        var input = codec.decodeInvocation(command.payload());
        if (!(input.candidate().state() instanceof BattleState battle) || battle.status() != BattleState.Status.PENDING
                || input.plan() != null || input.result() != null)
            throw new IllegalArgumentException("Expected pending battle discovery");
        BackgroundScopes.validate(command, input, codec, CommitRequest.Origin.BATTLE_RESULT, BackgroundScopes.battle(input));
        var player = context.directory().load(input.accountId());
        var current = reader.battle(battle);
        return player.thenCombine(current, (before, snapshot) -> {
            Optional<BattleDispatch> dispatch = Optional.empty();
            if (input.candidate().equals(snapshot.battle())) {
                BackgroundStateChecks.playerMarch(context, before, snapshot.highSequence(), battle.marchId(), snapshot.march());
                var fixed = codec.input(battle);
                NpcBattleRules.validateForDispatch(before, snapshot.battle(), snapshot.march(), fixed);
                if (snapshot.returnTask().isPresent()) throw new IllegalStateException("Pending battle already has a return task");
                dispatch = Optional.of(new BattleDispatch(new RealmWorkReader.PendingBattle(input.candidate().version(), battle), fixed));
            }
            return new OperationValue(codec.encodeDispatch(dispatch));
        });
    }
    @Override public int maxResultBytes() { return codec.maxWorkBytes(); }
}

package com.slg.game.application.background;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.execution.*;
import com.slg.game.application.player.PlayerOperationContext;
import com.slg.game.domain.march.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletionStage;

/** Worker 只能提供计算结果；资格内重读固定输入与预留后，伤亡/战报/返程任务在一笔事务结算。 */
final class SettleBattleHandler extends BackgroundWriteHandler {
    private final BackgroundSnapshotReader reader;
    private final Clock clock;
    SettleBattleHandler(PlayerOperationContext context, BackgroundCodec codec, BackgroundSnapshotReader reader, Clock clock) {
        super(context, codec); this.reader = reader; this.clock = clock;
    }
    @Override protected void validateInput(OperationCommand command, BackgroundInvocation input) {
        if (!(input.candidate().state() instanceof BattleState battle) || battle.status() != BattleState.Status.PENDING
                || input.result() == null || input.plan() != null)
            throw new IllegalArgumentException("Expected original pending battle and Worker result");
        BackgroundScopes.validate(command, input, codec, CommitRequest.Origin.BATTLE_RESULT, BackgroundScopes.battle(input));
    }
    @Override protected CompletionStage<CommitDecision> decide(OperationCommand command, BackgroundInvocation input, PlayerSnapshot before) {
        var candidate = (BattleState) input.candidate().state();
        return reader.battle(candidate).thenApply(snapshot -> {
            if (snapshot.battle() != null && ((BattleState) snapshot.battle().state()).status() == BattleState.Status.SETTLED)
                throw new MarchDataException("Settled battle is missing its matching system receipt");
            if (!input.candidate().equals(snapshot.battle())) throw new OperationStateChangedException();
            BackgroundStateChecks.playerMarch(context, before, snapshot.highSequence(), candidate.marchId(), snapshot.march());
            var next = NpcBattleRules.settle(before, snapshot.battle(), snapshot.march(), snapshot.returnTask(),
                    codec.input(candidate), input.result(), clock.millis());
            var settled = new BattleState(candidate.battleId(), candidate.marchId(), candidate.playerId(), BattleState.Status.SETTLED,
                    candidate.heroIds(), candidate.inputHash(), candidate.rulesVersion(), candidate.marchVersion(), candidate.input(),
                    codec.result(next.result()));
            var changes = new ArrayList<>(next.changes()); changes.add(new ObjectChange(settled.key(), snapshot.battle().version(), settled));
            return decision(command, input, "NPC_BATTLE_SETTLED", "APPLIED", changes, next.readVersions(),
                    List.of("NPC_BATTLE_SETTLED", "MARCH_RETURN_STARTED"), null);
        });
    }
}

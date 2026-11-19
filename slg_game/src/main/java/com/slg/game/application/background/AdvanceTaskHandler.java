package com.slg.game.application.background;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.execution.*;
import com.slg.game.application.march.MarchPayloadCodec;
import com.slg.game.application.player.PlayerOperationContext;
import com.slg.game.domain.march.*;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletionStage;

/** 一个到期候选只推进一个持久阶段；发现、规划、排队都不代表任务完成，旧候选不写空事件。 */
final class AdvanceTaskHandler extends BackgroundWriteHandler {
    private final BackgroundSnapshotReader reader;
    private final MarchPayloadCodec marchCodec;
    private final Clock clock;
    AdvanceTaskHandler(PlayerOperationContext context, BackgroundCodec codec, BackgroundSnapshotReader reader,
                       MarchPayloadCodec marchCodec, Clock clock) {
        super(context, codec); this.reader = reader; this.marchCodec = marchCodec; this.clock = clock;
    }
    @Override protected void validateInput(OperationCommand command, BackgroundInvocation input) {
        if (!(input.candidate().state() instanceof TaskState task) || task.status() != TaskState.Status.PENDING
                || input.plan() == null || input.result() != null)
            throw new IllegalArgumentException("Expected a pending task and complete plan");
        BackgroundScopes.validate(command, input, codec, CommitRequest.Origin.DUE_TASK, BackgroundScopes.task(input));
    }
    @Override protected CompletionStage<CommitDecision> decide(OperationCommand command, BackgroundInvocation input, PlayerSnapshot before) {
        var candidate = (TaskState) input.candidate().state();
        return reader.task(input).thenApply(snapshot -> {
            if (!input.plan().equals(BackgroundScopes.plan(candidate.marchId(), snapshot.march())))
                throw new OperationStateChangedException();
            BackgroundStateChecks.playerMarch(context, before, snapshot.highSequence(), candidate.marchId(), snapshot.march());
            var next = MarchRules.advance(before, input.candidate(), snapshot.march(), snapshot.tasks(), snapshot.site(), snapshot.battle(), clock.millis());
            if (next.status() == DueTransition.Status.SKIPPED) throw new OperationStateChangedException();
            var changes = new ArrayList<>(next.changes());
            if (next.pendingBattle() != null) {
                var battle = marchCodec.prepareBattle(next.pendingBattle());
                if (snapshot.versions().getOrDefault(battle.key(), 0L) != 0)
                    throw new MarchDataException("Arrival would replace an existing battle");
                changes.add(new ObjectChange(battle.key(), 0, battle));
            }
            String reason = next.status() == DueTransition.Status.BLOCKED ? changes.stream()
                    .map(ObjectChange::afterState).filter(TaskState.class::isInstance).map(TaskState.class::cast)
                    .map(TaskState::reason).filter(Objects::nonNull).findFirst().orElse("Invalid task dependencies") : null;
            return decision(command, input, next.status() == DueTransition.Status.BLOCKED ? "MARCH_TASK_BLOCKED" : "MARCH_TASK_ADVANCED",
                    next.status().name(), changes, merge(snapshot.versions(), next.readVersions()), next.phases(), reason);
        });
    }
}

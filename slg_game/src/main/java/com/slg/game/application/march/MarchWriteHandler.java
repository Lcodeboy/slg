package com.slg.game.application.march;

import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.player.*;
import com.slg.game.domain.march.MarchTransition;
import com.slg.game.domain.persistence.*;
import java.util.*;

/** 行军写用例只共享编码/决定组装；准入、幂等、会话、发布和修复均继承统一玩家生命周期。 */
abstract class MarchWriteHandler extends AbstractPlayerWriteHandler<MarchInvocation> {
    protected final MarchPayloadCodec codec;

    MarchWriteHandler(PlayerOperationContext context, MarchPayloadCodec codec) {
        super(context); this.codec = codec;
    }
    @Override protected MarchInvocation decode(OperationCommand command) { return codec.decodeInvocation(command.payload()); }
    @Override protected PlayerAccess access(MarchInvocation input) { return input.access(); }

    protected CommitDecision decision(OperationCommand command, MarchInvocation input, String operation,
                                        MarchTransition transition, Map<ObjectKey, Long> observed) {
        var changes = new ArrayList<>(transition.changes());
        if (transition.pendingBattle() != null) {
            var battle = codec.prepareBattle(transition.pendingBattle());
            if (observed.getOrDefault(battle.key(), 0L) != 0)
                throw new IllegalStateException("Unexpected existing battle for first NPC arrival");
            changes.add(new ObjectChange(battle.key(), 0, battle));
        }
        var versions = new LinkedHashMap<>(observed);
        transition.readVersions().forEach((key, version) -> {
            Long old = versions.putIfAbsent(key, version);
            if (old != null && !old.equals(version)) throw new IllegalStateException("Inconsistent decision read versions");
        });
        PlayerOutcome outcome = transition.errorCode() != null
                ? PlayerOutcome.rejected(operation, transition.errorCode(), transition.errorMessage())
                : PlayerOutcome.START.equals(operation) ? PlayerOutcome.started(transition.march())
                : PlayerOutcome.recalled(transition.march());
        String factType = transition.errorCode() == null
                ? PlayerOutcome.START.equals(operation) ? "MARCH_STARTED" : "MARCH_RECALLED"
                : "COMMAND_REJECTED";
        // 召回补做的已到期阶段即使最终不能再召回，也和拒绝回执同事务保存；事实保留阶段列表。
        return new CommitDecision(command.scope().request(), digest(input), factType,
                codec.fact(operation, input.access().accountId(), input.marchId(), transition.phases(), transition.errorCode()),
                context.codec().encodeOutcome(outcome), changes, versions);
    }
}

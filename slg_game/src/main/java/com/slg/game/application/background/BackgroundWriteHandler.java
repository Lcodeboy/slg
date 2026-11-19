package com.slg.game.application.background;

import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.player.*;
import com.slg.game.domain.persistence.*;
import java.util.*;

/** 系统写入口只替换身份与回执策略，准备、提交后发布、UNKNOWN 重载与 pin 仍用同一模板。 */
abstract class BackgroundWriteHandler extends AbstractAccountWriteHandler<BackgroundInvocation> {
    protected final BackgroundCodec codec;
    BackgroundWriteHandler(PlayerOperationContext context, BackgroundCodec codec) { super(context); this.codec = codec; }
    @Override protected BackgroundInvocation decode(OperationCommand command) { return codec.decodeInvocation(command.payload()); }
    @Override protected long accountId(BackgroundInvocation input) { return input.accountId(); }
    @Override protected String digest(BackgroundInvocation input) { return codec.digest(input); }

    @Override protected void validateReplay(BackgroundInvocation input, OperationCommand command, CommitReceipt receipt) {
        var outcome = codec.decodeOutcome(receipt.resultJson());
        var expected = outcome(input, "APPLIED");
        if (!outcome.operation().equals(expected.operation()) || outcome.accountId() != expected.accountId()
                || !outcome.objectId().equals(expected.objectId()) || outcome.generation() != expected.generation()
                || (input.candidate().state() instanceof BattleState && !outcome.status().equals("APPLIED")))
            throw new IllegalArgumentException("System receipt differs from the original persisted candidate");
    }

    protected BackgroundOutcome outcome(BackgroundInvocation input, String status) {
        return switch (input.candidate().state()) {
            case TaskState task -> new BackgroundOutcome("DUE_TASK", input.accountId(), task.taskId(), task.generation(), status);
            case BattleState battle -> new BackgroundOutcome("BATTLE_RESULT", input.accountId(), battle.battleId(), battle.marchVersion(), status);
            default -> throw new IllegalArgumentException("Unknown persisted work kind");
        };
    }

    protected CommitDecision decision(OperationCommand command, BackgroundInvocation input, String type, String status,
                                        List<ObjectChange> changes, Map<ObjectKey, Long> versions, List<String> phases, String reason) {
        return new CommitDecision(command.scope().request(), digest(input), type, codec.fact(input, phases, reason),
                codec.encodeOutcome(outcome(input, status)), changes, versions);
    }

    static Map<ObjectKey, Long> merge(Map<ObjectKey, Long> observed, Map<ObjectKey, Long> required) {
        var result = new LinkedHashMap<>(observed);
        required.forEach((key, version) -> {
            Long existing = result.putIfAbsent(key, version);
            if (existing != null && !existing.equals(version)) throw new IllegalStateException("Conflicting snapshot versions");
        });
        return result;
    }
}

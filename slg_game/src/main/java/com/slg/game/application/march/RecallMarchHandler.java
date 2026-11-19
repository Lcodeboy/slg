package com.slg.game.application.march;

import com.slg.game.application.execution.*;
import com.slg.game.application.player.*;
import com.slg.game.domain.march.MarchRules;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.time.Clock;
import java.util.concurrent.CompletionStage;

/** 规划只提供范围提示；正式资格内重读并核验后，纯规则一次裁决补时和召回。 */
final class RecallMarchHandler extends MarchWriteHandler {
    private final MarchSnapshotReader reader;
    private final Clock clock;
    RecallMarchHandler(PlayerOperationContext context, MarchPayloadCodec codec, MarchSnapshotReader reader, Clock clock) {
        super(context, codec); this.reader = reader; this.clock = clock;
    }
    @Override protected String digest(MarchInvocation input) { return codec.recallDigest(input.marchId()); }
    @Override protected CompletionStage<CommitDecision> decide(OperationCommand command, MarchInvocation input, PlayerSnapshot before) {
        if (input.order() != null || input.plan() == null) throw new IllegalArgumentException("Missing qualified recall plan");
        if (!command.scope().objects().containsAll(MarchApplication.recallObjects(input.plan())))
            throw new IllegalArgumentException("Recall read would escape its declared world scope");
        return reader.recall(input.access().accountId(), input.marchId(), input.plan()).thenApply(snapshot -> {
            context.requireAccess(input.access());
            if (snapshot.highSequence() < before.highSequence())
                throw new IllegalStateException("World read precedes the accepted player snapshot");
            if (!input.plan().equals(MarchSnapshotReader.plan(input.marchId(), snapshot.march())))
                throw new OperationStateChangedException();
            if (snapshot.march() == null && before.activeMarches().containsKey(input.marchId())) {
                // 驻留集合仍有预留，权威根却消失/换属主，是不一致而非正常“找不到”；不写拒绝回执。
                context.directory().invalidate(input.access().accountId());
                throw new OperationStateChangedException();
            }
            if (snapshot.march() != null) {
                var march = (MarchState) snapshot.march().state();
                var cached = before.activeMarches().get(input.marchId());
                boolean active = march.status() != MarchState.Status.COMPLETED;
                if (before.profileState().isEmpty() || !before.playerId().equals(march.playerId()) ||
                        (active ? !snapshot.march().equals(cached) : cached != null)) {
                    context.directory().invalidate(input.access().accountId());
                    throw new OperationStateChangedException();
                }
            }
            var transition = MarchRules.recall(before, snapshot.march(), snapshot.tasks(), snapshot.site(), clock.millis());
            return decision(command, input, PlayerOutcome.RECALL, transition, snapshot.versions());
        });
    }
}

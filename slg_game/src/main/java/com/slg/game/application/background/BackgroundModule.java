package com.slg.game.application.background;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.execution.*;
import com.slg.game.application.march.MarchPayloadCodec;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.persistence.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletionStage;

/** 持久任务与战斗用例注册；后台只增加来源/用例，不创建另一套玩家资格或写入通道。 */
public final class BackgroundModule implements PlayerUseCaseModule {
    public static final String ADVANCE = "system.march.advance.v1";
    public static final String PLAN = "system.march.plan.v1";
    public static final String PREPARE_BATTLE = "system.battle.prepare.v1";
    public static final String SETTLE = "system.battle.settle.v1";
    public static final String GET_BATTLE = "battle.get.v1";
    private final RealmIdentity realm;
    private final BackgroundSnapshotReader reader;
    private final BackgroundCodec codec;
    private final MarchPayloadCodec marchCodec;
    private final Clock clock;

    public BackgroundModule(RealmIdentity realm, RealmStateReader states, BoundedReadExecutor reads, ReadBudget budget,
                            BackgroundCodec codec, MarchPayloadCodec marchCodec, Clock clock) {
        this.realm = Objects.requireNonNull(realm); this.codec = Objects.requireNonNull(codec);
        this.marchCodec = Objects.requireNonNull(marchCodec); this.clock = Objects.requireNonNull(clock);
        if (reads.limits().maxOutstanding() < 3)
            throw new IllegalArgumentException("Background preparation requires three outstanding cold reads");
        reader = new BackgroundSnapshotReader(realm, Objects.requireNonNull(states), reads, Objects.requireNonNull(budget));
    }
    @Override public Map<String, OperationHandler> writeHandlers(PlayerOperationContext context) { return Map.of(); }
    @Override public Map<String, QualifiedQueryHandler> queryHandlers(PlayerOperationContext context) {
        requireRealm(context);
        return Map.of(GET_BATTLE, new QualifiedQueryHandler() {
            @Override public CompletionStage<OperationValue> execute(OperationCommand command) {
                var input = context.codec().decodeInvocation(command.payload());
                context.validateInput(command, input.access());
                BackgroundApplication.requireBattleId(input.value());
                if (!command.scope().objects().contains(new ObjectKey.Battle(input.value())))
                    throw new IllegalArgumentException("Battle query lacks its object qualification");
                return reader.report(input.access().accountId(), input.value()).thenApply(battle -> {
                    context.requireAccess(input.access());
                    return new OperationValue(codec.encodeReport(codec.report(battle)));
                });
            }
            @Override public int maxResultBytes() { return codec.maxWorkBytes(); }
        });
    }
    @Override public Map<String, OperationHandler> systemWriteHandlers(PlayerOperationContext context) {
        requireRealm(context);
        return Map.of(ADVANCE, new AdvanceTaskHandler(context, codec, reader, marchCodec, clock),
                SETTLE, new SettleBattleHandler(context, codec, reader, clock));
    }
    @Override public Map<String, QualifiedQueryHandler> systemQueryHandlers(PlayerOperationContext context) {
        requireRealm(context);
        return Map.of(PREPARE_BATTLE, new PrepareBattleHandler(context, codec, reader), PLAN, new QualifiedQueryHandler() {
            @Override public CompletionStage<OperationValue> execute(OperationCommand command) {
                var input = codec.decodeInvocation(command.payload());
                if (!(input.candidate().state() instanceof TaskState task) || input.plan() != null || input.result() != null)
                    throw new IllegalArgumentException("Expected unplanned task discovery");
                BackgroundScopes.validate(command, input, codec, CommitRequest.Origin.DUE_TASK, Set.of(new ObjectKey.March(task.marchId())));
                return reader.plan(task).thenApply(plan -> new OperationValue(codec.encodePlan(plan)));
            }
            @Override public int maxResultBytes() { return 8192; }
        });
    }
    private void requireRealm(PlayerOperationContext context) {
        if (!realm.equals(context.realm())) throw new IllegalArgumentException("Background module belongs to another dataset");
    }
}

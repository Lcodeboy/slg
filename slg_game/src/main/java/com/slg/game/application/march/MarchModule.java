package com.slg.game.application.march;

import com.slg.game.application.execution.*;
import com.slg.game.application.player.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.persistence.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * 行军用例的显式注册模块。只向每区唯一执行器注册长期 handler，不新建邮箱、线程池或
 * 数据源。读取端口和时钟由组合根注入；领域规则不需要知道这些运行资源。
 */
public final class MarchModule implements PlayerUseCaseModule {
    public static final String START = "march.start.v2";
    public static final String RECALL = "march.recall.v1";
    public static final String GET = "march.get.v1";
    public static final String PLAN_RECALL = "march.plan-recall.v1";
    private final RealmIdentity realm;
    private final MarchSnapshotReader reader;
    private final MarchPayloadCodec codec;
    private final Clock clock;

    public MarchModule(RealmIdentity realm, RealmStateReader states, BoundedReadExecutor reads,
                       ReadBudget budget, MarchPayloadCodec codec, Clock clock) {
        this.realm = Objects.requireNonNull(realm); this.codec = Objects.requireNonNull(codec);
        this.clock = Objects.requireNonNull(clock);
        // 公共模板的玩家/回执两次读取可能仍在通知，行军正式读取需第三个槽；不是额外的无界池。
        if (reads.limits().maxOutstanding() < 3)
            throw new IllegalArgumentException("March use cases require at least three outstanding cold reads");
        reader = new MarchSnapshotReader(realm, states, reads, budget);
    }

    @Override public Map<String, OperationHandler> writeHandlers(PlayerOperationContext context) {
        requireRealm(context);
        return Map.of(START, new StartMarchHandler(context, codec, clock),
                RECALL, new RecallMarchHandler(context, codec, reader, clock));
    }

    @Override public Map<String, QualifiedQueryHandler> queryHandlers(PlayerOperationContext context) {
        requireRealm(context);
        return Map.of(GET, query(context, codec.maxResultBytes(), input -> reader.get(input.access().accountId(), input.marchId())
                        .thenApply(codec::encodeMarch)),
                PLAN_RECALL, query(context, 8192, input -> reader.plan(input.access().accountId(), input.marchId())
                        .thenApply(codec::encodePlan)));
    }

    private QualifiedQueryHandler query(PlayerOperationContext context, int maxOutput,
                                         Function<MarchInvocation, CompletionStage<byte[]>> action) {
        return new QualifiedQueryHandler() {
            @Override public CompletionStage<OperationValue> execute(OperationCommand command) {
                var input = codec.decodeInvocation(command.payload());
                context.validateInput(command, input.access());
                MarchApplication.requireMarchId(input.marchId());
                if (input.order() != null || input.plan() != null ||
                        !command.scope().objects().contains(new ObjectKey.March(input.marchId())))
                    throw new IllegalArgumentException("Query differs from declared march scope");
                return action.apply(input).thenApply(bytes -> {
                    context.requireAccess(input.access());
                    return new OperationValue(bytes);
                });
            }
            @Override public int maxResultBytes() { return maxOutput; }
        };
    }

    private void requireRealm(PlayerOperationContext context) {
        if (!realm.equals(context.realm())) throw new IllegalArgumentException("March module belongs to another dataset");
    }
}

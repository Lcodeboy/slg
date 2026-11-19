package com.slg.game.application.player;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.execution.OperationHandler;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.*;
import java.util.concurrent.*;

/**
 * 玩家与后台写入共用的固定生命周期。身份/来源及回执格式由入口模板校验，账号资格、幂等读取、
 * 摘要不变性与提交后的缓存修复只有这一份实现；后台任务不需要伪造在线会话。
 * 子类只能准备完整决定，不能直接提交或提前发布。异步等待仍由外层执行器持有资格和 pin。
 */
public abstract class AbstractAccountWriteHandler<I> implements OperationHandler {
    protected final PlayerOperationContext context;
    protected AbstractAccountWriteHandler(PlayerOperationContext context) { this.context = Objects.requireNonNull(context); }
    protected abstract I decode(OperationCommand command);
    protected abstract long accountId(I input);
    protected abstract void validateInput(OperationCommand command, I input);
    protected abstract String digest(I input);
    protected abstract CompletionStage<CommitDecision> decide(OperationCommand command, I input, PlayerSnapshot before);
    protected abstract void validateReplay(I input, OperationCommand command, CommitReceipt receipt);

    @Override public final CompletionStage<CommitDecision> prepareAsync(OperationCommand command) {
        I input = decode(command);
        long account = accountId(input);
        if (account <= 0 || !command.scope().accounts().contains(account))
            throw new IllegalArgumentException("Input account differs from declared execution scope");
        validateInput(command, input);
        String requestDigest = digest(input);
        // 两项读取均在准备线程发起；不占着冷读通知额度串行申请下一份基础读取。
        var player = context.directory().load(account);
        var receipt = context.observe(command.scope().request());
        return player.thenCombine(receipt, Preparation::new).thenCompose(prepared -> {
            validateInput(command, input);
            if (prepared.observed() instanceof RealmReceiptReader.Observation.Found found) {
                if (!command.scope().request().equals(found.receipt().request()))
                    throw new IllegalArgumentException("Receipt request differs from qualified request");
                validateReplay(input, command, found.receipt());
                // 原响应先验证归属与结构；存储仍用本次摘要判断重放/冲突，不重跑旧业务。
                return CompletableFuture.completedFuture(new CommitDecision(command.scope().request(), requestDigest,
                        "ACCOUNT_REQUEST_REPLAY", "{\"schema\":1,\"operation\":\"REPLAY\"}",
                        found.receipt().resultJson(), List.of(), Map.of()));
            }
            return decide(command, input, prepared.player()).thenApply(decision -> {
                validateInput(command, input);
                if (!requestDigest.equals(decision.digest()))
                    throw new IllegalArgumentException("Use case decision changed the request digest");
                return decision;
            });
        });
    }

    @Override public final CompletionStage<Void> publishAsync(OperationCommand command, CommitDecision decision,
                                                              CommitResult.Committed committed) {
        return context.publish(accountId(decode(command)), decision, committed);
    }
    @Override public final CompletionStage<Void> reloadAsync(OperationCommand command, CommitDecision decision,
                                                             CommitResult.Committed committed) {
        return context.reload(accountId(decode(command)), committed);
    }
    @Override public final CompletionStage<Void> discardAsync(OperationCommand command, CommitResult.NotCommitted rejected) {
        return context.discard(accountId(decode(command)), rejected);
    }
    private record Preparation(PlayerSnapshot player, RealmReceiptReader.Observation observed) { }
}

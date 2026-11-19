package com.slg.game.application.player;

import com.slg.game.application.commit.CommitResult;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * 仅串联一次有界规划和一次有界写入，不维护重试队列。规划期间取消阻止第二阶段；正式写入
 * 一旦开始，取消只能结束网络等待，不能取消内部提交。每阶段的 pin/容量仍由统一执行器释放。
 */
public final class PlannedWriteCall<P> implements PlayerCall<CommitResult> {
    private final Object gate = new Object();
    private final CompletableFuture<CommitResult> result = new CompletableFuture<>();
    private PlayerCall<?> current;
    private boolean writing;
    private boolean cancelled;

    public PlannedWriteCall(PlayerCall<P> planning, Function<P, PlayerCall<CommitResult>> submit) {
        current = planning;
        planning.result().whenComplete((plan, failure) -> {
            PlayerCall<CommitResult> next = null;
            boolean stopped;
            synchronized (gate) {
                stopped = cancelled;
                if (!stopped && failure == null) {
                    try {
                        next = submit.apply(plan);
                        current = next; writing = true;
                    } catch (Throwable rejected) { failure = rejected; }
                }
            }
            // 对外通知不得在gate内执行，慢网络回调不能锁住取消/阶段切换。
            if (stopped) result.complete(new CommitResult.NotCommitted(CommitResult.Reason.CANCELLED));
            else if (failure != null) result.completeExceptionally(failure);
            else next.result().whenComplete((value, error) -> {
                if (error == null) result.complete(value); else result.completeExceptionally(error);
            });
        });
    }

    @Override public CompletionStage<CommitResult> result() { return result.minimalCompletionStage(); }

    @Override public boolean cancelBeforeStart() {
        synchronized (gate) {
            if (result.isDone() || cancelled) return false;
            if (writing) return current.cancelBeforeStart();
            cancelled = true;
            current.cancelBeforeStart();
            return true; // 规划读即使已经开始，仍保证永不提交第二阶段。
        }
    }
}

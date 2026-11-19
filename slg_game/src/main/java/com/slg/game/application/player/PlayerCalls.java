package com.slg.game.application.player;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/** 将缓存 pin 绑定到内部终态；公开 stage 的取消不能提前释放后台仍在使用的快照。 */
final class PlayerCalls {
    private PlayerCalls() { }

    static <I, T> PlayerCall<T> hold(CompletionStage<I> internal, BooleanSupplier cancel,
                                    PlayerDirectory.Lease pin, Function<I, T> decode) {
        var output = new CompletableFuture<T>();
        internal.whenComplete((value, failure) -> {
            T decoded = null;
            try { if (failure == null) decoded = decode.apply(value); }
            catch (Throwable invalid) { failure = invalid; }
            finally { pin.close(); }
            // 清理先于公开通知；慢回调仍由执行器的通知预算承载，不再创建无界回调池。
            if (failure == null) output.complete(decoded); else output.completeExceptionally(failure);
        });
        return new PlayerCall<>() {
            @Override public CompletionStage<T> result() { return output.minimalCompletionStage(); }
            @Override public boolean cancelBeforeStart() { return cancel.getAsBoolean(); }
        };
    }
}

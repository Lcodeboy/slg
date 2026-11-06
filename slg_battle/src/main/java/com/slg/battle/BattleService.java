package com.slg.battle;

import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.core.battle.ExpeditionBattleEngine;
import com.slg.rpc.BattleWire;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/** 网络线程只做受理；CPU 池执行有回合与兵组上限的确定性计算。 */
final class BattleService extends BattleWorkerRpcGrpc.BattleWorkerRpcImplBase {
    private final ThreadPoolExecutor workers;
    private final Function<BattleInput,BattleResult> compute;

    BattleService(ThreadPoolExecutor workers, Function<BattleInput,BattleResult> compute) {
        this.workers = workers;
        this.compute = compute;
    }

    @Override public void compute(BattleComputeRequest request, StreamObserver<BattleComputeReply> reply) {
        if (RpcMetadata.TOKEN.get() != null) {
            reply.onError(Status.PERMISSION_DENIED.withDescription("Internal service RPC only").asRuntimeException());
            return;
        }
        Context context = Context.current();
        if (context.isCancelled()) return;
        if (!ExpeditionBattleEngine.RULES_VERSION.equals(request.getRulesVersion())) {
            reply.onError(Status.FAILED_PRECONDITION.withDescription("Unsupported battle rules").asRuntimeException());
            return;
        }
        var job = new ComputeJob(request, reply, context);
        context.addListener(job, Runnable::run);
        try {
            workers.execute(job);
            // 取消可能恰好发生在注册监听和入队之间。
            if (context.isCancelled() && workers.remove(job)) context.removeListener(job);
        } catch (RejectedExecutionException busy) {
            context.removeListener(job);
            if (!context.isCancelled())
                reply.onError(Status.RESOURCE_EXHAUSTED.withDescription("Battle queue full or stopping").asRuntimeException());
        }
    }

    private final class ComputeJob implements Runnable, Context.CancellationListener {
        final BattleComputeRequest request;
        final StreamObserver<BattleComputeReply> reply;
        final Context context;
        ComputeJob(BattleComputeRequest request, StreamObserver<BattleComputeReply> reply, Context context) {
            this.request = request;
            this.reply = reply;
            this.context = context;
        }
        @Override public void cancelled(Context ignored) {
            if (workers.remove(this)) context.removeListener(this);
        }
        @Override public void run() {
            try {
                if (context.isCancelled()) return;
                BattleInput input;
                try { input = BattleWire.input(request); }
                catch (IllegalArgumentException badInput) {
                    reply.onError(Status.INVALID_ARGUMENT.withDescription("Invalid battle snapshot").asRuntimeException());
                    return;
                }
                BattleComputeReply outcome = BattleWire.reply(compute.apply(input), request.getInputHash());
                if (!context.isCancelled()) {
                    reply.onNext(outcome);
                    reply.onCompleted();
                }
            } catch (RuntimeException failure) {
                if (!context.isCancelled()) reply.onError(Status.INTERNAL.withDescription("Battle calculation failed").asRuntimeException());
            } finally { context.removeListener(this); }
        }
    }
}

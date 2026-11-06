package com.slg.rpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.rpc.v1.BattleComputeReply;
import com.slg.rpc.v1.BattleWorkerRpcGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** GAME 复用到 Worker 的连接。每次派发有独立 deadline；重派由持久战斗任务决定。 */
public final class BattleWorkerClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final BattleWorkerRpcGrpc.BattleWorkerRpcFutureStub stub;
    private final int deadlineMillis;

    public BattleWorkerClient(String target) {
        deadlineMillis = Integer.parseInt(System.getenv().getOrDefault("SLG_BATTLE_RPC_TIMEOUT_MS", "3000"));
        if (deadlineMillis < 1 || deadlineMillis > 30_000) throw new IllegalArgumentException("Invalid battle deadline");
        channel = BattleRpcTransport.channel(target);
        stub = BattleWorkerRpcGrpc.newFutureStub(channel);
    }

    public CompletableFuture<BattleResult> compute(BattleInput input, String inputHash, String rulesVersion) {
        var result = new CompletableFuture<BattleResult>();
        try {
            var request = BattleWire.request(input, inputHash, rulesVersion);
            var call = stub.withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS).compute(request);
            result.whenComplete((ignored, error) -> { if (result.isCancelled()) call.cancel(true); });
            Futures.addCallback(call, new FutureCallback<>() {
                @Override public void onSuccess(BattleComputeReply reply) {
                    try { result.complete(BattleWire.result(reply, input, inputHash, rulesVersion)); }
                    catch (RuntimeException invalid) {
                        result.completeExceptionally(Status.DATA_LOSS.withDescription("Invalid Worker reply")
                                .withCause(invalid).asRuntimeException());
                    }
                }
                @Override public void onFailure(Throwable error) { result.completeExceptionally(error); }
            }, Runnable::run);
        } catch (RuntimeException failure) { result.completeExceptionally(failure); }
        return result;
    }

    @Override public void close() {
        channel.shutdownNow();
        try { channel.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}

package com.slg.game;

import com.slg.core.auth.SessionClaims;
import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.game.application.execution.QueryRejectedException;
import com.slg.game.application.player.PlayerApplication;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.Objects;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 连接控制面与玩法分开预算，批量续租只访问有界内存目录；不为每玩家创建长期 RPC 流。
 * 唯一会改变当前连接所有者的 Open 必须取得同一账号资格，不能在网络线程更新目录后让旧业务继续写。
 */
final class SessionRpcService extends SessionRpcGrpc.SessionRpcImplBase {
    private final TokenService tokens;
    private final PlayerApplication application;
    private final SessionDirectory sessions;
    private final BooleanSupplier ready;
    private final int maximumDeadlineMillis;
    private final RateLimiter authentication = new RateLimiter(60, 60, 100_000);

    SessionRpcService(TokenService tokens, PlayerApplication application, SessionDirectory sessions,
                      BooleanSupplier ready, int maximumDeadlineMillis) {
        this.tokens = Objects.requireNonNull(tokens); this.application = Objects.requireNonNull(application);
        this.sessions = Objects.requireNonNull(sessions); this.ready = Objects.requireNonNull(ready);
        if (maximumDeadlineMillis <= 0) throw new IllegalArgumentException("Positive RPC deadline required");
        this.maximumDeadlineMillis = maximumDeadlineMillis;
    }

    @Override public void prepare(SessionPrepareRequest request, StreamObserver<SessionPreparation> output) {
        try { reply(Context.current(), output, sessions.prepare(authenticate())); }
        catch (RuntimeException failure) { fail(Context.current(), output, failure); }
    }

    @Override public void open(SessionOpenRequest request, StreamObserver<SessionBinding> output) {
        Context context = Context.current();
        if (output instanceof ServerCallStreamObserver<?> server) server.setOnCancelHandler(() -> {});
        SessionClaims claims;
        long remaining = TimeUnit.MILLISECONDS.toNanos(maximumDeadlineMillis);
        try {
            claims = authenticate();
            if (context.getDeadline() != null)
                remaining = Math.min(remaining, context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS));
            if (context.isCancelled() || remaining <= 0)
                throw Status.DEADLINE_EXCEEDED.withDescription("Session expired before activation").asRuntimeException();
        } catch (Exception failure) { fail(context, output, failure); return; }
        // Open 与玩法使用同一账号资格，成功绑定不能越过旧连接仍在提交/核对的操作。
        openPlayer(context, claims, request, output, remaining);
    }

    private void openPlayer(Context context, SessionClaims claims, SessionOpenRequest request,
                            StreamObserver<SessionBinding> output, long remaining) {
        try {
            var call = application.executeControl(Long.parseLong(claims.accountId()), request.getAttemptId(),
                    PlayerSessionAdapter.OPEN, PlayerSessionAdapter.payload(claims, request), Duration.ofNanos(remaining));
            Context.CancellationListener cancellation = ignored -> call.cancelBeforeStart();
            context.addListener(cancellation, Runnable::run);
            call.result().whenComplete((value, failure) -> {
                context.removeListener(cancellation);
                if (context.isCancelled()) return;
                if (failure != null) { fail(context, output, failure); return; }
                try { reply(context, output, PlayerSessionAdapter.binding(value)); }
                catch (RuntimeException invalid) { fail(context, output, invalid); }
            });
        } catch (RuntimeException failure) { fail(context, output, failure); }
    }

    @Override public void check(SessionCheckRequest request, StreamObserver<SessionCheckReply> output) {
        Context context = Context.current();
        try {
            requireReady();
            // 先检查条数再构造集合，避免非法批次在网络线程做多余散列和分配。
            if (request.getBindingsCount() < 1 || request.getBindingsCount() > SessionDirectory.MAX_CHECK_BATCH)
                throw Status.INVALID_ARGUMENT.withDescription("Session check batch must contain 1..256 bindings").asRuntimeException();
            // 批次中重复的 proof 不额外续租，也不让 Gateway 对一个 key 收到多份模糊结果。
            if (new HashSet<>(request.getBindingsList()).size() != request.getBindingsCount())
                throw Status.INVALID_ARGUMENT.withDescription("Duplicate session key").asRuntimeException();
            var current = sessions.check(request.getBindingsList());
            reply(context, output, SessionCheckReply.newBuilder().addAllCurrent(current)
                    .setLeaseMillis(SessionDirectory.LEASE_MILLIS).build());
        } catch (RuntimeException failure) { fail(context, output, failure); }
    }

    @Override public void close(SessionCloseRequest request, StreamObserver<SessionCloseReply> output) {
        Context context = Context.current();
        try {
            requireReady();
            if (!request.hasBinding()) throw Status.INVALID_ARGUMENT.withDescription("Session key required").asRuntimeException();
            sessions.release(request.getBinding());
            reply(context, output, SessionCloseReply.getDefaultInstance());
        } catch (RuntimeException failure) { fail(context, output, failure); }
    }

    private SessionClaims authenticate() {
        requireReady();
        SessionClaims claims;
        try { claims = tokens.verify(RpcMetadata.TOKEN.get()); }
        catch (IllegalArgumentException invalid) { throw Status.UNAUTHENTICATED.withDescription("Valid session required").asRuntimeException(); }
        if (!authentication.allow(claims.accountId()))
            throw Status.RESOURCE_EXHAUSTED.withDescription("Session authentication rate exceeded").asRuntimeException();
        return claims;
    }

    private void requireReady() {
        if (!ready.getAsBoolean()) throw Status.UNAVAILABLE.withDescription("Realm not ready").asRuntimeException();
    }

    private static <T> void reply(Context context, StreamObserver<T> output, T result) {
        if (context.isCancelled()) return;
        try { output.onNext(result); output.onCompleted(); }
        catch (RuntimeException disconnected) { /* Open 可能已生效；不能为了补回包再分配一次代次。 */ }
    }

    private static void fail(Context context, StreamObserver<?> output, Throwable failure) {
        if (context.isCancelled()) return;
        while (failure instanceof CompletionException && failure.getCause() != null) failure = failure.getCause();
        Status status = failure instanceof StatusRuntimeException rpc ? rpc.getStatus()
                : failure instanceof QueryRejectedException rejected ? switch (rejected.reason()) {
                    case QUEUE_TIMEOUT, CANCELLED -> Status.DEADLINE_EXCEEDED.withDescription("Session expired before activation");
                    case QUEUE_FULL -> Status.RESOURCE_EXHAUSTED.withDescription("Session activation queue full");
                    default -> Status.UNAVAILABLE.withDescription("Session activation unavailable");
                }
                : failure instanceof com.slg.game.domain.GameException || failure instanceof IllegalArgumentException
                    ? Status.INVALID_ARGUMENT.withDescription("Invalid session activation request")
                : failure instanceof RejectedExecutionException ? Status.RESOURCE_EXHAUSTED.withDescription("Session activation queue full")
                : Status.UNAVAILABLE.withDescription("Session activation unavailable");
        try { output.onError(status.asRuntimeException()); }
        catch (RuntimeException disconnected) { /* 只结束传输，不改变当前连接资格。 */ }
    }
}

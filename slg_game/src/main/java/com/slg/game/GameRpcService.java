package com.slg.game;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.core.auth.TokenService;
import com.slg.core.auth.SessionClaims;
import com.slg.core.http.RateLimiter;
import com.slg.core.metrics.LatencyMetric;
import com.slg.game.domain.GameException;
import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.execution.QueryRejectedException;
import com.slg.game.application.player.PlayerApplication;
import com.slg.game.application.player.PlayerCall;
import com.slg.game.application.player.PlayerCacheException;
import com.slg.game.application.march.MarchApplication;
import com.slg.game.application.background.BackgroundApplication;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.WireStatus;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.regex.Pattern;

/** GAME 的玩法入口。gRPC 只鉴权和映射协议，读写均由共享账号资格的应用用例执行。 */
final class GameRpcService extends GameRpcGrpc.GameRpcImplBase {
    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final PlayerApplication application;
    private final MarchApplication marches;
    private final BackgroundApplication background;
    private final TokenService tokens;
    private final SessionDirectory sessions;
    private final BooleanSupplier ready;
    private final int maximumDeadlineMillis;
    private final RateLimiter rate = new RateLimiter(300, 10, 100_000);
    private final LongAdder cancelledCalls = new LongAdder();
    private final LatencyMetric inbound = new LatencyMetric();
    private final LongAdder successful = new LongAdder(), rejected = new LongAdder(), unknown = new LongAdder();
    private final LongAdder cancelledBeforeReply = new LongAdder();

    /** 基础玩家用例装配；可选模块未注册时明确拒绝相应路由。 */
    GameRpcService(TokenService tokens, PlayerApplication application, BooleanSupplier ready,
                   int maximumDeadlineMillis, SessionDirectory sessions) {
        this(tokens, application, ready, maximumDeadlineMillis, sessions, null);
    }

    /** 行军在组合根显式装配，共享玩家执行资格、回执与提交协议。 */
    GameRpcService(TokenService tokens, PlayerApplication application, BooleanSupplier ready,
                   int maximumDeadlineMillis, SessionDirectory sessions, MarchApplication marches) {
        this(tokens, application, ready, maximumDeadlineMillis, sessions, marches, null);
    }

    /** 后台应用只提供私有战报查询；远程客户端不能通过此入口提交系统任务或战斗结果。 */
    GameRpcService(TokenService tokens, PlayerApplication application, BooleanSupplier ready,
                   int maximumDeadlineMillis, SessionDirectory sessions, MarchApplication marches, BackgroundApplication background) {
        this.tokens = Objects.requireNonNull(tokens); this.application = Objects.requireNonNull(application);
        this.marches = marches;
        this.background = background;
        this.ready = Objects.requireNonNull(ready); this.sessions = Objects.requireNonNull(sessions);
        if (maximumDeadlineMillis <= 0) throw new IllegalArgumentException("Positive RPC deadline required");
        this.maximumDeadlineMillis = maximumDeadlineMillis;
    }

    @Override public void play(PlayRequest input, StreamObserver<PlayReply> observer) {
        var context = Context.current();
        if (observer instanceof ServerCallStreamObserver<?> server) server.setOnCancelHandler(() -> {});
        handlePlay(input, observed(context, observer));
    }

    /**
     * RPC 耗时覆盖 GAME 收到调用到作出回复决定或取消等待，包含资格排队、提交核对与业务执行，
     * 不包含 Gateway/客户端网络耗时。成功决定不表示客户端已收到。
     * 取消和回复竞争只记录一次；取消以后业务可能继续提交，最终结果以原权威回执为准。
     */
    private StreamObserver<PlayReply> observed(Context context, StreamObserver<PlayReply> output) {
        long began = System.nanoTime();
        AtomicBoolean finished = new AtomicBoolean();
        Context.CancellationListener cancellation = ignored -> {
            if (finished.compareAndSet(false, true)) {
                cancelledBeforeReply.increment();
                inbound.record(System.nanoTime() - began, false);
            }
        };
        context.addListener(cancellation, Runnable::run);
        return new StreamObserver<>() {
            @Override public void onNext(PlayReply value) {
                if (finished.compareAndSet(false, true)) {
                    int status;
                    try { status = WireStatus.fromReply(value); }
                    catch (IllegalArgumentException malformed) {
                        // 观测不能成为新的协议闸门：错误响应仍交给原传输路径处理。
                        status = WireStatus.OUTCOME_UNKNOWN;
                    }
                    boolean success = status == WireStatus.OK;
                    if (success) successful.increment();
                    else if (status == WireStatus.OUTCOME_UNKNOWN) unknown.increment();
                    else rejected.increment();
                    inbound.record(System.nanoTime() - began, success);
                }
                context.removeListener(cancellation);
                output.onNext(value);
            }
            @Override public void onError(Throwable error) {
                if (finished.compareAndSet(false, true)) {
                    unknown.increment();
                    inbound.record(System.nanoTime() - began, false);
                }
                context.removeListener(cancellation);
                output.onError(error);
            }
            @Override public void onCompleted() { output.onCompleted(); }
        };
    }

    private void handlePlay(PlayRequest input, StreamObserver<PlayReply> observer) {
        var context = Context.current();
        SessionClaims session;
        try { session = tokens.verify(RpcMetadata.TOKEN.get()); }
        catch (IllegalArgumentException invalid) {
            reply(context, observer, error(input, 401, "UNAUTHORIZED", "Valid session required"));
            return;
        }
        SessionKey binding = RpcMetadata.SESSION.get();
        if (!sessions.isCurrent(session, binding)) {
            reply(context, observer, error(input, 401, "SESSION_REPLACED", "Authenticate a new game connection"));
            return;
        }
        String account = session.accountId();
        if (!rate.allow(account)) {
            reply(context, observer, error(input, 429, "RATE_LIMITED", "Too many commands"));
            return;
        }
        long remaining = TimeUnit.MILLISECONDS.toNanos(maximumDeadlineMillis);
        if (context.getDeadline() != null) remaining = Math.min(remaining, context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS));
        if (context.isCancelled() || remaining <= 0) {
            reply(context, observer, error(input, 504, "DEADLINE_EXCEEDED", "Command expired before execution"));
            return;
        }
        if (!ready.getAsBoolean()) {
            reply(context, observer, error(input, 503, "SERVER_BUSY", "Realm not ready"));
            return;
        }
        if (!REQUEST_ID.matcher(input.getRequestId()).matches()) {
            reply(context, observer, error(input, 400, "INVALID_REQUEST", "Valid requestId required"));
            return;
        }
        if (!GameplayRoutes.supports(input.getMsgId())) {
            reply(context, observer, error(input, 400, "UNKNOWN_MSG_ID", "Unknown gameplay message"));
            return;
        }
        handlePlayer(input, observer, context, session, binding, Duration.ofNanos(remaining));
    }

    private void handlePlayer(PlayRequest input, StreamObserver<PlayReply> observer, Context context,
                              SessionClaims claims, SessionKey binding, Duration wait) {
        if (input.getMsgId() != GameplayRoutes.CREATE_PLAYER && input.getMsgId() != GameplayRoutes.GET_PLAYER
                && input.getMsgId() != GameplayRoutes.GET_RECEIPT
                && !(marches != null && (input.getMsgId() == GameplayRoutes.START_MARCH
                || input.getMsgId() == GameplayRoutes.RECALL_MARCH || input.getMsgId() == GameplayRoutes.GET_MARCH))
                && !(background != null && input.getMsgId() == GameplayRoutes.GET_BATTLE)) {
            reply(context, observer, error(input, 503, "SERVER_BUSY", "Gameplay command was not accepted"));
            return;
        }
        try {
            var invocation = GameplayRoutes.decode(input);
            var actor = PlayerSessionAdapter.access(claims, binding);
            // 应用已复制必要输入；等待提交期间仅保留回复关联头，避免 RPC 回调额外长期持有整份请求体。
            var header = PlayRequest.newBuilder().setMsgId(input.getMsgId()).setRequestId(input.getRequestId()).build();
            switch (input.getMsgId()) {
                case GameplayRoutes.CREATE_PLAYER -> awaitPlayer(header, observer, context,
                        application.createPlayer(actor, input.getRequestId(), ((CreatePlayerReq) invocation.body()).getName(), wait),
                        result -> writeResult(header, result, claims, binding));
                case GameplayRoutes.START_MARCH -> awaitPlayer(header, observer, context,
                        marches.startMarch(actor, input.getRequestId(), PlayerRpcAdapter.startOrder((StartMarchReq) invocation.body()), wait),
                        result -> writeResult(header, result, claims, binding));
                case GameplayRoutes.RECALL_MARCH -> awaitPlayer(header, observer, context,
                        marches.recallMarch(actor, input.getRequestId(), ((RecallMarchReq) invocation.body()).getMarchId(), wait),
                        result -> writeResult(header, result, claims, binding));
                case GameplayRoutes.GET_MARCH -> awaitPlayer(header, observer, context,
                        marches.getMarch(actor, input.getRequestId(), ((GetMarchReq) invocation.body()).getMarchId(), wait),
                        result -> success(header, GetMarchRsp.newBuilder().setMarch(PlayerRpcAdapter.march(result)).build()));
                case GameplayRoutes.GET_BATTLE -> awaitPlayer(header, observer, context,
                        background.getBattle(actor, input.getRequestId(), ((GetBattleReq) invocation.body()).getBattleId(), wait),
                        result -> success(header, PlayerRpcAdapter.battle(result)));
                case GameplayRoutes.GET_PLAYER -> awaitPlayer(header, observer, context,
                        application.getPlayer(actor, input.getRequestId(), wait), result -> success(header,
                                GetPlayerRsp.newBuilder().setPlayer(PlayerRpcAdapter.player(result)).build()));
                case GameplayRoutes.GET_RECEIPT -> {
                    String target = ((GetReceiptReq) invocation.body()).getTargetRequestId();
                    awaitPlayer(header, observer, context, application.getReceipt(actor, input.getRequestId(), target, wait),
                            result -> success(header, PlayerRpcAdapter.receipt(target, application, result)));
                }
                default -> throw new IllegalStateException("Unsupported player route");
            }
        } catch (InvalidProtocolBufferException malformed) {
            reply(context, observer, error(input, 400, "INVALID_REQUEST", "Invalid gameplay message body"));
        } catch (IllegalArgumentException invalid) {
            // 同步输入编码失败发生在受理前；异步存储/回执解码失败不能反过来归罪于客户端请求。
            reply(context, observer, error(input, 400, "INVALID_REQUEST", "Invalid player command"));
        } catch (RuntimeException failure) { reply(context, observer, playerFailure(input, failure)); }
    }

    private PlayReply writeResult(PlayRequest header, CommitResult result, SessionClaims claims, SessionKey binding) {
        if (result instanceof CommitResult.Committed committed)
            return PlayerRpcAdapter.written(header, application, committed.receipt());
        // 会话失败只解释明确未提交；已提交原回执不能因客户端离开而改成认证失败。
        if (!sessions.isCurrent(claims, binding))
            return error(header, 401, "SESSION_REPLACED", "Authenticate a new game connection");
        return notCommitted(header, ((CommitResult.NotCommitted) result).reason());
    }

    private <T> void awaitPlayer(PlayRequest input, StreamObserver<PlayReply> observer, Context context,
                                 PlayerCall<T> call, Function<T, PlayReply> encode) {
        // 只请求撤销尚未执行的项。绝不 cancel 派生 future：真实提交/核对/发布仍持有账号资格。
        Context.CancellationListener cancellation = ignored -> { if (call.cancelBeforeStart()) cancelledCalls.increment(); };
        context.addListener(cancellation, Runnable::run);
        call.result().whenComplete((value, failure) -> {
            context.removeListener(cancellation);
            if (context.isCancelled()) return;
            PlayReply result;
            try { result = failure == null ? encode.apply(value) : playerFailure(input, failure); }
            catch (RuntimeException invalid) { result = error(input, 503, "RESULT_UNKNOWN", "Response unavailable; query the original requestId"); }
            if (result.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES)
                result = error(input, 503, "RESULT_UNKNOWN", "Response exceeds limit; query the original requestId");
            reply(context, observer, result);
        });
    }

    private static PlayReply success(PlayRequest request, com.google.protobuf.MessageLite response) {
        return PlayReply.newBuilder().setMsgId(request.getMsgId()).setRequestId(request.getRequestId()).setStatus(200)
                .setBody(response.toByteString()).build();
    }

    private static PlayReply notCommitted(PlayRequest request, CommitResult.Reason reason) {
        return switch (reason) {
            case QUEUE_TIMEOUT, CANCELLED -> error(request, 504, "DEADLINE_EXCEEDED", "Command expired before execution");
            case VERSION_CONFLICT -> error(request, 409, "STATE_CHANGED", "State changed; refresh before retrying");
            case REQUEST_CONFLICT -> error(request, 409, "REQUEST_CONFLICT", "RequestId belongs to a different command");
            default -> error(request, 503, "SERVER_BUSY", "Command was not accepted; retry with the same requestId");
        };
    }

    private static PlayReply playerFailure(PlayRequest request, Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) failure = failure.getCause();
        if (failure instanceof GameException rejected)
            return error(request, rejected.code().equals("SESSION_REPLACED") ? 401 : 200, rejected.code(), rejected.getMessage());
        if (failure instanceof QueryRejectedException rejected) return notCommitted(request, rejected.reason());
        if (failure instanceof PlayerCacheException || failure instanceof RejectedExecutionException)
            return error(request, 503, "SERVER_BUSY", "Command was not accepted; retry with the same requestId");
        boolean mutation = request.getMsgId() == GameplayRoutes.CREATE_PLAYER || request.getMsgId() == GameplayRoutes.START_MARCH
                || request.getMsgId() == GameplayRoutes.RECALL_MARCH;
        return error(request, 503, mutation ? "RESULT_UNKNOWN" : "SERVER_BUSY",
                "Player result unavailable; retry with the same requestId");
    }

    private static PlayReply error(PlayRequest input, int status, String code, String message) {
        return PlayReply.newBuilder().setMsgId(input.getMsgId()).setRequestId(input.getRequestId()).setStatus(status)
                .setError(BusinessError.newBuilder().setCode(code).setMessage(message)).build();
    }

    Map<String,Object> metrics() {
        return Map.of("cancelledCalls", cancelledCalls.sum(), "inbound", inbound.snapshot(),
                "successfulReplies", successful.sum(), "rejectedReplies", rejected.sum(),
                "unknownReplies", unknown.sum(), "cancelledBeforeReply", cancelledBeforeReply.sum());
    }

    private static void reply(Context context, StreamObserver<PlayReply> observer, PlayReply result) {
        if (context.isCancelled()) return;
        try { observer.onNext(result); observer.onCompleted(); }
        catch (RuntimeException ignored) { /* 客户端离开，已提交结果可从原权威回执查询。 */ }
    }
}

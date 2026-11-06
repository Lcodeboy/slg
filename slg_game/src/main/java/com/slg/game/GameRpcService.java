package com.slg.game;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.game.domain.GameException;
import com.slg.game.realm.ActivePlayer;
import com.slg.game.realm.ActivePlayers;
import com.slg.game.realm.MarchExecutionTarget;
import com.slg.game.realm.QueueDeadlineException;
import com.slg.game.realm.RealmMarchService;
import com.slg.game.storage.RealmJournal;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

/** GAME 的玩法入口。gRPC 线程核验令牌和预算；从卷读写、玩家状态推进均按账号邮箱顺序执行。 */
final class GameRpcService extends GameRpcGrpc.GameRpcImplBase {
    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final RealmMarchService service;
    private final ActivePlayers players;
    private final RealmJournal journal;
    private final TokenService tokens;
    private final BooleanSupplier ready;
    private final int maximumDeadlineMillis;
    private final RateLimiter rate = new RateLimiter(300, 10, 100_000);
    private final LongAdder cancelledCalls = new LongAdder();

    GameRpcService(TokenService tokens, ActivePlayers players, RealmMarchService service, RealmJournal journal,
                   BooleanSupplier ready, int maximumDeadlineMillis) {
        this.tokens = tokens;
        this.players = players;
        this.service = service;
        this.journal = journal;
        this.ready = ready;
        this.maximumDeadlineMillis = maximumDeadlineMillis;
    }

    @Override public void play(PlayRequest input, StreamObserver<PlayReply> observer) {
        var context = Context.current();
        if (observer instanceof ServerCallStreamObserver<?> server) server.setOnCancelHandler(() -> {});
        String account;
        try { account = tokens.verify(RpcMetadata.TOKEN.get()).accountId(); }
        catch (IllegalArgumentException invalid) {
            reply(context, observer, error(input, 401, "UNAUTHORIZED", "Valid session required"));
            return;
        }
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
        long deadline = System.nanoTime() + remaining;
        CompletableFuture<PlayReply> result;
        if (input.getMsgId() == GameplayRoutes.RECALL_MARCH) {
            // 只有召回要从请求取得行军 ID 以登记第二个执行对象。此刻只解码一次；
            // 解析矿点则必须在本玩家队首的读取线程进行，此时上一项出征已经提交。
            GameplayRoutes.Invocation call;
            try { call = GameplayRoutes.decode(input); }
            catch (InvalidProtocolBufferException malformed) {
                reply(context, observer, error(input, 400, "INVALID_REQUEST", "Invalid gameplay message body"));
                return;
            }
            String marchId = ((RecallMarchReq) call.body()).getMarchId();
            result = players.submitResolvingTarget(account, deadline,
                    player -> MarchExecutionTarget.forMarch(journal, player.playerId(), marchId),
                    player -> execute(input, call, player, context));
        } else {
            result = players.submit(account, deadline, player -> {
                if (context.isCancelled()) throw new QueueDeadlineException();
                try { return execute(input, GameplayRoutes.decode(input), player, context); }
                catch (InvalidProtocolBufferException malformed) {
                    return error(input, 400, "INVALID_REQUEST", "Invalid gameplay message body");
                }
            });
        }
        // 撤销的只是网络等待和未开始的排队。若批次已提交，玩家须凭原请求 ID 重试或查询。
        Context.CancellationListener cancel = ignored -> { if (result.cancel(false)) cancelledCalls.increment(); };
        context.addListener(cancel, Runnable::run);
        result.whenComplete((response, failure) -> {
            context.removeListener(cancel);
            if (!context.isCancelled()) reply(context, observer, failure == null ? response : failed(input, failure));
        });
    }

    private PlayReply execute(PlayRequest input, GameplayRoutes.Invocation call, ActivePlayer player, Context context)
            throws IOException, QueueDeadlineException {
        if (context.isCancelled()) throw new QueueDeadlineException();
        try {
            var response = call.dispatch(new GameplayHandler(service, player));
            if (response.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES)
                return error(input, 503, "RESULT_UNKNOWN", "Response exceeds limit; retry with original requestId");
            return response;
        } catch (GameException rejected) {
            // 已知业务拒绝直接回复。让它逃出邮箱会误把正常拒绝计为玩家状态损坏并驱逐内存。
            return error(input, 200, rejected.code(), rejected.getMessage());
        }
    }

    /** 实现生成接口只需绑定已认证玩家与领域方法，业务代码没有第二份编号映射。 */
    private record GameplayHandler(RealmMarchService service, ActivePlayer player) implements GameplayRoutes.Handlers {
        @Override public CreatePlayerRsp createPlayer(String id, CreatePlayerReq req) throws IOException {
            return service.createPlayer(player, id, req);
        }
        @Override public GetPlayerRsp getPlayer(String id, GetPlayerReq req) throws IOException {
            return service.getPlayer(player, id, req);
        }
        @Override public StartMarchRsp startMarch(String id, StartMarchReq req) throws IOException {
            return service.startMarch(player, id, req);
        }
        @Override public GetMarchRsp getMarch(String id, GetMarchReq req) throws IOException {
            return service.getMarch(player, id, req);
        }
        @Override public RecallMarchRsp recallMarch(String id, RecallMarchReq req) throws IOException {
            return service.recallMarch(player, id, req);
        }
        @Override public GetBattleRsp getBattle(String id, GetBattleReq req) throws IOException {
            return service.getBattle(player, id, req);
        }
        @Override public GetReceiptRsp getReceipt(String id, GetReceiptReq req) throws IOException {
            return service.getReceipt(player, id, req);
        }
    }

    private static PlayReply failed(PlayRequest request, Throwable cause) {
        if (cause instanceof QueueDeadlineException)
            return error(request, 504, "DEADLINE_EXCEEDED", "Command expired before execution");
        if (cause instanceof RejectedExecutionException)
            return error(request, 503, "SERVER_BUSY", "Command was not accepted; retry with the same requestId");
        if (cause instanceof ActivePlayers.LoadFailed)
            return error(request, 503, "PLAYER_LOAD_UNAVAILABLE", "Player could not be loaded; retry with the same requestId");
        if (cause instanceof ActivePlayers.TargetLoadFailed)
            return error(request, 503, "TARGET_LOAD_UNAVAILABLE", "March target could not be read; retry with the same requestId");
        return error(request, 503, "RESULT_UNKNOWN", "Command was not confirmed; retry with the same requestId");
    }

    private static PlayReply error(PlayRequest input, int status, String code, String message) {
        return PlayReply.newBuilder().setMsgId(input.getMsgId()).setRequestId(input.getRequestId()).setStatus(status)
                .setError(BusinessError.newBuilder().setCode(code).setMessage(message)).build();
    }

    Map<String,Object> metrics() { return Map.of("cancelledCalls", cancelledCalls.sum()); }

    private static void reply(Context context, StreamObserver<PlayReply> observer, PlayReply result) {
        if (context.isCancelled()) return;
        try { observer.onNext(result); observer.onCompleted(); }
        catch (RuntimeException ignored) { /* 客户端离开，已提交结果可从原卷回执查询。 */ }
    }
}

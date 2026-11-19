package com.slg.gate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.core.auth.SessionClaims;
import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.WireFrame;
import com.slg.rpc.WireStatus;
import com.slg.rpc.generated.WireRoutes;
import com.slg.rpc.v1.*;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 连接级状态只在 Netty EventLoop 修改。固定包头决定请求/响应/推送及执行域；业务正文保持原始
 * Protobuf 字节送入 GAME。玩家玩法、地图查询、地图订阅各有自己的有界名额，心跳在 Gateway 本地处理。
 * requestId 使用规范 UUID 字符串原样进入 GAME 回执键，重连重试不能换成连接内递增流水号。
 */
final class GameTcpHandler extends ConnectionHandler<WireFrame> {
    private static final Pattern SUBSCRIPTION_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final TokenService tokens;
    private final UpstreamClient upstream;
    private final RateLimiter requests, authRequests, commands, controls;
    private final RateLimiter mapRequests = new RateLimiter(10, 1, 1);
    private String token;
    private SessionClaims session;
    private SessionUpstreamClient.Lease binding;
    private CompletableFuture<SessionUpstreamClient.Lease> authenticating;
    private ScheduledFuture<?> authDeadline, sessionCheck;
    private CompletableFuture<PlayReply> pending;
    private CompletableFuture<MapQueryReply> pendingMap;
    private MapUpstreamClient.Watch mapWatch;
    private long mapWatchGeneration;
    private String mapWatchId, mapResetPending;
    private UUID heartbeatId;
    private long heartbeatNonce;

    GameTcpHandler(TokenService tokens, UpstreamClient upstream, RateLimiter requests, RateLimiter authRequests,
                   GatewaySettings settings) {
        this.tokens = tokens;
        this.upstream = upstream;
        this.requests = requests;
        this.authRequests = authRequests;
        commands = new RateLimiter(settings.commandsPerSecond(), 1, 1);
        controls = new RateLimiter(settings.controlFramesPerSecond(), 1, 1);
    }

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 认证期限从建连开始；不断发送半包或拖延 TLS 握手都不能无限占住未认证连接。
        authDeadline = ctx.executor().schedule(() -> closeWithError(ctx, "AUTH_TIMEOUT", "Authentication timeout"),
                10, TimeUnit.SECONDS);
        super.channelActive(ctx);
    }

    @Override protected void channelRead0(ChannelHandlerContext ctx, WireFrame frame) {
        if (closing) return;
        if (!ctx.channel().isWritable()) { ctx.close(); return; }
        try {
            if (token == null) {
                if (authenticating != null) {
                    closeWithError(ctx, "AUTH_IN_PROGRESS", "Wait for authentication acknowledgment");
                    return;
                }
                if (frame.kind() == WireFrame.REQUEST && frame.msgId() == WireRoutes.AUTH)
                    authenticate(ctx, frame, ((GatewayAuth) WireRoutes.parseRequest(frame.msgId(), frame.body())).getToken());
                else closeWithError(ctx, "UNAUTHORIZED", "First frame must authenticate the session");
                return;
            }
            if (!validSession(ctx)) return;
            if (frame.msgId() == WireRoutes.HEARTBEAT) { heartbeat(ctx, frame); return; }
            if (frame.kind() != WireFrame.REQUEST) {
                closeWithError(ctx, "INVALID_FRAME", "Client may only send requests or a heartbeat response");
                return;
            }
            if (!WireRoutes.clientMayRequest(frame.msgId())) {
                reject(ctx, frame, WireStatus.PROTOCOL_ERROR, "UNKNOWN_MSG_ID", "Unknown request message ID");
                return;
            }
            // 执行域来自同一份 proto 注册表。新增玩法不需要在 Gateway 手工维护 msgId 分支。
            switch (WireRoutes.domain(frame.msgId())) {
                case GAME -> play(ctx, frame);
                case AOI -> {
                    var body = WireRoutes.parseRequest(frame.msgId(), frame.body());
                    if (body instanceof MapQueryRequest query) queryMap(ctx, frame, query);
                    else if (body instanceof MapWatchRequest watch) watchMap(ctx, frame, watch);
                    else if (body instanceof MapWatchCancel cancel) cancelMap(ctx, frame, cancel);
                    else closeWithError(ctx, "INVALID_FRAME", "Unsupported map operation");
                }
                case GATEWAY -> closeWithError(ctx, "INVALID_FRAME", "Authentication cannot be repeated on this connection");
            }
        } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) {
            closeWithError(ctx, "INVALID_FRAME", "Invalid control message body");
        }
    }

    private void authenticate(ChannelHandlerContext ctx, WireFrame frame, String bearer) {
        if (!authRequests.allow(remote(ctx))) {
            closeWithError(ctx, "RATE_LIMITED", "Too many authentication attempts");
            return;
        }
        try { session = tokens.verify(bearer); }
        catch (IllegalArgumentException invalid) { closeWithError(ctx, "UNAUTHORIZED", "Valid session required"); return; }
        // 验签只证明 UC 登录凭证有效。必须先在 GAME 的账号邮箱中绑定代次，才可确认此 TCP 会话。
        // Prepare/Open 共用一次 deadline，失败不自动重新认证，避免迟到重试踢掉已经成功的新连接。
        var future = upstream.sessions().open(bearer, session.accountId(), reason ->
                dispatch(ctx, () -> closeWithError(ctx, reason, "Session no longer owns this connection")));
        authenticating = future;
        future.whenComplete((lease, failure) -> {
            // 普通回包的 dispatch 会丢弃已关闭连接上的工作；认证回调还拥有新 Lease，
            // 即使 TCP 已关闭也必须执行精确释放，不能让迟到认证留下无人持有的在线资格。
            try { ctx.executor().execute(() -> {
            if (authenticating != future || closing || !ctx.channel().isActive()) {
                if (lease != null) lease.close();
                return;
            }
            authenticating = null;
            if (failure != null) {
                var status = io.grpc.Status.fromThrowable(failure).getCode();
                String code = switch (status) {
                    case UNAUTHENTICATED -> "UNAUTHORIZED";
                    case FAILED_PRECONDITION -> "SESSION_REPLACED";
                    case RESOURCE_EXHAUSTED -> "SESSION_BUSY";
                    default -> "SESSION_UNAVAILABLE";
                };
                closeWithError(ctx, code, "Game session could not be established; reconnect explicitly");
                return;
            }
            binding = lease;
            token = bearer;
            if (!validSession(ctx)) return;
            authDeadline.cancel(false);
            sessionCheck = ctx.executor().scheduleAtFixedRate(() -> validSession(ctx), 1, 1, TimeUnit.SECONDS);
            send(ctx, WireFrame.response(frame.msgId(), frame.requestId(), GatewayAuthOk.newBuilder()
                    .setAccountId(session.accountId()).setExpiresAt(session.expiresAt())
                    .setGameInstance(binding.key().getGameInstance()).setSessionGeneration(binding.key().getGeneration()).build()));
            }); } catch (java.util.concurrent.RejectedExecutionException stopped) {
                if (lease != null) lease.close();
            }
        });
    }

    private boolean validSession(ChannelHandlerContext ctx) {
        if (closing) return false;
        if (session == null || !tokens.isCurrent(session)) {
            closeWithError(ctx, "UNAUTHORIZED", "Login credential expired");
            return false;
        }
        // Check 的撤销通知可能尚未在 EventLoop 执行；资格与原因从同一 Lease 状态读取，
        // 旧帧或巡检先到也必须保留 SESSION_REPLACED，不能一律改报 SESSION_EXPIRED。
        String reason = binding == null ? "SESSION_EXPIRED" : binding.invalidReason();
        if (reason == null) return true;
        closeWithError(ctx, reason, "Session no longer owns this connection");
        return false;
    }

    /** 服务端主动探测只保留一条待回显 UUID；不能把任意业务响应当作合法 PONG。 */
    private void heartbeat(ChannelHandlerContext ctx, WireFrame frame) throws InvalidProtocolBufferException {
        if (!controls.allow("control")) { closeWithError(ctx, "RATE_LIMITED", "Heartbeat rate exceeded"); return; }
        if (!validSession(ctx)) return;
        if (frame.kind() == WireFrame.REQUEST && WireRoutes.clientMayRequest(frame.msgId())) {
            var ping = (GatewayHeartbeatRequest) WireRoutes.parseRequest(frame.msgId(), frame.body());
            send(ctx, WireFrame.response(frame.msgId(), frame.requestId(),
                    GatewayHeartbeatReply.newBuilder().setNonce(ping.getNonce()).build()));
        } else if (frame.kind() == WireFrame.RESPONSE && frame.status() == WireStatus.OK
                && WireRoutes.serverMayRequest(frame.msgId())) {
            var pong = (GatewayHeartbeatReply) WireRoutes.parseResponse(frame.msgId(), frame.body());
            if (!frame.requestId().equals(heartbeatId) || pong.getNonce() != heartbeatNonce)
                closeWithError(ctx, "INVALID_FRAME", "Heartbeat response does not match its request");
            else heartbeatId = null;
        } else closeWithError(ctx, "INVALID_FRAME", "Invalid heartbeat direction or status");
    }

    /** 玩法同连接最多一个在途调用；这里不阻塞 EventLoop，也不创建无界等待队列。 */
    private void play(ChannelHandlerContext ctx, WireFrame frame) {
        if (!commands.allow("command") || !requests.allow(remote(ctx))) {
            closeWithError(ctx, "RATE_LIMITED", "Request rate exceeded");
            return;
        }
        if (!validSession(ctx)) return;
        if (pending != null) {
            reject(ctx, frame, WireStatus.NOT_ACCEPTED, "BUSY", "Wait for the preceding command");
            return;
        }
        var request = PlayRequest.newBuilder().setMsgId(frame.msgId()).setRequestId(frame.requestId().toString())
                .setBody(frame.body()).build();
        // TCP 正文上限不等于内部 RPC 消息上限：内部还需添加 msgId 和 UUID 字符串。
        // 在投递之前校验完整内部消息，明确回复未提交的协议错误，不能误报为结果未知。
        if (request.getSerializedSize() > RpcTransport.MAX_REQUEST_BYTES) {
            reject(ctx, frame, WireStatus.PROTOCOL_ERROR, "PAYLOAD_TOO_LARGE", "Internal command envelope exceeds budget");
            return;
        }
        // 断开只取消等待，GAME 可能已经提交。Gateway 不自动重发；客户端需使用原 UUID 查询或重试。
        pending = upstream.play(request, token, binding.key());
        pending.whenComplete((reply, error) -> dispatch(ctx, () -> {
            pending = null;
            if (!validSession(ctx)) return;
            if (error != null) {
                var failure = UpstreamClient.failure(error);
                int status = failure.code().endsWith("_UNKNOWN") ? WireStatus.OUTCOME_UNKNOWN : WireStatus.NOT_ACCEPTED;
                reject(ctx, frame, status, failure.code(), failure.getMessage());
            } else if (reply.getStatus() == 401) {
                closeWithError(ctx, reply.hasError() && reply.getError().getCode().equals("SESSION_REPLACED")
                        ? "SESSION_REPLACED" : "UNAUTHORIZED", "Session rejected");
            } else {
                int status = WireStatus.fromReply(reply);
                send(ctx, new WireFrame(WireFrame.RESPONSE, frame.msgId(), frame.requestId(), status,
                        reply.hasError() ? reply.getError().toByteString() : reply.getBody()));
            }
        }));
    }

    private void queryMap(ChannelHandlerContext ctx, WireFrame frame, MapQueryRequest request) {
        if (!allowMap(ctx)) return;
        if (!request.hasViewport()) {
            reject(ctx, frame, WireStatus.REJECTED, "INVALID_REQUEST", "Map viewport required");
            return;
        }
        if (pendingMap != null) {
            reject(ctx, frame, WireStatus.NOT_ACCEPTED, "MAP_BUSY", "Wait for the preceding map query");
            return;
        }
        var call = upstream.maps().query(request, token, binding.key());
        pendingMap = call;
        call.whenComplete((reply, error) -> dispatch(ctx, () -> {
            if (pendingMap != call) return;
            pendingMap = null;
            if (!validSession(ctx)) return;
            if (error != null) {
                String code = mapFailureCode(error);
                if (code.equals("UNAUTHORIZED")) { closeWithError(ctx, "SESSION_REPLACED", "Map session rejected"); return; }
                reject(ctx, frame, code.equals("MAP_ACCESS_DENIED") || code.equals("INVALID_REQUEST")
                        ? WireStatus.REJECTED : WireStatus.NOT_ACCEPTED, code, "Map query could not be completed");
            } else send(ctx, WireFrame.response(frame.msgId(), frame.requestId(), reply));
        }));
    }

    /**
     * ACK 仅表示 Gateway 已接受订阅控制请求，首个完整快照才表示视野可用。RPC 回调始终通过 dispatch
     * 排入 EventLoop，因此此处的 ACK 一定先写入 TCP，再写任何该订阅的首包或失败推送。
     */
    private void watchMap(ChannelHandlerContext ctx, WireFrame frame, MapWatchRequest request) {
        if (!allowMap(ctx)) return;
        if (!SUBSCRIPTION_ID.matcher(request.getSubscriptionId()).matches() || !request.hasViewport()) {
            reject(ctx, frame, WireStatus.REJECTED, "INVALID_REQUEST", "Map subscription ID and viewport required");
            return;
        }
        // 替换先结束旧视野。新请求若遇到池满，旧流也不恢复；客户端必须另发新 ID 并等待完整快照。
        stopMapWatch();
        long generation = mapWatchGeneration;
        mapWatchId = request.getSubscriptionId();
        try {
            mapWatch = upstream.maps().watch(request, token, binding.key(),
                    update -> dispatch(ctx, () -> deliverMap(ctx, generation, update)),
                    failure -> dispatch(ctx, () -> {
                        if (generation != mapWatchGeneration || mapWatchId == null) return;
                        if (mapFailureCode(failure).equals("UNAUTHORIZED")) {
                            closeWithError(ctx, "SESSION_REPLACED", "Map session rejected"); return;
                        }
                        String id = mapWatchId;
                        stopMapWatch();
                        mapReset(ctx, id, "MAP_RESYNC_REQUIRED", "Map stream ended; subscribe with a new ID");
                    }));
            send(ctx, WireFrame.response(frame.msgId(), frame.requestId(), MapWatchReply.newBuilder()
                    .setSubscriptionId(request.getSubscriptionId()).build()));
        } catch (RuntimeException failure) {
            stopMapWatch();
            // 池满属于受理前拒绝，不发送订阅 ACK，也不伪造已建立视野；玩法连接继续可用。
            reject(ctx, frame, WireStatus.NOT_ACCEPTED, mapFailureCode(failure), "Map subscription unavailable");
        }
    }

    private void cancelMap(ChannelHandlerContext ctx, WireFrame frame, MapWatchCancel request) {
        if (!allowMap(ctx)) return;
        if (!SUBSCRIPTION_ID.matcher(request.getSubscriptionId()).matches()) {
            reject(ctx, frame, WireStatus.REJECTED, "INVALID_REQUEST", "Map subscription ID required");
            return;
        }
        // 迟到的旧视野取消只确认该控制请求，不允许结束替换后的新订阅。
        if (request.getSubscriptionId().equals(mapWatchId)) stopMapWatch();
        send(ctx, WireFrame.response(frame.msgId(), frame.requestId(), MapCancelReply.newBuilder()
                .setSubscriptionId(request.getSubscriptionId()).build()));
    }

    private void deliverMap(ChannelHandlerContext ctx, long generation, MapViewUpdate update) {
        if (generation != mapWatchGeneration || mapWatch == null || !validSession(ctx)) return;
        if (!ctx.channel().isWritable()) {
            String id = mapWatchId;
            stopMapWatch();
            mapResetPending = id;
            return;
        }
        var frame = WireRoutes.push(update);
        if (frame.body().size() + GatewayWire.REMAINDER_HEADER_BYTES > GatewayWire.MAX_SERVER_FRAME_BYTES) {
            String id = mapWatchId;
            stopMapWatch();
            mapReset(ctx, id, "MAP_RESYNC_REQUIRED", "Map update exceeds transport budget");
            return;
        }
        if (update.hasError()) stopMapWatch();
        // 单条流只允许一个待发送消息；写完 TCP 之后才向 gRPC 再申请一个，慢连接不会形成无界队列。
        ctx.writeAndFlush(frame).addListener(done -> {
            if (!done.isSuccess()) { ctx.close(); return; }
            if (generation == mapWatchGeneration && mapWatch != null) mapWatch.requestNext();
        });
    }

    private boolean allowMap(ChannelHandlerContext ctx) {
        if (!validSession(ctx)) return false;
        if (mapRequests.allow("map")) return true;
        closeWithError(ctx, "RATE_LIMITED", "Map control rate exceeded");
        return false;
    }

    private void stopMapWatch() {
        mapWatchGeneration++;
        if (mapWatch != null) mapWatch.close();
        mapWatch = null;
        mapWatchId = null;
        mapResetPending = null;
    }

    private static String mapFailureCode(Throwable failure) {
        while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null)
            failure = failure.getCause();
        if (failure instanceof java.util.concurrent.RejectedExecutionException) return "MAP_BUSY";
        return switch (io.grpc.Status.fromThrowable(failure).getCode()) {
            case UNAUTHENTICATED -> "UNAUTHORIZED";
            case PERMISSION_DENIED -> "MAP_ACCESS_DENIED";
            case INVALID_ARGUMENT -> "INVALID_REQUEST";
            case RESOURCE_EXHAUSTED -> "MAP_BUSY";
            case DEADLINE_EXCEEDED -> "MAP_TIMEOUT";
            default -> "MAP_UNAVAILABLE";
        };
    }

    private void mapReset(ChannelHandlerContext ctx, String subscriptionId, String code, String message) {
        if (!ctx.channel().isWritable()) { mapResetPending = subscriptionId; return; }
        send(ctx, WireRoutes.push(MapViewUpdate.newBuilder().setSubscriptionId(subscriptionId)
                .setError(BusinessError.newBuilder().setCode(code).setMessage(message)).build()));
    }

    @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable() && mapResetPending != null) {
            String id = mapResetPending;
            mapResetPending = null;
            if (validSession(ctx)) mapReset(ctx, id, "MAP_RESYNC_REQUIRED", "Slow map consumer; subscribe again");
        }
        super.channelWritabilityChanged(ctx);
    }

    private void reject(ChannelHandlerContext ctx, WireFrame request, int status, String code, String message) {
        send(ctx, WireFrame.failure(request.msgId(), request.requestId(), status,
                BusinessError.newBuilder().setCode(code).setMessage(message).build()));
    }

    private void send(ChannelHandlerContext ctx, WireFrame frame) {
        if (!ctx.channel().isWritable() || frame.body().size() + GatewayWire.REMAINDER_HEADER_BYTES > GatewayWire.MAX_SERVER_FRAME_BYTES) {
            ctx.close(); return;
        }
        ctx.writeAndFlush(frame).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private void closeWithError(ChannelHandlerContext ctx, String code, String message) {
        if (closing) return;
        closing = true;
        if (!ctx.channel().isWritable()) { ctx.close(); return; }
        // 连接级错误独立注册为 PUSH，requestId 为零；不能伪造某次业务请求已失败的响应。
        ctx.writeAndFlush(WireRoutes.push(GatewayConnectionError.newBuilder()
                .setError(BusinessError.newBuilder().setCode(code).setMessage(message)).build()))
                .addListener(ChannelFutureListener.CLOSE);
    }

    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent idle && idle.state() == IdleState.WRITER_IDLE) {
            if (token != null && !closing && ctx.channel().isWritable() && heartbeatId == null) {
                heartbeatId = UUID.randomUUID();
                heartbeatNonce = System.nanoTime();
                send(ctx, WireFrame.request(WireRoutes.HEARTBEAT, heartbeatId,
                        GatewayHeartbeatRequest.newBuilder().setNonce(heartbeatNonce).build()));
            }
        } else super.userEventTriggered(ctx, event);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (authDeadline != null) authDeadline.cancel(false);
        if (sessionCheck != null) sessionCheck.cancel(false);
        if (authenticating != null) authenticating.cancel(false);
        authenticating = null;
        if (binding != null) binding.close();
        binding = null;
        if (pending != null) pending.cancel(true);
        if (pendingMap != null) pendingMap.cancel(true);
        stopMapWatch();
        token = null;
        session = null;
        heartbeatId = null;
        super.channelInactive(ctx);
    }
}

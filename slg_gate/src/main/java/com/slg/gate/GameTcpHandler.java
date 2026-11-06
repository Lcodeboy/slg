package com.slg.gate;

import com.slg.core.auth.SessionClaims;
import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.rpc.GatewayWire;
import com.slg.rpc.v1.BusinessError;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.PlayRequest;
import com.slg.rpc.v1.PlayReply;
import com.slg.rpc.v1.GatewayAuthOk;
import com.slg.rpc.v1.GatewayFrame;
import com.slg.rpc.v1.GatewayHeartbeat;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 一条 Unity 连接依次完成首帧鉴权、收发命令和心跳。GAME 接收原 Protobuf 命令与绑定的会话令牌。
 * 连接没有命令等待队列：一个命令等待 GAME 时仍可处理心跳，额外的命令当场返回带原 ID 的 BUSY。
 */
final class GameTcpHandler extends ConnectionHandler<GatewayFrame> {
    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final TokenService tokens;
    private final UpstreamClient upstream;
    private final RateLimiter requests, authRequests, commands, controls;
    private String token;
    private SessionClaims session;
    private ScheduledFuture<?> authDeadline, sessionCheck;
    private CompletableFuture<PlayReply> pending;

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
        // 从 TCP 建连时起计时。拆包、TLS 握手或不断发送部分字节都不能延长未鉴权连接的寿命。
        authDeadline = ctx.executor().schedule(() -> closeWithError(ctx, "AUTH_TIMEOUT", "Authentication timeout"),
                10, TimeUnit.SECONDS);
        super.channelActive(ctx);
    }

    @Override protected void channelRead0(ChannelHandlerContext ctx, GatewayFrame frame) {
        if (closing) return;
        if (!ctx.channel().isWritable()) { ctx.close(); return; }
        if (frame.getVersion() != GatewayWire.VERSION) {
            closeWithError(ctx, "UNSUPPORTED_PROTOCOL", "Unsupported gateway protocol version");
            return;
        }
        if (token == null) {
            if (frame.hasAuth()) authenticate(ctx, frame.getAuth().getToken());
            else closeWithError(ctx, "UNAUTHORIZED", "First frame must authenticate the session");
            return;
        }
        switch (frame.getBodyCase()) {
            case PLAY_REQUEST -> play(ctx, frame.getPlayRequest());
            case PING, PONG -> {
                // 控制帧另计限额，避免空闲探测与正常行军相互抢占配额。
                if (!controls.allow("control")) closeWithError(ctx, "RATE_LIMITED", "Heartbeat rate exceeded");
                else if (validSession(ctx) && frame.hasPing()) send(ctx, envelope().setPong(frame.getPing()));
            }
            default -> closeWithError(ctx, "INVALID_FRAME", "Unexpected client frame for authenticated session");
        }
    }

    private void authenticate(ChannelHandlerContext ctx, String bearer) {
        if (!authRequests.allow(remote(ctx))) {
            closeWithError(ctx, "RATE_LIMITED", "Too many authentication attempts");
            return;
        }
        try { session = tokens.verify(bearer); }
        catch (IllegalArgumentException invalid) { closeWithError(ctx, "UNAUTHORIZED", "Valid session required"); return; }
        token = bearer;
        authDeadline.cancel(false);
        // 即使对端一直安静，也要回收过期会话。每次投递及写回时再校验，覆盖两次检查之间的过期窗口。
        sessionCheck = ctx.executor().scheduleAtFixedRate(() -> validSession(ctx), 5, 5, TimeUnit.SECONDS);
        send(ctx, envelope().setAuthOk(GatewayAuthOk.newBuilder()
                .setAccountId(session.accountId()).setExpiresAt(session.expiresAt())));
    }

    private boolean validSession(ChannelHandlerContext ctx) {
        if (closing) return false;
        if (tokens.isCurrent(session)) return true;
        closeWithError(ctx, "UNAUTHORIZED", "Session expired");
        return false;
    }

    /** 所有玩法共用一个在途名额，心跳可以在 GAME 排队或提交期间继续处理。 */
    private void play(ChannelHandlerContext ctx, PlayRequest request) {
        if (!commands.allow("command") || !requests.allow(remote(ctx))) {
            closeWithError(ctx, "RATE_LIMITED", "Request rate exceeded");
            return;
        }
        if (!validSession(ctx)) return;
        String id = request.getRequestId();
        if (!REQUEST_ID.matcher(id).matches()) {
            reject(ctx, request.getMsgId(), "", 400, "INVALID_REQUEST", "Valid request ID required");
            return;
        }
        if (pending != null) {
            reject(ctx, request.getMsgId(), id, 429, "BUSY", "Wait for the preceding command");
            return;
        }
        // 未登记的消息号在网关结束。玩法正文原样送到 GAME，由协议生成的分发器选择解析类型。
        if (!GameplayRoutes.supports(request.getMsgId())) {
            reject(ctx, request.getMsgId(), id, 400, "UNKNOWN_MSG_ID", "Unknown gameplay message ID");
            return;
        }
        // 保留原 future 以便断连时取消 gRPC 等待。GAME 可能已经提交，网关不能自行补发。
        pending = upstream.play(request, token);
        pending.whenComplete((reply, error) -> dispatch(ctx, () -> {
            pending = null;
            if (!validSession(ctx)) return;
            if (error != null) {
                var failure = UpstreamClient.failure(error);
                reject(ctx, request.getMsgId(), id, failure.status(), failure.code(), failure.getMessage());
            } else if (reply.getStatus() == 401) {
                closeWithError(ctx, "UNAUTHORIZED", "Session rejected");
            } else {
                send(ctx, envelope().setPlayReply(reply));
            }
        }));
    }

    private static GatewayFrame.Builder envelope() {
        return GatewayFrame.newBuilder().setVersion(GatewayWire.VERSION);
    }

    private void reject(ChannelHandlerContext ctx, int msgId, String id, int status, String code, String message) {
        send(ctx, envelope().setPlayReply(PlayReply.newBuilder().setMsgId(msgId).setRequestId(id).setStatus(status)
                .setError(BusinessError.newBuilder().setCode(code).setMessage(message))));
    }

    private void send(ChannelHandlerContext ctx, GatewayFrame.Builder message) {
        GatewayFrame frame = message.build();
        if (!ctx.channel().isWritable() || frame.getSerializedSize() > GatewayWire.MAX_SERVER_FRAME_BYTES) {
            ctx.close();
            return;
        }
        ctx.writeAndFlush(frame).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    private void closeWithError(ChannelHandlerContext ctx, String code, String message) {
        if (closing) return;
        closing = true;
        if (!ctx.channel().isWritable()) { ctx.close(); return; }
        ctx.writeAndFlush(envelope().setError(BusinessError.newBuilder().setCode(code).setMessage(message)).build())
                .addListener(ChannelFutureListener.CLOSE);
    }

    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent idle && idle.state() == IdleState.WRITER_IDLE) {
            if (token != null && !closing && ctx.channel().isWritable())
                send(ctx, envelope().setPing(GatewayHeartbeat.newBuilder().setNonce(System.nanoTime())));
        } else super.userEventTriggered(ctx, event);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (authDeadline != null) authDeadline.cancel(false);
        if (sessionCheck != null) sessionCheck.cancel(false);
        if (pending != null) pending.cancel(true);
        token = null;
        session = null;
        super.channelInactive(ctx);
    }
}

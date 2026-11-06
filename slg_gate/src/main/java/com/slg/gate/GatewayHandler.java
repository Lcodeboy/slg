package com.slg.gate;

import com.slg.core.auth.TokenService;
import com.slg.core.http.HttpSupport;
import com.slg.core.http.RateLimiter;
import com.slg.core.http.HttpResult;
import com.slg.core.protocol.Json;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** 短 HTTP 接口处理账号凭证、区服列表和探活。每个连接至多等待一个 UC 调用。 */
final class GatewayHandler extends ConnectionHandler<FullHttpRequest> {
    private final TokenService tokens;
    private final UpstreamClient upstream;
    private final RateLimiter requests;
    private final RateLimiter authRequests;
    private final Supplier<Map<String, Object>> health;
    private CompletableFuture<UpstreamClient.Reply> httpCall;

    GatewayHandler(TokenService tokens, UpstreamClient upstream, RateLimiter requests, RateLimiter authRequests,
                   Supplier<Map<String, Object>> health) {
        this.tokens = tokens;
        this.upstream = upstream;
        this.requests = requests;
        this.authRequests = authRequests;
        this.health = health;
    }

    /** 校验 HTTP 路径、方法、认证和请求格式，再异步转发；同连接不并行处理普通 HTTP 请求。 */
    @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (closing) return;
        if (!request.decoderResult().isSuccess()) { httpError(ctx, 400, "INVALID_REQUEST", "Invalid HTTP request"); return; }
        if (httpCall != null) { httpError(ctx, 429, "BUSY", "One HTTP request at a time"); return; }
        URI uri;
        try { uri = URI.create(request.uri()); }
        catch (IllegalArgumentException e) { httpError(ctx, 400, "INVALID_REQUEST", "Invalid request path"); return; }
        // 禁止绝对地址和查询参数，避免变成开放代理，也避免令牌出现在 URL 日志中。
        if (uri.isAbsolute() || uri.getRawQuery() != null || uri.getFragment() != null) {
            httpError(ctx, 400, "INVALID_REQUEST", "Query strings, including session tokens, are not supported"); return;
        }
        String path = uri.getRawPath();
        if ("/health".equals(path) && request.method().equals(HttpMethod.GET)) {
            httpReply(ctx, 200, Json.write(health.get()), HttpUtil.isKeepAlive(request)); return;
        }
        if (!requests.allow(remote(ctx))) { httpError(ctx, 429, "RATE_LIMITED", "Request rate exceeded"); return; }
        // 白名单路由限定转发范围，注册/登录另有更严格的每 IP 配额。
        boolean publicAuth = "/api/auth/register".equals(path) || "/api/auth/login".equals(path);
        boolean realms = "/api/realms".equals(path);
        if (!publicAuth && !realms) { httpError(ctx, 404, "NOT_FOUND", "Endpoint not found"); return; }
        if (publicAuth && !authRequests.allow(remote(ctx))) {
            httpError(ctx, 429, "RATE_LIMITED", "Too many authentication attempts"); return;
        }
        if (!(realms ? request.method().equals(HttpMethod.GET) : request.method().equals(HttpMethod.POST))) {
            httpError(ctx, 405, "METHOD_NOT_ALLOWED", "HTTP method not supported"); return;
        }
        String bearer = null;
        if (!publicAuth) {
            try {
                if (request.headers().getAll(HttpHeaderNames.AUTHORIZATION).size() != 1) throw new IllegalArgumentException();
                bearer = HttpSupport.bearer(request.headers().get(HttpHeaderNames.AUTHORIZATION));
                tokens.verify(bearer);
            } catch (IllegalArgumentException e) { httpError(ctx, 401, "UNAUTHORIZED", "Valid session required"); return; }
        }
        String body = request.content().toString(CharsetUtil.UTF_8);
        if (!realms) {
            String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE, "");
            if (!contentType.toLowerCase(java.util.Locale.ROOT).startsWith("application/json")) {
                httpError(ctx, 415, "UNSUPPORTED_MEDIA_TYPE", "Use application/json"); return;
            }
        }
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        // 上游线程不能直接修改连接状态或写连接；回到所属 EventLoop 后串行收尾。
        httpCall = upstream.request(path, request.method().name(), body, bearer);
        httpCall.whenComplete((response, error) -> dispatch(ctx, () -> {
            httpCall = null;
            if (error != null) {
                var failure = UpstreamClient.failure(error);
                httpReply(ctx, failure.status(), Json.write(HttpResult.fail( failure.code(), failure.getMessage())), false); return;
            }
            httpReply(ctx, response.status(), response.body(), keepAlive);
        }));
    }

    /** 将入口错误转换成统一响应，并关闭连接以结束当前失败请求。 */
    private void httpError(ChannelHandlerContext ctx, int status, String code, String message) {
        httpReply(ctx, status, Json.write(HttpResult.fail( code, message)), false);
    }

    /** 写回 JSON 并禁止缓存会话/业务内容；是否复用连接沿用已校验请求的 Keep-Alive 语义。 */
    private void httpReply(ChannelHandlerContext ctx, int status, String body, boolean keepAlive) {
        if (!keepAlive) closing = true;
        if (!ctx.channel().isWritable()) { ctx.close(); return; }
        var bytes = Unpooled.copiedBuffer(body, CharsetUtil.UTF_8);
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(status), bytes);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
        response.headers().set("X-Content-Type-Options", "nosniff");
        HttpUtil.setContentLength(response, bytes.readableBytes());
        response.headers().set(HttpHeaderNames.CONNECTION, keepAlive ? HttpHeaderValues.KEEP_ALIVE : HttpHeaderValues.CLOSE);
        var written = ctx.writeAndFlush(response);
        if (!keepAlive) written.addListener(ChannelFutureListener.CLOSE);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (httpCall != null) httpCall.cancel(true);
        super.channelInactive(ctx);
    }
}

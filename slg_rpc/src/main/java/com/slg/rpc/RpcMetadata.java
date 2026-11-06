package com.slg.rpc;

import io.grpc.*;

/** 会话放在 RPC metadata；Context 仅携带原始令牌，身份必须由目标服务独立验签。 */
public final class RpcMetadata {
    public static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> TOKEN = Context.key("slg-bearer-token");
    private RpcMetadata() {}
    public static ServerInterceptor interceptor() {
        return new ServerInterceptor() {
            @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                String authorization = headers.get(AUTHORIZATION);
                String token = authorization != null && authorization.startsWith("Bearer ") ? authorization.substring(7) : null;
                return Contexts.interceptCall(Context.current().withValue(TOKEN, token), call, headers, next);
            }
        };
    }
}

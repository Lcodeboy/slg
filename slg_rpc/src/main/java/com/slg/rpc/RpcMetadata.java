package com.slg.rpc;

import io.grpc.*;
import com.slg.rpc.v1.SessionKey;

/**
 * RPC metadata 只携带原始登录凭证和 GAME 签发的连接绑定，解析成功不等于授权成功。
 * 目标服务必须独立验签，并向当前实例的会话目录核验完整绑定；缺字段或非法代次视为无绑定。
 */
public final class RpcMetadata {
    public static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> TOKEN = Context.key("slg-bearer-token");
    public static final Context.Key<SessionKey> SESSION = Context.key("slg-game-session");
    private static final Metadata.Key<String> SESSION_ACCOUNT = Metadata.Key.of("slg-session-account", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SESSION_INSTANCE = Metadata.Key.of("slg-session-instance", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SESSION_GENERATION = Metadata.Key.of("slg-session-generation", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SESSION_BINDING = Metadata.Key.of("slg-session-binding", Metadata.ASCII_STRING_MARSHALLER);
    private RpcMetadata() {}
    public static ServerInterceptor interceptor() {
        return new ServerInterceptor() {
            @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                String authorization = headers.get(AUTHORIZATION);
                String token = authorization != null && authorization.startsWith("Bearer ") ? authorization.substring(7) : null;
                return Contexts.interceptCall(Context.current().withValues(TOKEN, token, SESSION, session(headers)), call, headers, next);
            }
        };
    }

    /** 绑定由 Gateway 认证控制流程取得，绝不从 Unity 的业务正文或自报账号复制。 */
    public static Metadata headers(String token, SessionKey binding) {
        var headers = new Metadata();
        if (token != null) headers.put(AUTHORIZATION, "Bearer " + token);
        if (binding != null) {
            headers.put(SESSION_ACCOUNT, binding.getAccountId());
            headers.put(SESSION_INSTANCE, binding.getGameInstance());
            headers.put(SESSION_GENERATION, Long.toString(binding.getGeneration()));
            headers.put(SESSION_BINDING, binding.getBindingId());
        }
        return headers;
    }

    private static SessionKey session(Metadata headers) {
        String account = headers.get(SESSION_ACCOUNT), instance = headers.get(SESSION_INSTANCE),
                generation = headers.get(SESSION_GENERATION), binding = headers.get(SESSION_BINDING);
        if (account == null || instance == null || generation == null || binding == null) return null;
        try {
            long parsed = Long.parseLong(generation);
            if (parsed <= 0 || !Long.toString(parsed).equals(generation)) return null;
            return SessionKey.newBuilder().setAccountId(account).setGameInstance(instance)
                    .setGeneration(parsed).setBindingId(binding).build();
        } catch (IllegalArgumentException invalid) { return null; }
    }
}

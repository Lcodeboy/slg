package com.slg.rpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/** 统一 HTTP/2 通道与 TLS 配置；本地开发明文，生产可用 TLS 或双向证书认证。 */
public final class RpcTransport {
    public static final int MAX_REQUEST_BYTES = 65_536, MAX_RESPONSE_BYTES = 1_048_576;
    /** 单条 HTTP/2 连接的传输上限；网关长流池的每连接预算不得超过它。 */
    public static final int MAX_CONCURRENT_CALLS_PER_CONNECTION = 512;
    private RpcTransport() {}
    public static NettyServerBuilder serverBuilder(InetSocketAddress address) {
        return serverBuilder(address, System.getenv());
    }
    static NettyServerBuilder serverBuilder(InetSocketAddress address, Map<String, String> environment) {
        var builder = NettyServerBuilder.forAddress(address).directExecutor()
                .maxInboundMessageSize(MAX_REQUEST_BYTES).maxInboundMetadataSize(8192)
                .maxConcurrentCallsPerConnection(MAX_CONCURRENT_CALLS_PER_CONNECTION).permitKeepAliveWithoutCalls(false)
                .permitKeepAliveTime(30, TimeUnit.SECONDS).intercept(RpcMetadata.interceptor());
        String certificate = env(environment, "SLG_RPC_TLS_CERT"), key = env(environment, "SLG_RPC_TLS_KEY"), ca = env(environment, "SLG_RPC_TLS_CA");
        if ((certificate == null) != (key == null)) throw new IllegalArgumentException("RPC TLS certificate and key must be supplied together");
        if ("true".equalsIgnoreCase(env(environment, "SLG_RPC_TLS_ENABLED")) && certificate == null)
            throw new IllegalArgumentException("RPC server TLS requires a certificate and key");
        if (certificate != null) {
            try {
                var tls = GrpcSslContexts.forServer(new File(certificate), new File(key));
                if (ca != null) tls.trustManager(new File(ca)).clientAuth(ClientAuth.REQUIRE);
                builder.sslContext(tls.build());
            } catch (SSLException failure) { throw new IllegalArgumentException("Cannot configure RPC server TLS"); }
        } else if (ca != null) throw new IllegalArgumentException("RPC server CA requires a server certificate and key");
        return builder;
    }
    public static ManagedChannel channel(String target) {
        return channel(target, System.getenv());
    }
    static ManagedChannel channel(String target, Map<String, String> environment) {
        if (target == null || target.isBlank() || target.contains("@") || target.contains("?") || target.contains("#"))
            throw new IllegalArgumentException("RPC target must be a host:port or DNS target");
        var builder = NettyChannelBuilder.forTarget(target).maxInboundMessageSize(MAX_RESPONSE_BYTES)
                .maxInboundMetadataSize(8192).disableRetry().keepAliveTime(60, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveWithoutCalls(false);
        String ca = env(environment, "SLG_RPC_TLS_CA"), certificate = env(environment, "SLG_RPC_CLIENT_TLS_CERT"), key = env(environment, "SLG_RPC_CLIENT_TLS_KEY");
        if ((certificate == null) != (key == null)) throw new IllegalArgumentException("RPC client certificate and key must be supplied together");
        if (ca != null || certificate != null || "true".equalsIgnoreCase(env(environment, "SLG_RPC_TLS_ENABLED"))) {
            try {
                var tls = GrpcSslContexts.forClient();
                if (ca != null) tls.trustManager(new File(ca));
                if (certificate != null) tls.keyManager(new File(certificate), new File(key));
                builder.sslContext(tls.build());
            } catch (SSLException failure) { throw new IllegalArgumentException("Cannot configure RPC client TLS"); }
        } else builder.usePlaintext();
        return builder.build();
    }
    private static String env(Map<String, String> environment, String key) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? null : value;
    }
}

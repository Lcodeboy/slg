package com.slg.rpc;

import com.google.common.net.InetAddresses;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** 战斗 RPC 的双向信任分别配置。Worker 的服务端证书和 GAME 的客户端证书可以来自不同的 CA。 */
public final class BattleRpcTransport {
    private static final String GAME_CLIENT_CA = "SLG_BATTLE_GAME_CLIENT_CA";
    private BattleRpcTransport() {}

    public static NettyServerBuilder serverBuilder(InetSocketAddress address) {
        return serverBuilder(address, System.getenv());
    }

    static NettyServerBuilder serverBuilder(InetSocketAddress address, Map<String, String> settings) {
        if (address.isUnresolved()) throw new IllegalArgumentException("Unresolved battle RPC bind address");
        boolean tls = tlsEnabled(settings) || present(settings, "SLG_RPC_TLS_CERT") || present(settings, "SLG_RPC_TLS_KEY");
        if (!present(settings, GAME_CLIENT_CA)) {
            if (!address.getAddress().isLoopbackAddress() || tls)
                throw new IllegalArgumentException("Battle RPC requires SLG_BATTLE_GAME_CLIENT_CA for client authentication");
            return RpcTransport.serverBuilder(address, settings);
        }
        // 通用服务 CA 不能替代 GAME 身份根。这里只覆盖 Worker 验证客户端时使用的根。
        var workerSettings = new HashMap<>(settings);
        workerSettings.put("SLG_RPC_TLS_CA", settings.get(GAME_CLIENT_CA));
        return RpcTransport.serverBuilder(address, workerSettings);
    }

    static ManagedChannel channel(String target) { return channel(target, System.getenv()); }

    static ManagedChannel channel(String target, Map<String, String> settings) {
        boolean local = loopbackTarget(target);
        boolean ca = present(settings, "SLG_RPC_TLS_CA");
        boolean cert = present(settings, "SLG_RPC_CLIENT_TLS_CERT");
        boolean key = present(settings, "SLG_RPC_CLIENT_TLS_KEY");
        if ((!local || tlsEnabled(settings) || cert || key) && !(ca && cert && key))
            throw new IllegalArgumentException("Battle RPC requires a trusted server CA and GAME client certificate and key");
        return RpcTransport.channel(target, settings);
    }

    /** 只对显式回环地址放行明文。绝不使用当前 DNS 解析结果判断远端服务是否本地。 */
    static boolean loopbackTarget(String target) {
        if (target == null || target.isBlank() || target.contains("%")) throw invalidTarget();
        String authority = target.startsWith("dns:///") ? target.substring("dns:///".length()) : target;
        URI uri;
        try { uri = URI.create("tcp://" + authority); }
        catch (IllegalArgumentException malformed) { throw invalidTarget(); }
        if (uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65_535 || uri.getRawUserInfo() != null
                || !uri.getRawPath().isEmpty() || uri.getRawQuery() != null || uri.getRawFragment() != null)
            throw invalidTarget();
        String host = uri.getHost();
        if (host.equalsIgnoreCase("localhost")) return true;
        if (host.startsWith("[")) host = host.substring(1, host.length() - 1);
        return InetAddresses.isInetAddress(host) && InetAddresses.forString(host).isLoopbackAddress();
    }

    private static IllegalArgumentException invalidTarget() {
        return new IllegalArgumentException("Battle RPC target must be host:port or dns:///host:port");
    }
    private static boolean tlsEnabled(Map<String, String> settings) {
        return "true".equalsIgnoreCase(settings.get("SLG_RPC_TLS_ENABLED")) || present(settings, "SLG_RPC_TLS_CA");
    }
    private static boolean present(Map<String, String> settings, String key) {
        String value = settings.get(key);
        return value != null && !value.isBlank();
    }
}

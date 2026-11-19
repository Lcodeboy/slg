package com.slg.game;

import com.slg.aoi.AoiRuntime;
import com.slg.core.auth.TokenService;
import com.slg.core.http.HttpSupport;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.rpc.RpcTransport;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** 网络、地图与观测装配；health 只读内存，数据库探测在独立控制通道执行。 */
final class RealmTransportRuntime implements AutoCloseable {
    private final MySqlRealmRuntime runtime;
    private AoiRuntime aoi;
    private AoiRpcService mapRpc;
    private MapFeedService feed;
    private SessionMetrics sessionMetrics;
    private ProcessObservability observations;
    private GameRpcService game;
    private volatile Server rpc;
    private volatile boolean stopping;
    private HttpServer health;
    private boolean closed;
    RealmTransportRuntime(MySqlRealmRuntime runtime) { this.runtime = runtime; }

    void initialize(TokenService tokens, RealmIdentity realm, Map<String, String> env,
                       long startedAt, long startedNanos) throws Exception {
        String host = env.getOrDefault("SLG_BIND", "127.0.0.1");
        var rpcAddress = new InetSocketAddress(env.getOrDefault("SLG_RPC_BIND", host), port(env, "SLG_GAME_RPC_PORT", 9082));
        var healthAddress = new InetSocketAddress(host, port(env, "SLG_GAME_PORT", 8082));
        observations = ProcessObservability.authority(runtime::connections, startedAt, startedNanos, runtime.startupNanos());
        sessionMetrics = SessionMetrics.register(realm.realmId(), runtime.sessions());
        aoi = new AoiRuntime(runtime.mapSource(), AoiRuntime.Settings.fromEnvironment(env));
        mapRpc = new AoiRpcService(aoi, tokens, runtime::ready, runtime.mapAccess()::allows, runtime.sessions());
        feed = MapFeedService.configured(runtime.mapSource(), runtime::ready, rpcAddress, env);
        game = new GameRpcService(tokens, runtime.players(), runtime::ready, runtime.settings().deadlineMillis(),
                runtime.sessions(), runtime.marches(), runtime.background());
        var sessionRpc = new SessionRpcService(tokens, runtime.players(), runtime.sessions(), runtime::ready,
                runtime.settings().deadlineMillis());
        var builder = RpcTransport.serverBuilder(rpcAddress).addService(game).addService(sessionRpc).addService(mapRpc);
        if (feed != null) builder.addService(feed);
        rpc = builder.build();
        health = HttpServer.create(healthAddress, 16);
        health.createContext("/health", exchange -> {
            try (exchange) {
                if (!exchange.getRequestMethod().equals("GET") || !exchange.getRequestURI().getPath().equals("/health")) {
                    exchange.sendResponseHeaders(404, -1); return;
                }
                var values = new LinkedHashMap<String, Object>(runtime.health());
                boolean ready = runtime.ready() && !rpc.isShutdown();
                values.put("status", ready ? "UP" : "DEGRADED");
                values.put("aoi", aoi.metrics()); values.put("sessions", runtime.sessions().metrics());
                values.put("rpc", game.metrics()); values.put("observability", observations.metrics());
                HttpSupport.json(exchange, ready ? 200 : 503, values);
            }
        });
    }
    void start() throws Exception {
        if (stopping) throw new IllegalStateException("Transport stopped during initialization");
        aoi.start(); health.start(); rpc.start();
        // SIGTERM 可与启动交错。即便停止发生在 start 检查之后，也不能留下重新开放的 RPC。
        if (stopping) rpc.shutdownNow();
    }
    void markReady() { observations.markReady(); }
    int rpcPort() { return rpc.getPort(); }
    int healthPort() { return health.getAddress().getPort(); }
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException { return rpc.awaitTermination(timeout, unit); }
    void awaitTermination() throws InterruptedException { rpc.awaitTermination(); }
    void stopRequests() {
        stopping = true;
        var current = rpc;
        if (current != null) current.shutdownNow();
    }

    @Override public synchronized void close() throws Exception {
        if (closed) return;
        runtime.stopAccepting(); stopRequests();
        if (health != null) health.stop(0);
        if (rpc != null && !rpc.awaitTermination(10, TimeUnit.SECONDS))
            throw new IOException("RPC transport did not stop; authority resources remain open");
        // 地图授权读取与流式快照仍可能持连接，必须等其退出才允许外层关闭数据源。
        if (mapRpc != null) mapRpc.close();
        if (feed != null) feed.close();
        if (aoi != null) aoi.close();
        if (observations != null) observations.close();
        if (sessionMetrics != null) sessionMetrics.close();
        closed = true;
    }
    private static int port(Map<String, String> env, String key, int fallback) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < 1 || value > 65535) throw new IllegalArgumentException("Invalid listener port: " + key);
        return value;
    }
}

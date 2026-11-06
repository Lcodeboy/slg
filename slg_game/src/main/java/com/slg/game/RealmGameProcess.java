package com.slg.game;

import com.slg.core.auth.TokenService;
import com.slg.core.http.HttpSupport;
import com.slg.game.projection.RealmProjection;
import com.slg.game.realm.ActivePlayers;
import com.slg.game.realm.RealmBattlePump;
import com.slg.game.realm.RealmMarchService;
import com.slg.game.realm.RealmTaskPump;
import com.slg.game.storage.RealmVolume;
import com.slg.game.storage.RealmDiskGuard;
import com.slg.game.storage.JournalRetentionMonitor;
import com.slg.rpc.BattleWorkerClient;
import com.slg.rpc.RpcTransport;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** 新区的可执行 GAME。先打开并验证登记的原卷，再向 Gateway 开放命令端口。 */
final class RealmGameProcess {
    private RealmGameProcess() {}

    static void serve(Path directory, int realmId, UUID volumeId, PrintStream out)
            throws IOException, InterruptedException {
        Clock clock = Clock.systemUTC();
        TokenService tokens = new TokenService(System.getenv("SLG_TOKEN_SECRET"), clock);
        var settings = GameServer.Settings.fromEnvironment(Math.min(Runtime.getRuntime().availableProcessors(), 8));
        String host = HttpSupport.env("SLG_BIND", "127.0.0.1");
        var rpcAddress = new InetSocketAddress(HttpSupport.env("SLG_RPC_BIND", host),
                HttpSupport.port("SLG_GAME_RPC_PORT", 9082));
        var healthAddress = new InetSocketAddress(host, HttpSupport.port("SLG_GAME_PORT", 8082));

        CountDownLatch released = new CountDownLatch(1);
        Thread stop = null;
        try (RealmDiskGuard disk = RealmDiskGuard.configure(directory, System.getenv());
             RealmVolume volume = RealmVolume.open(directory, realmId, volumeId);
             RealmProjection projection = RealmProjection.configure(volume.journal(), realmId, volumeId, System.getenv());
             JournalRetentionMonitor retention = new JournalRetentionMonitor(volume.journal(), realmId, volumeId, System.getenv())) {
            var journal = volume.journal();
            var service = new RealmMarchService(journal, clock, projection::admitsClientWrite, disk::admitsClientWrite);
            try (ActivePlayers players = new ActivePlayers(service::loadPlayer, settings.workers(),
                    setting("SLG_REALM_LOAD_WORKERS", Math.min(4, settings.workers()), 32),
                    settings.queueCapacity(), settings.perAccountCapacity(),
                    setting("SLG_REALM_MAX_RESIDENTS", 16_384, 1_000_000),
                    Duration.ofSeconds(setting("SLG_REALM_PLAYER_IDLE_SECONDS", 300, 86_400)));
                 RealmTaskPump tasks = new RealmTaskPump(journal, players, service, clock,
                         setting("SLG_REALM_TIMER_BATCH", Math.min(16, settings.queueCapacity()), 1024));
                 BattleWorkerClient worker = new BattleWorkerClient(HttpSupport.env("SLG_BATTLE_RPC_TARGET", "127.0.0.1:9083"));
                 RealmBattlePump battles = new RealmBattlePump(journal, players, service, worker::compute,
                         setting("SLG_REALM_BATTLE_INFLIGHT", 16, 64))) {
                AtomicBoolean accepting = new AtomicBoolean();
                var rpc = RpcTransport.serverBuilder(rpcAddress)
                        .addService(new GameRpcService(tokens, players, service, journal,
                                () -> accepting.get() && tasks.healthy() && battles.healthy(),
                                settings.deadlineMillis())).build();
                var health = HttpServer.create();
                health.createContext("/health", exchange -> {
                    try (exchange) {
                        if (!exchange.getRequestMethod().equals("GET") || !exchange.getRequestURI().getPath().equals("/health")) {
                            exchange.sendResponseHeaders(404, -1);
                            return;
                        }
                        long high;
                        try {
                            high = journal.highSequence();
                        } catch (IOException unavailable) {
                            HttpSupport.json(exchange, 503, Map.of("status", "DEGRADED", "realmId", realmId,
                                    "volumeId", volumeId, "disk", disk.metrics(), "journalRetention",
                                    retention.unavailable(projection.retentionConsumer(realmId, volumeId))));
                            return;
                        }
                        boolean ready = accepting.get() && tasks.healthy() && battles.healthy() && !rpc.isShutdown();
                        HttpSupport.json(exchange, ready ? 200 : 503, Map.of("status", ready ? "UP" : "DEGRADED",
                                "realmId", realmId, "volumeId", volumeId, "journalSeq", high,
                                "players", players.metrics(), "tasks", tasks.metrics(), "battles", battles.metrics(),
                                "projection", projection.metrics(high), "disk", disk.metrics(),
                                "journalRetention", retention.preview(high, projection.retentionConsumer(realmId, volumeId))));
                    }
                });
                try {
                    health.bind(healthAddress, 16);
                    disk.start();
                    retention.start();
                    rpc.start();
                    projection.start(); // 就绪检查不借 SQL 连接；恢复期间客户端从原卷读写。
                    tasks.start();
                    battles.start();
                    accepting.set(true);
                    health.start();
                    stop = new Thread(() -> stopAndWait(rpc, released), "realm-shutdown");
                    Runtime.getRuntime().addShutdownHook(stop);
                    out.printf("Realm %d ready on RPC port %d (health %d)%n",
                            realmId, rpc.getPort(), health.getAddress().getPort());
                    rpc.awaitTermination();
                } finally {
                    accepting.set(false);
                    rpc.shutdownNow();
                    health.stop(0);
                }
            } // 先停任务发现，再排空玩家目录，随后关闭投影线程、释放原卷。
        } finally {
            released.countDown();
            if (stop != null) {
                try { Runtime.getRuntime().removeShutdownHook(stop); }
                catch (IllegalStateException shuttingDown) { /* JVM 正在运行该钩子。 */ }
            }
        }
    }

    private static int setting(String key, int fallback, int max) {
        int value = Integer.parseInt(HttpSupport.env(key, Integer.toString(fallback)));
        if (value < 1 || value > max) throw new IllegalArgumentException(key + " must be in 1.." + max);
        return value;
    }

    private static void stopAndWait(Server rpc, CountDownLatch released) {
        rpc.shutdownNow();
        try { released.await(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}

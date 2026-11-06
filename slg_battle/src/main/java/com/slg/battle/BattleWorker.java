package com.slg.battle;

import com.slg.core.battle.ExpeditionBattleEngine;
import com.slg.core.http.HttpSupport;
import com.slg.rpc.BattleRpcTransport;
import io.grpc.Server;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** 独立的纯计算进程；没有数据卷、数据库账号或对 GAME 的写入接口。 */
public final class BattleWorker {
    private BattleWorker() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        var address = new InetSocketAddress(HttpSupport.env("SLG_BATTLE_RPC_BIND", "127.0.0.1"),
                HttpSupport.port("SLG_BATTLE_RPC_PORT", 9083));
        int threads = setting("SLG_BATTLE_WORKERS", Math.min(8, Runtime.getRuntime().availableProcessors()), 128);
        int capacity = setting("SLG_BATTLE_QUEUE_CAPACITY", 64, 4096);
        var pool = pool(threads, capacity);
        Server rpc = BattleRpcTransport.serverBuilder(address)
                .addService(new BattleService(pool, new ExpeditionBattleEngine()::simulate)).build();
        Thread stop = new Thread(() -> { rpc.shutdownNow(); pool.shutdownNow(); }, "battle-shutdown");
        try {
            rpc.start();
            Runtime.getRuntime().addShutdownHook(stop);
            System.out.println("Battle Worker ready on RPC port " + rpc.getPort());
            rpc.awaitTermination();
        } finally {
            rpc.shutdownNow();
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            try { Runtime.getRuntime().removeShutdownHook(stop); }
            catch (IllegalStateException shuttingDown) { /* JVM 已经开始有序关闭。 */ }
        }
    }

    static ThreadPoolExecutor pool(int threads, int capacity) {
        return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(capacity),
                Thread.ofPlatform().daemon().name("battle-compute-", 0).factory());
    }

    private static int setting(String key, int fallback, int max) {
        int n = Integer.parseInt(HttpSupport.env(key, Integer.toString(fallback)));
        if (n < 1 || n > max) throw new IllegalArgumentException(key + " must be in 1.." + max);
        return n;
    }
}

package com.slg.game;

import com.slg.core.auth.TokenService;
import com.slg.game.domain.persistence.RealmIdentity;
import java.io.PrintStream;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** MySQL 区服进程；资源装配与网络分层，正常启动不建库、不迁移、不回退旧原卷。 */
final class RealmGameProcess {
    private RealmGameProcess() { }

    static void serve(RealmIdentity realm, Map<String, String> env, PrintStream out) throws Exception {
        long started = System.nanoTime(), epochMillis = System.currentTimeMillis();
        var tokens = new TokenService(env.get("SLG_TOKEN_SECRET"), Clock.systemUTC());
        var runtime = MySqlRealmRuntime.open(realm, env);
        RealmTransportRuntime network = null;
        Thread hook = null;
        var released = new CountDownLatch(1);
        try {
            network = new RealmTransportRuntime(runtime);
            var transport = network;
            hook = new Thread(() -> {
                // 钩子只摘流并唤醒主线程；同一个关闭线程负责真实排空，不并发关闭连接池。
                runtime.stopAccepting(); transport.stopRequests();
                boolean interrupted = false;
                for (;;) {
                    try { released.await(); break; }
                    catch (InterruptedException stop) { interrupted = true; }
                }
                if (interrupted) Thread.currentThread().interrupt();
            }, "realm-authority-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            // 网络初始化可能绑定端口；先装钩子，SIGTERM 落在部分初始化时也由 finally 逐层释放。
            network.initialize(tokens, realm, env, epochMillis, started);
            network.start(); runtime.start();
            out.printf("Realm %d starting: dataset=%s; waiting for recovery scans%n", realm.realmId(), realm.datasetId());
            // acquire 已隔离旧写者；只等两类首次有界扫描，不要求清空全部积压才开放客户端。
            while (!runtime.ready() && !network.awaitTermination(100, TimeUnit.MILLISECONDS)) { }
            if (runtime.ready()) {
                network.markReady();
                out.printf("Realm %d ready on RPC %d (health %d), dataset=%s%n",
                        realm.realmId(), network.rpcPort(), network.healthPort(), realm.datasetId());
            }
            network.awaitTermination();
        } finally {
            runtime.stopAccepting();
            try {
                // 不用资源列表自动连锁关闭：上层未排空时不得关闭其依赖的数据库与写权。
                if (network != null) network.close();
                runtime.close();
            } finally {
                released.countDown();
                if (hook != null) try { Runtime.getRuntime().removeShutdownHook(hook); }
                catch (IllegalStateException shuttingDown) { /* JVM 已在执行该钩子。 */ }
            }
        }
    }
}

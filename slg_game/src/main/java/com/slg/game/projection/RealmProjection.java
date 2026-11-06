package com.slg.game.projection;

import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.JournalRetentionPreview;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 一个区服只需一个顺序消费线程。玩家只等 RocksDB 原子提交；SQL 获取连接、读取游标、追赶积压和
 * 重试全部在这里进行。已提交日志本身就是可恢复的待办，因此无需在玩家线程向另一条队列双写消息。
 */
public final class RealmProjection implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(RealmProjection.class.getName());
    private static final int IDLE_POLL_MS = 200;
    private final RealmJournal journal;
    private final HikariDataSource pool;
    private final MySqlProjectionStore store;
    private final int batchSize;
    private final long maxLag;
    private final Thread worker;
    private volatile boolean closed;
    // 健康探针和准入检查只读同一份不可变快照，既不借 SQL 连接，也不等待正在执行的事务。
    private record Progress(String state, Long applied, long failures, String diagnostic, long confirmedNanos) {}
    private volatile Progress progress;

    private RealmProjection(RealmJournal journal, int realmId, UUID volumeId, HikariDataSource pool,
                            int batchSize, long maxLag) {
        this.journal = journal;
        this.pool = pool;
        this.batchSize = batchSize;
        this.maxLag = maxLag;
        this.store = pool == null ? null : new MySqlProjectionStore(pool, realmId, volumeId);
        this.worker = pool == null ? null : Thread.ofPlatform().daemon()
                .name("realm-" + realmId + "-mysql-projection").unstarted(this::consume);
        this.progress = new Progress(pool == null ? "DISABLED" : "STARTING", null, 0, "", 0);
    }

    /**
     * 正常运行默认开启。配置缺失应在开始接流前报错；网络暂时不通则是后台可重试的故障。
     * 独立凭据、单连接池和有限的连接/套接字等待，防止慢投影耗尽 UC 或玩家工作线程的预算。
     * 池不预填连接，也不做首次联网校验。首次借用来自消费线程，因此数据库离线时 GAME 仍可从原卷启动。
     */
    public static RealmProjection configure(RealmJournal journal, int realmId, UUID volumeId, Map<String, String> env) {
        String enabled = env.getOrDefault("SLG_PROJECTION_ENABLED", "true");
        if ("false".equalsIgnoreCase(enabled)) return new RealmProjection(journal, realmId, volumeId, null, 0, 0);
        if (!"true".equalsIgnoreCase(enabled))
            throw new IllegalArgumentException("SLG_PROJECTION_ENABLED must be true or false");
        String url = env.get("SLG_PROJECTION_JDBC_URL");
        String user = env.getOrDefault("SLG_PROJECTION_DB_USER", "slg_projection");
        String password = env.get("SLG_PROJECTION_DB_PASSWORD");
        if (url == null || !url.startsWith("jdbc:mysql://") || user.isBlank() || password == null || password.isEmpty())
            throw new IllegalArgumentException("Set a MySQL SLG_PROJECTION_JDBC_URL and dedicated projection credentials");
        int size = setting(env, "SLG_PROJECTION_BATCH_SIZE", 64, 128);
        int maxLag = setting(env, "SLG_PROJECTION_MAX_LAG", 100_000, Integer.MAX_VALUE);
        var config = new HikariConfig();
        config.setPoolName("realm-" + realmId + "-projection");
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(2000);
        config.setValidationTimeout(1000);
        config.setInitializationFailTimeout(-1);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        config.addDataSourceProperty("connectTimeout", "2000");
        config.addDataSourceProperty("socketTimeout", "5000");
        config.addDataSourceProperty("tcpKeepAlive", "true");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        return new RealmProjection(journal, realmId, volumeId, new HikariDataSource(config), size, maxLag);
    }

    private static int setting(Map<String, String> env, String key, int fallback, int max) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < 1 || value > max) throw new IllegalArgumentException(key + " must be in 1.." + max);
        return value;
    }

    /** 返回时只启动后台线程；MySQL 游标读取和历史补写都不作为 GAME 的就绪条件。 */
    public void start() {
        if (worker != null) worker.start();
    }

    /**
     * 只限制需要产生新日志的客户端请求。数据库失联或刚重启尚不知道 SQL 水位时，分别采用
     * 上次已确认的位置或零做保守预算。到期任务和已确认回执另走原路径，不能因 SQL 落后而遗失。
     */
    public boolean admitsClientWrite(long high) {
        if (worker == null) return true;
        Long known = progress.applied();
        return high - (known == null ? 0L : known) < maxLag;
    }

    private void consume() {
        long lastCheck = 0, lastConfirmed = 0;
        long backoff = 250;
        boolean reload = true;
        while (!closed) {
            long delay = IDLE_POLL_MS;
            try {
                Long applied = progress.applied();
                // 即使区服空闲也要低频核对 SQL 游标，以便发现失联或数据库恢复到了较早的备份。
                if (reload || applied == null || System.nanoTime() - lastCheck >= TimeUnit.SECONDS.toNanos(5)) {
                    if (reload) store.requireRoleIdentitySchema();
                    long checkpoint = store.checkpoint();
                    if (checkpoint > journal.highSequence())
                        throw new ProjectionConflict("MySQL checkpoint is ahead of the original volume");
                    if (checkpoint > 0 && (reload || !Long.valueOf(checkpoint).equals(applied))) {
                        // 进程重启、连接恢复或外部游标变化时，重投最后已确认的一条作交叉校验。
                        // 即使卷 ID 和序号碰巧相同，归档事实不同也必须阻断，不能续接两段历史。
                        var boundary = journal.entry(checkpoint);
                        if (boundary == null) throw new ProjectionConflict("Original journal checkpoint is missing");
                        checkpoint = store.apply(List.of(boundary));
                        if (checkpoint > journal.highSequence())
                            throw new ProjectionConflict("Verified checkpoint exceeds the original volume");
                    }
                    applied = checkpoint;
                    lastCheck = System.nanoTime();
                    lastConfirmed = lastCheck;
                    progress = new Progress("CATCHING_UP", applied, progress.failures(), "", lastCheck);
                    reload = false;
                }
                long high = journal.highSequence();
                if (applied < high) {
                    var entries = journal.entriesAfter(applied, batchSize);
                    long committed;
                    try {
                        committed = store.apply(entries);
                    } catch (ProjectionConflict mismatch) {
                        // 同一原卷的 SQL 备份可能在两次探测间恢复到了更早的连续位置。只在实际
                        // 游标回退时重新读取并验证边界；若位置未回退，就是输入/对象冲突，保留现场。
                        if (store.checkpoint() < applied) {
                            reload = true;
                            continue;
                        }
                        throw mismatch;
                    }
                    high = journal.highSequence();
                    if (committed < entries.getLast().sequence() || committed > high)
                        throw new ProjectionConflict("MySQL acknowledged a checkpoint outside the submitted prefix");
                    applied = committed;
                    lastConfirmed = System.nanoTime();
                }
                boolean caughtUp = applied == high;
                progress = new Progress(caughtUp ? "CAUGHT_UP" : "CATCHING_UP", applied, progress.failures(), "", lastConfirmed);
                // 有积压直接翻下一页；空闲时只在本地检查水位，避免按玩家/命令数去轮询数据库。
                delay = caughtUp ? IDLE_POLL_MS : 0;
                backoff = 250;
            } catch (ProjectionConflict | RealmJournal.InvalidPrefix corrupt) {
                failed("BLOCKED", corrupt.getMessage());
                return; // 错卷、日志空洞或业务键冲突需要人工核对，不能把游标直接跳到下一条。
            } catch (SQLException | IOException unavailable) {
                if (closed) return;
                // COMMIT 可能已生效但确认包丢失。下次先读 SQL 中的事务游标，再决定是否重送。
                // 驱动异常可能含连接信息，诊断仅记录错误类型和 SQLState。
                String detail = unavailable instanceof SQLException sql
                        ? "MySQL unavailable (SQLState=" + sql.getSQLState() + ", code=" + sql.getErrorCode() + ")"
                        : "Realm journal temporarily unavailable";
                failed("RETRYING", detail);
                reload = true;
                delay = backoff;
                backoff = Math.min(5000, backoff * 2);
            } catch (RuntimeException invalid) {
                failed("BLOCKED", "Unexpected projection failure: " + invalid.getClass().getSimpleName());
                return;
            }
            if (delay > 0 && !closed) {
                try { Thread.sleep(delay); }
                catch (InterruptedException stopped) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private void failed(String state, String detail) {
        Progress before = progress;
        progress = new Progress(state, before.applied(), before.failures() + 1, detail, before.confirmedNanos());
        if (!state.equals(before.state()) || !detail.equals(before.diagnostic()))
            LOG.log(System.Logger.Level.WARNING, "Projection " + state + " after " + before.applied() + ": " + detail);
    }

    /** 只有近期收到 SQL 确认的连续位置可以参与清理预演。阻断、失联和过期的数字仅用于诊断。 */
    public JournalRetentionPreview.Consumer retentionConsumer(int realmId, UUID volumeId) {
        var current = progress;
        return new JournalRetentionPreview.Consumer(realmId, volumeId, current.applied(), observedState(current));
    }

    private static String observedState(Progress current) {
        String state = current.state();
        if (("CAUGHT_UP".equals(state) || "CATCHING_UP".equals(state))
                && System.nanoTime() - current.confirmedNanos() > TimeUnit.SECONDS.toNanos(15))
            state = "STALE";
        return state;
    }

    /** lag 单位是区服决定条数。首次成功读取游标前用 null 表示未知，便于运维区分空库与失联。 */
    public Map<String, Object> metrics(long high) {
        var current = progress;
        Long applied = current.applied();
        Long lag = applied == null ? null : Math.max(0, high - applied);
        var result = new LinkedHashMap<String, Object>();
        String state = observedState(current);
        result.put("state", "CAUGHT_UP".equals(state) && lag != null && lag > 0 ? "CATCHING_UP" : state);
        result.put("lastAppliedSeq", applied);
        result.put("lag", lag);
        result.put("maxLag", maxLag);
        result.put("clientWritesLimited", !admitsClientWrite(high));
        result.put("failedAttempts", current.failures());
        result.put("diagnostic", current.diagnostic());
        return result;
    }

    /**
     * 停服先停止消费并释放池，再等待线程退出，最后由调用方关闭原卷。停在事务中也只会留下
     * 已提交的一整批或尚未提交的原游标，下一任 GAME 无需等待关服时把全部积压写完。
     */
    @Override public void close() {
        if (worker == null) return;
        closed = true;
        worker.interrupt();
        // 释放池也会终止在途慢连接。无论 JDBC 是否已发出 COMMIT，下一次启动都从 SQL 游标恢复。
        pool.close();
        boolean interrupted = false;
        while (worker.isAlive()) {
            try { worker.join(); }
            catch (InterruptedException stopping) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}

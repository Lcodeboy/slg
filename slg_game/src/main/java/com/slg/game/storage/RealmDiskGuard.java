package com.slg.game.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 在原卷写满之前给已确认的工作预留空间。只用于判定能否受理产生新日志的客户端写入；
 * 读请求、已提交回执重放、到期任务和战斗结算由调用方走各自的路径。阈值是按剩余字节数
 * 提前减轻新流量的手段，实际预留需要按峰值写入量、原卷压缩和已受理待办的完成成本配置。
 */
public final class RealmDiskGuard implements AutoCloseable {
    static final long SAMPLE_INTERVAL_MS = 5_000;
    static final long STALE_AFTER_MS = 3 * SAMPLE_INTERVAL_MS;
    private static final System.Logger LOG = System.getLogger(RealmDiskGuard.class.getName());
    private static final long GIB = 1L << 30;

    @FunctionalInterface
    interface SpaceProbe {
        long usableBytes() throws IOException;
    }

    /** HARD_LIMIT 为已经触底，RECOVERING 为触底或失去观测后尚未恢复到告警线。 */
    private enum State {
        STARTING, NORMAL, WARNING, HARD_LIMIT, RECOVERING, UNKNOWN, STALE, CLOSED;

        boolean permitsNewWrite() { return this == NORMAL || this == WARNING; }
    }

    private record Sample(State state, long startedNanos, Long usableBytes) {}

    private final long warnBytes;
    private final long hardBytes;
    private final SpaceProbe probe;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("realm-disk-space").factory());
    private volatile Sample latest = new Sample(State.STARTING, 0, null);
    private boolean started;
    private volatile boolean closed;

    RealmDiskGuard(long warnBytes, long hardBytes, SpaceProbe probe, LongSupplier nanoTime) {
        if (hardBytes <= 0 || warnBytes <= hardBytes)
            throw new IllegalArgumentException("Require SLG_REALM_DISK_WARN_BYTES > SLG_REALM_DISK_HARD_BYTES > 0");
        this.warnBytes = warnBytes;
        this.hardBytes = hardBytes;
        this.probe = Objects.requireNonNull(probe);
        this.nanoTime = Objects.requireNonNull(nanoTime);
    }

    public static RealmDiskGuard configure(Path realmDirectory, Map<String, String> env) {
        long warn = threshold(env, "SLG_REALM_DISK_WARN_BYTES", 2 * GIB);
        long hard = threshold(env, "SLG_REALM_DISK_HARD_BYTES", GIB);
        return new RealmDiskGuard(warn, hard, () -> {
            if (!Files.isDirectory(realmDirectory, LinkOption.NOFOLLOW_LINKS) || !Files.isReadable(realmDirectory)
                    || !Files.isWritable(realmDirectory)) throw new IOException("Realm directory is inaccessible");
            // 查询原卷所在的文件系统而非工作目录。getUsableSpace 反映本进程可用的字节，
            // 比文件系统总剩余字节更适合做写入准入；整个探测不创建文件，也不触发 RocksDB 写入。
            var store = Files.getFileStore(realmDirectory);
            if (store.isReadOnly()) throw new IOException("Read-only realm filesystem");
            return store.getUsableSpace();
        }, System::nanoTime);
    }

    private static long threshold(Map<String, String> env, String key, long fallback) {
        String configured = env.get(key);
        if (configured == null) return fallback;
        try { return Long.parseLong(configured); }
        catch (NumberFormatException invalid) { throw new IllegalArgumentException(key + " must be an integer byte count"); }
    }

    /** 观测命令只取一次当下的读数，不启动后台线程，也不代表正在运行的 GAME 曾经是否触底。 */
    public static Map<String, Object> observe(Path realmDirectory, Map<String, String> env) {
        try (var guard = configure(realmDirectory, env)) {
            guard.sample();
            return guard.metrics();
        }
    }

    /** 单线程、上一轮结束后再等待一个周期；即使文件系统调用卡住，也不会排出一串并发探测。 */
    public synchronized void start() {
        if (closed || started) throw new IllegalStateException("Disk guard already started or closed");
        started = true;
        sampler.scheduleWithFixedDelay(this::sample, 0, SAMPLE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** 后台串行调用。记录采样开始时间，避免缓慢返回的旧读数被当成刚确认的可用空间。 */
    void sample() {
        long startedAt = nanoTime.getAsLong();
        State previous = stateAt(latest, startedAt);
        Sample next;
        try {
            long usable = probe.usableBytes();
            if (usable < 0) throw new IOException("Invalid usable space");
            // 采样进行中，上一份读数也可能刚刚过期；不能用开始时的放行状态绕过恢复滞回。
            previous = stateAt(latest, nanoTime.getAsLong());
            State state;
            if (usable <= hardBytes) state = State.HARD_LIMIT;
            else if (usable >= warnBytes) state = State.NORMAL;
            else if (previous == State.STARTING || previous.permitsNewWrite()) state = State.WARNING;
            else state = State.RECOVERING;
            // 初次有效样本落在两线之间只告警；触底或失联后须回到告警线才放行，防止临界点反复启停。
            next = new Sample(state, startedAt, usable);
        } catch (IOException | RuntimeException failure) {
            // 失去读数时立即撤销原准入结论；后续探测仍按正常周期运行。
            next = new Sample(State.UNKNOWN, startedAt, null);
        }
        if (closed) return;
        latest = next;
        if (next.state() != previous && (next.state() != State.NORMAL || previous != State.STARTING)) {
            var level = next.state() == State.NORMAL ? System.Logger.Level.INFO : System.Logger.Level.WARNING;
            LOG.log(level, "Realm disk {0} -> {1}; usableBytes={2}", previous, next.state(), next.usableBytes());
        }
    }

    private State stateAt(Sample sample, long now) {
        if (closed) return State.CLOSED;
        if (sample.state() == State.STARTING || sample.state() == State.UNKNOWN) return sample.state();
        // 守卫线程自身阻塞或进程暂停时依然生效；业务路径用单调时钟判断新鲜度，完全无需访问文件系统。
        if (now - sample.startedNanos() >= TimeUnit.MILLISECONDS.toNanos(STALE_AFTER_MS)) return State.STALE;
        return sample.state();
    }

    /** 在原卷的短提交锁内可调用：只读已发布样本与单调时钟，不能在此处查询文件系统。 */
    public boolean admitsClientWrite() {
        return stateAt(latest, nanoTime.getAsLong()).permitsNewWrite();
    }

    /** 所有字段来自同一个样本。STALE 的 usableBytes 是上次实测值，sampleAgeMillis 表示它的年龄。 */
    public Map<String, Object> metrics() {
        Sample sample = latest;
        long now = nanoTime.getAsLong();
        State state = stateAt(sample, now);
        var result = new LinkedHashMap<String, Object>();
        result.put("state", state.name());
        result.put("usableBytes", sample.usableBytes());
        result.put("warnBytes", warnBytes);
        result.put("hardBytes", hardBytes);
        result.put("sampleIntervalMillis", SAMPLE_INTERVAL_MS);
        result.put("staleAfterMillis", STALE_AFTER_MS);
        result.put("sampleAgeMillis", sample.state() == State.STARTING ? null
                : TimeUnit.NANOSECONDS.toMillis(now - sample.startedNanos()));
        result.put("clientWritesLimited", !state.permitsNewWrite());
        return result;
    }

    /** 先关闭准入再取消探测。即使底层文件系统延迟返回，晚到的结果也无法重新开放写入。 */
    @Override public synchronized void close() {
        closed = true;
        sampler.shutdownNow();
    }
}

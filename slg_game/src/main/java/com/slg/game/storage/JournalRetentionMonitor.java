package com.slg.game.storage;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** 分页核查追加日志。探针只读最近一页的结果，不会在 HTTP 线程扫描整卷，也不会写进度到原卷。 */
public final class JournalRetentionMonitor implements AutoCloseable {
    static final Set<String> REQUIRED_CONSUMERS = Set.of("mysql");
    private final RealmJournal journal;
    private final int realmId;
    private final UUID volumeId;
    private final long keepNewest;
    private final Thread worker;
    private volatile boolean closed;
    private record Sample(RealmJournal.Inspection history, long observedNanos) {}
    private volatile Sample latest;

    public JournalRetentionMonitor(RealmJournal journal, int realmId, UUID volumeId, Map<String, String> env) {
        this.journal = journal;
        this.realmId = realmId;
        this.volumeId = volumeId;
        this.keepNewest = keepNewest(env);
        this.worker = Thread.ofPlatform().daemon().name("realm-" + realmId + "-journal-inspect").unstarted(this::inspect);
    }

    static long keepNewest(Map<String, String> env) {
        long count = Long.parseLong(env.getOrDefault("SLG_REALM_JOURNAL_KEEP_ENTRIES", "10000"));
        if (count < 1) throw new IllegalArgumentException("SLG_REALM_JOURNAL_KEEP_ENTRIES must be positive");
        return count;
    }

    public void start() { worker.start(); }

    private void inspect() {
        long verified = 0;
        while (!closed) {
            long delayMillis = 1000;
            try {
                var page = journal.inspectPage(verified, 128);
                latest = new Sample(page, System.nanoTime());
                if (page.issue() != null) return; // 同一次原卷打开期间保留坏记录现场，不从其后继续。
                verified = page.verifiedThrough();
                if (verified < page.high()) delayMillis = 20;
            } catch (IOException | RuntimeException failure) {
                // I/O 中断不能用旧的完整标记代替新证据。下次从头核验。
                latest = null;
                verified = 0;
            }
            try { Thread.sleep(delayMillis); }
            catch (InterruptedException stopped) { Thread.currentThread().interrupt(); return; }
        }
    }

    public JournalRetentionPreview.Result preview(long liveHigh, JournalRetentionPreview.Consumer mysql) {
        var sample = latest;
        var history = sample == null ? null : sample.history();
        if (history != null && history.issue() == null && (liveHigh != history.high()
                || System.nanoTime() - sample.observedNanos() > TimeUnit.SECONDS.toNanos(15))) {
            // 保留本次真实快照的首末和水位，不把更新的 GAME 水位拼进旧的核验结果。
            history = new RealmJournal.Inspection(history.high(), history.first(), history.last(),
                    history.verifiedThrough(), "Waiting for a current inspection of the journal", history.prunedThrough());
        }
        // 在线 GAME 没有经过恢复演练的备份凭据。仅在离线预演显式核验独立恢复副本后才可带入该证据。
        return JournalRetentionPreview.calculate(realmId, volumeId, history, REQUIRED_CONSUMERS,
                Map.of("mysql", mysql), null, keepNewest);
    }

    /** 原卷本身不可读时，连先前通过检查的数字也不能充当当前水位。 */
    public JournalRetentionPreview.Result unavailable(JournalRetentionPreview.Consumer mysql) {
        return JournalRetentionPreview.calculate(realmId, volumeId, null, REQUIRED_CONSUMERS,
                Map.of("mysql", mysql), null, keepNewest);
    }

    @Override public void close() {
        closed = true;
        worker.interrupt();
        boolean interrupted = false;
        while (worker.isAlive()) {
            try { worker.join(); }
            catch (InterruptedException stopping) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}

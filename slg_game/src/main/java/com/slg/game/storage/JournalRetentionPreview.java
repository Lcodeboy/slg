package com.slg.game.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 只回答本次观测中，哪些仍保留的业务日志构成了可清理的前缀。H 是原卷快照的提交水位，
 * F 是与上次清理同批持久化的已清水位；从未清理的卷 F=0，当前可读范围为 J(F+1)..JH。
 * 本类只接收证据，不持有 RocksDB 句柄。结果不授权删除日志，也不适用于 RocksDB 自身的 WAL、
 * 玩家状态、回执和待办。这几类数据各自需要不同的保留条件。
 */
public final class JournalRetentionPreview {
    private JournalRetentionPreview() {}

    /** 必需消费者确认的连续水位。重试、停用或已阻断时即使带有上次成功的数字，也只能用于诊断。 */
    public record Consumer(int realmId, UUID volumeId, Long appliedSeq, String state) {}

    /**
     * 同卷恢复点的证明，sequence 包含在快照内。只有验证过快照身份、完整性，并在隔离卷上
     * 成功恢复状态、从 sequence+1 接续日志的验证方，才可提供 restoreAndReplayVerified=true。
     * 仅创建了备份文件、看见 MySQL 游标或手填了一个序号，都不能充当此证明。
     */
    public record RecoveryPoint(int realmId, UUID volumeId, long sequence, boolean restoreAndReplayVerified) {}

    /**
     * 两个 eligible 序号均为闭区间；没有候选时同时为 null。bounds 只列出证据充分的上界，
     * limitingFactors 保留所有并列最小项。BLOCKED 时 blockers 解释必须停止的原因，
     * 其余已知上界仅供排查，不能当作备选清理位置。
     */
    public record Result(boolean dryRun, String state, int realmId, UUID volumeId, RealmJournal.Inspection history,
                         long keepNewestEntries, Map<String, Consumer> consumers, RecoveryPoint snapshot,
                         Map<String, Long> bounds, List<String> limitingFactors, List<String> blockers,
                         Long firstEligibleSeq, Long lastEligibleSeq) {}

    /**
     * requiredConsumers 是部署中必须持久消费的完整名单，而不是本次恰好连得上的服务。
     * 对每一项独立取证，再取上界的最小值。任何无法核验的前提都使整个候选范围为空；
     * 即便磁盘紧张，也不能放宽这些条件。F 必须来自同一卷的持久化标记，绝不能看到 J1
     * 缺失就自行推算 F 并把数据损坏解释成正常清理。
     */
    public static Result calculate(int realmId, UUID volumeId, RealmJournal.Inspection history,
                                   Set<String> requiredConsumers, Map<String, Consumer> observations,
                                   RecoveryPoint snapshot, long keepNewestEntries) {
        if (realmId <= 0) throw new IllegalArgumentException("Expected a positive realm ID");
        Objects.requireNonNull(volumeId, "Expected a volume ID");

        var bounds = new LinkedHashMap<String, Long>();
        var blockers = new ArrayList<String>();
        if (keepNewestEntries < 0) blockers.add("INVALID_RETENTION_WINDOW");
        long keep = Math.max(1, keepNewestEntries);
        historyLimits(history, keep, bounds, blockers);
        Long high = history == null || history.high() < 0 ? null : history.high();
        long floor = history == null ? 0 : history.prunedThrough();

        var seen = new TreeMap<String, Consumer>();
        if (requiredConsumers == null || requiredConsumers.isEmpty()) {
            blockers.add("REQUIRED_CONSUMERS_UNKNOWN");
        } else {
            // 本阶段 MySQL 已是持久消费者，即使暂时禁用它也不能把它从必要名单移除。
            if (!requiredConsumers.contains("mysql")) blockers.add("MYSQL_CONSUMER_NOT_REGISTERED");
            var names = new TreeSet<String>();
            for (String name : requiredConsumers) {
                if (name == null || name.isBlank()) blockers.add("REQUIRED_CONSUMER_NAME_UNKNOWN");
                else names.add(name);
            }
            for (String name : names) {
                Consumer cursor = observations == null ? null : observations.get(name);
                seen.put(name, cursor);
                consumerLimit(name, cursor, realmId, volumeId, high, floor, bounds, blockers);
            }
        }
        recoveryLimit(snapshot, realmId, volumeId, high, floor, bounds, blockers);

        Long first = null, last = null;
        String state = "BLOCKED";
        var factors = new ArrayList<String>();
        if (blockers.isEmpty()) {
            long through = Collections.min(bounds.values());
            bounds.forEach((key, value) -> { if (value == through) factors.add(key); });
            if (through <= floor) {
                state = "NO_CANDIDATES";
            } else {
                state = "CANDIDATE";
                first = floor + 1;
                last = through;
            }
        }
        return new Result(true, state, realmId, volumeId, history, keep, Collections.unmodifiableMap(seen),
                snapshot, Collections.unmodifiableMap(bounds), List.copyOf(factors), List.copyOf(blockers), first, last);
    }

    private static void historyLimits(RealmJournal.Inspection history, long keep, Map<String, Long> bounds,
                                      List<String> blockers) {
        if (history == null || history.high() < 0) {
            blockers.add("JOURNAL_UNKNOWN");
            return;
        }
        long high = history.high();
        long floor = history.prunedThrough();
        int priorBlockers = blockers.size();
        // recover() 会逐项检查水位和物理末条是否匹配；即便保留窗口配成 0，也至少留下末条。
        bounds.put("journalTail", Math.max(0, high - 1));
        bounds.put("retentionWindow", Math.max(0, high - keep));
        if (history.issue() != null) blockers.add("JOURNAL_INSPECTION_FAILED");
        if (history.verifiedThrough() != high) blockers.add("JOURNAL_PREFIX_UNVERIFIED");
        if (high == 0 ? floor != 0 || history.first() != null || history.last() != null
                : floor < 0 || floor >= high || !Long.valueOf(floor + 1).equals(history.first())
                        || !Long.valueOf(high).equals(history.last())) {
            blockers.add("JOURNAL_RANGE_MISMATCH");
        }
        // 首条须精确等于 F+1，每条保留日志须连续核对到 H。遇到空洞、损坏或尚未核对完时
        // 停在当前已清位置，不把缺口前一条误当作候选上界。
        if (blockers.size() == priorBlockers) bounds.put("verifiedPrefix", high);
    }

    private static void consumerLimit(String name, Consumer cursor, int realmId, UUID volumeId, Long high, long floor,
                                      Map<String, Long> bounds, List<String> blockers) {
        if (cursor == null) {
            blockers.add("CONSUMER_MISSING:" + name);
        } else if (cursor.realmId() != realmId || !volumeId.equals(cursor.volumeId())) {
            blockers.add("CONSUMER_IDENTITY_MISMATCH:" + name);
        } else if (!"CAUGHT_UP".equals(cursor.state()) && !"CATCHING_UP".equals(cursor.state())) {
            blockers.add("CONSUMER_UNCONFIRMED:" + name);
        } else if (cursor.appliedSeq() == null || cursor.appliedSeq() < 0
                || (high != null && cursor.appliedSeq() > high)) {
            blockers.add("CONSUMER_CURSOR_INVALID:" + name);
        } else if (floor > 0 && cursor.appliedSeq() <= floor) {
            // 消费者恢复了过早的备份或变成空库，原卷已无法单独提供所需的 Jc / J1。
            blockers.add("CONSUMER_CURSOR_PRUNED:" + name);
        } else {
            // c 的业务效果已经提交，但投影器重连时必须重送 Jc，与 MySQL 的归档指纹做交叉核验。
            // 删除到 c 会使下一次重启无从验证历史。c=0/1 时，当前格式连 J1 也不能列入候选。
            bounds.put("consumer:" + name, Math.max(0, cursor.appliedSeq() - 1));
        }
    }

    private static void recoveryLimit(RecoveryPoint point, int realmId, UUID volumeId, Long high, long floor,
                                      Map<String, Long> bounds, List<String> blockers) {
        if (point == null) {
            blockers.add("VERIFIED_SNAPSHOT_MISSING");
        } else if (point.realmId() != realmId || !volumeId.equals(point.volumeId())) {
            blockers.add("SNAPSHOT_IDENTITY_MISMATCH");
        } else if (!point.restoreAndReplayVerified()) {
            blockers.add("SNAPSHOT_RESTORE_UNVERIFIED");
        } else if (point.sequence() < 0 || (high != null && point.sequence() > high)) {
            blockers.add("SNAPSHOT_SEQUENCE_INVALID");
        } else if (point.sequence() < floor) {
            // 回放必须从 S+1 开始。如果 S<F，这个起点落在已清区间，再旧的凭据也不能授权新一批。
            blockers.add("SNAPSHOT_BEFORE_PRUNED_PREFIX");
        } else {
            // 已验证快照覆盖 [1,s]，并已证明能以 s+1 为起点恢复。s 是闭区间的上界；
            // 仍需取消费者、保留窗口等其他上界的最小值，防止删除其他恢复路径要读的 Jc。
            bounds.put("recoverySnapshot", point.sequence());
        }
    }
}

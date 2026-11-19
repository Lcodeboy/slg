package com.slg.game;

import com.slg.game.application.background.BackgroundPumpLimits;
import com.slg.game.application.commit.CommitQueueLimits;
import com.slg.game.application.execution.OperationLimits;
import com.slg.game.application.player.PlayerCacheLimits;
import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.read.ReadExecutionLimits;
import com.slg.game.infrastructure.mysql.runtime.AuthorityDatabaseSettings;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * 新权威运行路径的显式预算，启动前一次解析。容量是保守默认值，不是吞吐验收承诺；各层
 * 独立限额仍生效。配置不携带 JDBC 凭据，错误仅报告参数名，不把原始环境值写入日志。
 */
public record RealmRuntimeSettings(int deadlineMillis, int sessionLimit, Duration leaseDuration,
        Duration probeInterval, Duration probeFreshness, Duration closeTimeout, String workerTarget,
        ReadBudget playerRead, ReadBudget backgroundRead, ReadBudget mapSnapshotRead,
        ReadBudget mapEventRead, ReadBudget mapAccessRead, ReadExecutionLimits reads,
        CommitQueueLimits commits, OperationLimits operations, PlayerCacheLimits cache,
        BackgroundPumpLimits pump) {
    private static final long MIB = 1024L * 1024;

    public RealmRuntimeSettings {
        Objects.requireNonNull(workerTarget);
        if (deadlineMillis < 1 || sessionLimit < 1 || workerTarget.isBlank())
            throw new IllegalArgumentException("Invalid realm runtime settings");
        if (probeInterval.compareTo(probeFreshness) >= 0 || probeFreshness.compareTo(leaseDuration) >= 0
                || commits.renewInterval().multipliedBy(2).compareTo(leaseDuration) >= 0)
            throw new IllegalArgumentException("Probe freshness and renewal must precede owner lease expiry");
        if (operations.systemReservedOperations() < 2 || reads.maxOutstanding() < 3)
            throw new IllegalArgumentException("Background handoff needs system and cold-read reserves");
    }

    public static RealmRuntimeSettings fromEnvironment(Map<String, String> env, AuthorityDatabaseSettings database) {
        Objects.requireNonNull(env); Objects.requireNonNull(database);
        if (database.controlPoolSize() < 2)
            throw new IllegalArgumentException("Control pool needs separate owner and probe connection capacity");
        int deadline = number(env, "SLG_GAME_DEADLINE_MS", 3000, 1, 120_000);
        var wait = Duration.ofMillis(deadline);
        var close = millis(env, "SLG_GAME_CLOSE_TIMEOUT_MS", 15_000, 100, 300_000);
        var lease = millis(env, "SLG_GAME_OWNER_LEASE_MS", 30_000, 5000, 300_000);
        var renew = millis(env, "SLG_GAME_OWNER_RENEW_MS", 5000, 100, 60_000);
        var interval = millis(env, "SLG_GAME_DB_PROBE_INTERVAL_MS", 1000, 100, 30_000);
        var fresh = millis(env, "SLG_GAME_DB_PROBE_FRESH_MS", 3000, 200, 60_000);
        int readWorkers = number(env, "SLG_GAME_READ_WORKERS", Math.min(4, database.readPoolSize()), 1, database.readPoolSize());
        int writeWorkers = number(env, "SLG_GAME_COMMIT_WORKERS", Math.min(4, database.txPoolSize()), 1, database.txPoolSize());
        int readCapacity = number(env, "SLG_GAME_READ_CAPACITY", 128, 3, 65_536);
        int operationCapacity = number(env, "SLG_GAME_OPERATION_CAPACITY", 256, 8, 65_536);
        int commitCapacity = number(env, "SLG_GAME_COMMIT_CAPACITY", 256, 8, 65_536);
        var playerRead = new ReadBudget(number(env, "SLG_GAME_PLAYER_READ_ROWS", 10_000, 8, 1_000_000),
                mebibytes(env, "SLG_GAME_PLAYER_READ_MIB", 8, 1, 64), Duration.ofSeconds(5));
        var workRead = new ReadBudget(number(env, "SLG_GAME_BACKGROUND_READ_ROWS", 20_000, 8, 1_000_000),
                mebibytes(env, "SLG_GAME_BACKGROUND_READ_MIB", 16, 2, 64), Duration.ofSeconds(5));
        var reads = new ReadExecutionLimits(readWorkers, readCapacity, Math.multiplyExact((long) readCapacity, 1024),
                2, readCapacity, close);
        var commits = new CommitQueueLimits(commitCapacity, mebibytes(env, "SLG_GAME_COMMIT_MIB", 64, 8, 4096),
                Math.max(2, commitCapacity / 8), mebibytes(env, "SLG_GAME_SYSTEM_COMMIT_MIB", 16, 1, 2048),
                writeWorkers, 2, wait, Duration.ofMillis(100), renew);
        var operations = new OperationLimits(operationCapacity, mebibytes(env, "SLG_GAME_OPERATION_MIB", 128, 16, 8192),
                128, Math.min(4, readWorkers), 2, 2, Math.max(2, operationCapacity / 8),
                mebibytes(env, "SLG_GAME_SYSTEM_OPERATION_MIB", 32, 8, 4096), wait);
        var cache = new PlayerCacheLimits(number(env, "SLG_GAME_CACHE_ENTRIES", 16_384, 1, 1_000_000),
                Math.multiplyExact(operationCapacity, 2), mebibytes(env, "SLG_GAME_CACHE_MIB", 256, 8, 16_384),
                mebibytes(env, "SLG_GAME_PLAYER_SNAPSHOT_MIB", 2, 1, 64),
                Duration.ofSeconds(number(env, "SLG_GAME_PLAYER_IDLE_SECONDS", 300, 1, 86_400)));
        var pump = new BackgroundPumpLimits(number(env, "SLG_GAME_BACKGROUND_TASKS", 32, 1, 1024),
                number(env, "SLG_GAME_BACKGROUND_BATTLES", 16, 1, 64),
                number(env, "SLG_GAME_BACKGROUND_PAGE_SIZE", 64, 1, 1024),
                mebibytes(env, "SLG_GAME_BACKGROUND_MIB", 160, 16, 4096),
                number(env, "SLG_GAME_BACKGROUND_RETRIES", 4096, 2, 100_000),
                millis(env, "SLG_GAME_BACKGROUND_INTERVAL_MS", 100, 10, 10_000),
                Duration.ofMillis(200), Duration.ofSeconds(10), wait, close);
        return new RealmRuntimeSettings(deadline, number(env, "SLG_GAME_SESSION_LIMIT", 20_000, 1, 1_000_000),
                lease, interval, fresh, close, env.getOrDefault("SLG_BATTLE_RPC_TARGET", "127.0.0.1:9083"),
                playerRead, workRead,
                new ReadBudget(number(env, "SLG_GAME_MAP_SNAPSHOT_ROWS", 1_000_000, 8, 10_000_000),
                        mebibytes(env, "SLG_GAME_MAP_SNAPSHOT_MIB", 256, 1, 4096), Duration.ofSeconds(60)),
                new ReadBudget(10_000, mebibytes(env, "SLG_GAME_MAP_EVENT_MIB", 16, 1, 128), Duration.ofSeconds(5)),
                new ReadBudget(8, 4096, Duration.ofSeconds(3)), reads, commits, operations, cache, pump);
    }

    private static Duration millis(Map<String, String> env, String key, int value, int min, int max) {
        return Duration.ofMillis(number(env, key, value, min, max));
    }
    private static long mebibytes(Map<String, String> env, String key, int value, int min, int max) {
        return Math.multiplyExact((long) number(env, key, value, min, max), MIB);
    }
    private static int number(Map<String, String> env, String key, int value, int min, int max) {
        final int parsed;
        try { parsed = Integer.parseInt(env.getOrDefault(key, Integer.toString(value))); }
        catch (RuntimeException malformed) { throw new IllegalArgumentException("Invalid " + key); }
        if (parsed < min || parsed > max) throw new IllegalArgumentException("Invalid " + key);
        return parsed;
    }
}

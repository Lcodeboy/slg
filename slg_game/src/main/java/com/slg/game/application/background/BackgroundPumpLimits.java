package com.slg.game.application.background;

import java.time.Duration;
import java.util.Objects;

/** 发现与派发预算；固定槽位字节覆盖完整输入、解码结果和等待结算的战果，不等同精确 JVM 堆大小。 */
public record BackgroundPumpLimits(int taskCapacity, int battleCapacity, int pageSize, long maxRetainedBytes,
                                   int maxRetryEntries, Duration scanInterval, Duration retryBase,
                                   Duration retryMax, Duration operationWait, Duration closeTimeout) {
    // reason最多2048个UTF-16字符，JSON控制字符最坏6倍转义，再加三个256字节身份与元数据。
    // 16KiB覆盖合法任务的编码载荷；操作执行器仍独立计量自己的命令/决定，不承诺精确堆大小。
    public static final long TASK_SLOT_BYTES = 16L * 1024;
    public static final long BATTLE_SLOT_BYTES = 8L * 1024 * 1024;

    public BackgroundPumpLimits {
        if (taskCapacity < 1 || taskCapacity > 1024 || battleCapacity < 1 || battleCapacity > 64
                || pageSize < 1 || pageSize > 1024 || maxRetainedBytes <= BATTLE_SLOT_BYTES
                || maxRetryEntries < 2 || maxRetryEntries > 100_000)
            throw new IllegalArgumentException("Invalid background pump limits");
        positive(scanInterval); positive(retryBase); positive(retryMax); positive(operationWait); positive(closeTimeout);
        if (retryMax.compareTo(retryBase) < 0) throw new IllegalArgumentException("Retry maximum precedes base delay");
    }

    private static void positive(Duration duration) {
        Objects.requireNonNull(duration);
        if (duration.isZero() || duration.isNegative()) throw new IllegalArgumentException("Positive background duration required");
        try { duration.toNanos(); } catch (ArithmeticException overflow) { throw new IllegalArgumentException("Background duration too large"); }
    }
}

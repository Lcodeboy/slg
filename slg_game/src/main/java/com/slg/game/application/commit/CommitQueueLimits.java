package com.slg.game.application.commit;

import java.time.Duration;
import java.util.Objects;

/**
 * 准入预算覆盖排队、执行、核对和结果通知尚未返回的全部任务，不把移出队列视为释放内存。
 * 系统保留数/字节禁止 CLIENT 使用；全部来源仍共享总上限并按来源轮转。
 */
public record CommitQueueLimits(int maxTasks, long maxBytes, int systemReservedTasks, long systemReservedBytes,
                                int databaseWorkers, int notificationWorkers, Duration maxQueueWait,
                                Duration controlInterval, Duration renewInterval) {
    public CommitQueueLimits {
        if (maxTasks <= 0 || maxBytes <= 0 || systemReservedTasks < 0 || systemReservedTasks >= maxTasks
                || systemReservedBytes < 0 || systemReservedBytes >= maxBytes
                || databaseWorkers <= 0 || databaseWorkers > maxTasks
                || notificationWorkers <= 0 || notificationWorkers > maxTasks)
            throw new IllegalArgumentException("Invalid bounded commit capacity");
        positive(maxQueueWait, "maxQueueWait"); positive(controlInterval, "controlInterval"); positive(renewInterval, "renewInterval");
    }
    private static void positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.isZero()) throw new IllegalArgumentException(name + " must be positive");
        try { value.toNanos(); } catch (ArithmeticException overflow) { throw new IllegalArgumentException(name + " is too large"); }
    }
}

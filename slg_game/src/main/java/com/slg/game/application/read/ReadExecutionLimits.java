package com.slg.game.application.read;

import java.time.Duration;
import java.util.Objects;

/** 冷读执行与结果交付各自有界；每个已受理任务预留一个通知位置，慢回调不能反压 JDBC 线程。 */
public record ReadExecutionLimits(int workers, int maxOutstanding, long maxMetadataBytes,
                                  int notificationWorkers, int notificationQueueCapacity,
                                  Duration closeTimeout) {
    public ReadExecutionLimits {
        if (workers <= 0 || maxOutstanding <= 0 || maxMetadataBytes <= 0
                || notificationWorkers <= 0 || notificationQueueCapacity < 0)
            throw new IllegalArgumentException("Read execution limits must be positive; notification queue may be zero");
        if ((long) notificationWorkers + notificationQueueCapacity < maxOutstanding)
            throw new IllegalArgumentException("Every admitted read must reserve a notification worker or queue position");
        Objects.requireNonNull(closeTimeout, "closeTimeout");
        if (closeTimeout.isNegative() || closeTimeout.isZero())
            throw new IllegalArgumentException("Close timeout must be positive");
        closeTimeout.toNanos();
    }
}

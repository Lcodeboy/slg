package com.slg.game.application.execution;

import java.time.Duration;
import java.util.Objects;

/** 数量与字节覆盖等待、准备、提交、发布、故障修复及未交付通知；CLIENT 不能消耗系统保留额度。 */
public record OperationLimits(int maxOperations, long maxRetainedBytes, int maxKeysPerOperation,
                              int preparationThreads, int publicationThreads, int notificationThreads,
                              int systemReservedOperations, long systemReservedBytes, Duration maxWait) {
    public OperationLimits {
        if (maxOperations <= 0 || maxRetainedBytes <= 0 || maxKeysPerOperation <= 0
                || preparationThreads <= 0 || publicationThreads <= 0 || notificationThreads <= 0
                || preparationThreads > maxOperations || publicationThreads > maxOperations || notificationThreads > maxOperations
                || systemReservedOperations < 0 || systemReservedOperations >= maxOperations
                || systemReservedBytes < 0 || systemReservedBytes >= maxRetainedBytes)
            throw new IllegalArgumentException("Invalid operation limits");
        Objects.requireNonNull(maxWait, "maxWait");
        if (maxWait.isZero() || maxWait.isNegative()) throw new IllegalArgumentException("Maximum wait must be positive");
        try { maxWait.toNanos(); } catch (ArithmeticException overflow) { throw new IllegalArgumentException("Maximum wait is too large"); }
    }
}

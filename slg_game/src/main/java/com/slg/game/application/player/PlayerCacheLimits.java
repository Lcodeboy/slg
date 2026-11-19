package com.slg.game.application.player;

import java.time.Duration;
import java.util.Objects;

/** 条目/租用次数独立有界；字节预算覆盖驻留快照、目录元数据和每项未结束加载的最大预留。 */
public record PlayerCacheLimits(int maxEntries, int maxPins, long maxBytes, long maxSnapshotBytes,
                                Duration idleTtl) {
    public PlayerCacheLimits {
        if (maxEntries <= 0 || maxPins <= 0 || maxBytes <= 0 || maxSnapshotBytes <= 0 || maxSnapshotBytes > maxBytes)
            throw new IllegalArgumentException("Player cache capacities must be positive and snapshot must fit total bytes");
        Objects.requireNonNull(idleTtl, "idleTtl");
        if (idleTtl.isZero() || idleTtl.isNegative()) throw new IllegalArgumentException("Idle TTL must be positive");
        try { idleTtl.toNanos(); } catch (ArithmeticException overflow) { throw new IllegalArgumentException("Idle TTL too large"); }
    }
}

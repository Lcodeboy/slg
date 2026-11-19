package com.slg.game.application.port;

import java.time.Duration;
import java.util.Objects;

/**
 * 一次读取或一份快照的行数、编码字节和总耗时上限，三项均须为正。
 * 时间预算从适配器开始执行读取起按单调时钟计算；快照内多次读取累计使用同一预算。
 * 超限应明确失败，不能把截断的扫描伪装为完整结果。页接口可以在完整记录边界返回
 * 较小的有效页，但连一条完整记录也容纳不了时必须失败。
 */
public record ReadBudget(int maxRows, long maxBytes, Duration timeout) {
    public ReadBudget {
        if (maxRows <= 0) throw new IllegalArgumentException("maxRows must be positive");
        if (maxBytes <= 0) throw new IllegalArgumentException("maxBytes must be positive");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative())
            throw new IllegalArgumentException("timeout must be positive");
    }
}

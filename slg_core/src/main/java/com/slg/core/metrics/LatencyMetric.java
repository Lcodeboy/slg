package com.slg.core.metrics;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * 固定桶的进程累计耗时，不保存逐请求样本，也不按玩家或请求 ID 创建标签。
 * 高负载时每次只更新有限计数器；失败同样入桶，避免只统计成功请求而掩盖超时和故障。
 * 代码段耗时来自单调时钟差值，单位为纳秒；业务到期延迟另按业务时钟计算并在指标处说明，
 * 不把可能被校时的业务延迟混称为代码执行耗时。
 */
public final class LatencyMetric {
    private static final long[] BOUNDS = {100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000,
            10_000_000, 25_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000,
            1_000_000_000, 2_500_000_000L, 5_000_000_000L, 10_000_000_000L, Long.MAX_VALUE};
    private final AtomicLongArray buckets = new AtomicLongArray(BOUNDS.length);
    private final LongAdder count = new LongAdder(), failures = new LongAdder(), sum = new LongAdder();
    private final AtomicLong maximum = new AtomicLong();

    public void record(long durationNanos, boolean success) {
        // 指标不能因为测试时钟回退等问题破坏已经完成的业务提交；负差值按零记录。
        long nanos = Math.max(0, durationNanos);
        int bucket = Arrays.binarySearch(BOUNDS, nanos);
        if (bucket < 0) bucket = -bucket - 1;
        buckets.incrementAndGet(bucket);
        sum.add(nanos);
        maximum.accumulateAndGet(nanos, Math::max);
        if (!success) failures.increment();
        count.increment();
    }

    /**
     * 桶为互斥区间 (前界, 当前上界]，不是 Prometheus 的累计 le 桶。
     * 返回全新的数组，不暴露内部状态。并发采样为弱一致，业务线程不会为采集持全局锁；
     * 消费者应检查 count 与桶和，跳过不一致样本，并按 processId 隔离进程重启。
     * 区间 P99 应由两次有效桶差量估算，不能相减两个累计 P99 或累计 max。
     */
    public Map<String, Object> snapshot() {
        long[] values = new long[BOUNDS.length];
        for (int i = 0; i < values.length; i++) values[i] = buckets.get(i);
        return Map.of("count", count.sum(), "failures", failures.sum(), "sumNanos", sum.sum(),
                "maxNanos", maximum.get(), "boundsNanos", BOUNDS.clone(), "buckets", values);
    }
}

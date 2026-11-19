package com.slg.game;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 低频采集进程资源。health 只读取最近一次不可变快照，不能为了查看健康而争抢提交锁、
 * 借 MySQL 连接或扫描对象。资源采样由单个线程每五秒执行，绝不逐玩家建监控项。
 * 本类先于连接池关闭；只读池内存指标，不执行健康探测 SQL。
 */
final class ProcessObservability implements AutoCloseable {
    @FunctionalInterface interface PoolMetrics { Map<String, Object> read() throws Exception; }
    private record Sample(Map<String, Object> values, long completedNanos) { }
    private final String processId = UUID.randomUUID().toString();
    private final long startedAtEpochMillis, startedNanos, authorityOpenRecoveryNanos;
    private final PoolMetrics poolMetrics;
    private final ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("realm-resource-metrics").factory());
    private volatile long readyAfterNanos = -1;
    private volatile Sample sample = new Sample(Map.of("status", "PENDING", "sampledAtEpochMillis", 0L), 0);
    private long failures;

    /** 只采集权威库连接池内存状态，启动恢复耗时由组合根传入。 */
    static ProcessObservability authority(PoolMetrics poolMetrics, long startedAtEpochMillis,
                                          long startedNanos, long authorityOpenRecoveryNanos) {
        return new ProcessObservability(poolMetrics, startedAtEpochMillis, startedNanos, authorityOpenRecoveryNanos);
    }

    private ProcessObservability(PoolMetrics poolMetrics, long startedAtEpochMillis, long startedNanos,
                                 long authorityOpenRecoveryNanos) {
        this.poolMetrics = java.util.Objects.requireNonNull(poolMetrics);
        this.startedAtEpochMillis = startedAtEpochMillis;
        this.startedNanos = startedNanos;
        this.authorityOpenRecoveryNanos = authorityOpenRecoveryNanos;
        sampler.scheduleWithFixedDelay(this::sampleNow, 0, 5, TimeUnit.SECONDS);
    }

    /** 仅说明业务入口可受理；数据库连接建立、业务恢复与 AOI 重建分别观测。 */
    void markReady() { readyAfterNanos = System.nanoTime() - startedNanos; }

    private void sampleNow() {
        long began = System.nanoTime();
        try {
            var jvm = jvmMetrics();
            var storage = poolMetrics.read();
            sample = new Sample(Map.of("status", "OK", "sampledAtEpochMillis", System.currentTimeMillis(),
                    "durationNanos", System.nanoTime() - began, "failures", failures,
                    "jvm", jvm, "mysql", storage), System.nanoTime());
        } catch (Exception unavailable) {
            // 不把采集失败改成零内存/零积压，也不让一次监控异常终止固定周期任务。
            // 仅输出异常类型，避免配置、目录或连接口令从异常文本泄露到 health。
            sample = new Sample(Map.of("status", "UNAVAILABLE", "sampledAtEpochMillis", System.currentTimeMillis(),
                    "durationNanos", System.nanoTime() - began, "failures", ++failures,
                    "errorType", unavailable.getClass().getSimpleName()), System.nanoTime());
        }
    }

    private static Map<String, Object> memory(MemoryUsage value) {
        return Map.of("usedBytes", value.getUsed(), "committedBytes", value.getCommitted(), "maxBytes", value.getMax());
    }

    private static Map<String, Object> jvmMetrics() {
        var memory = ManagementFactory.getMemoryMXBean();
        var gc = new LinkedHashMap<String, Object>();
        for (var collector : ManagementFactory.getGarbageCollectorMXBeans())
            gc.put(collector.getName(), Map.of("collections", collector.getCollectionCount(),
                    "timeMillis", collector.getCollectionTime()));
        var buffers = new LinkedHashMap<String, Object>();
        for (var pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
            buffers.put(pool.getName(), Map.of("count", pool.getCount(), "usedBytes", pool.getMemoryUsed(),
                    "capacityBytes", pool.getTotalCapacity()));
        var os = ManagementFactory.getOperatingSystemMXBean();
        var result = new LinkedHashMap<String, Object>();
        result.put("heap", memory(memory.getHeapMemoryUsage()));
        result.put("nonHeap", memory(memory.getNonHeapMemoryUsage()));
        result.put("gc", Map.copyOf(gc));
        result.put("buffers", Map.copyOf(buffers));
        result.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
        result.put("availableProcessors", os.getAvailableProcessors());
        result.put("uptimeMillis", ManagementFactory.getRuntimeMXBean().getUptime());
        result.put("javaVersion", System.getProperty("java.version"));
        // JVM 堆、direct buffer 等视角有重叠且不含全部 native 内存，不能相加冒充进程 RSS。
        if (os instanceof com.sun.management.OperatingSystemMXBean extended)
            result.put("processCpuNanos", extended.getProcessCpuTime());
        return Map.copyOf(result);
    }

    Map<String, Object> metrics() {
        var current = sample;
        return Map.of("processId", processId, "startedAtEpochMillis", startedAtEpochMillis,
                "readyAfterNanos", readyAfterNanos, "authorityOpenRecoveryNanos", authorityOpenRecoveryNanos,
                "sample", current.values(), "sampleStale", current.completedNanos() == 0
                        || System.nanoTime() - current.completedNanos() > TimeUnit.SECONDS.toNanos(15));
    }

    @Override public void close() {
        sampler.shutdownNow();
        boolean interrupted = false;
        for (;;) {
            try { if (sampler.awaitTermination(1, TimeUnit.SECONDS)) break; }
            catch (InterruptedException stopped) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}

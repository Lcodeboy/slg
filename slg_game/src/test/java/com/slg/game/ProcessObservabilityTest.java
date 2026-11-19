package com.slg.game;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class ProcessObservabilityTest {
    @Test void slowPoolSamplingDoesNotBlockHealthAndCloseWaitsForSampler() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var pool = Executors.newSingleThreadExecutor()) {
            var metrics = ProcessObservability.authority(() -> {
                entered.countDown();
                // 模拟尚未返回的连接池采样；close 必须等待真实采样者退出，不能伪装排空。
                boolean done = false;
                while (!done) try { release.await(); done = true; } catch (InterruptedException ignored) { }
                return Map.of("state", "AVAILABLE");
            }, 123, System.nanoTime(), 456);
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertEquals("PENDING", sample(metrics).get("status"));
                assertEquals(456L, metrics.metrics().get("authorityOpenRecoveryNanos"));
                metrics.markReady();
                assertTrue((long) metrics.metrics().get("readyAfterNanos") >= 0);
                var closing = pool.submit(metrics::close);
                assertThrows(TimeoutException.class, () -> closing.get(50, TimeUnit.MILLISECONDS));
                release.countDown();
                closing.get(2, TimeUnit.SECONDS);
                assertEquals("OK", sample(metrics).get("status"));
                assertFalse((boolean) metrics.metrics().get("sampleStale"));
            } finally { release.countDown(); metrics.close(); }
        }
    }

    @Test void failedSampleIsExplicitAndDoesNotInventZeroResources() throws Exception {
        try (var metrics = ProcessObservability.authority(() -> { throw new IOException("private path or credential"); },
                123, System.nanoTime(), 456)) {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (sample(metrics).get("status").equals("PENDING") && System.nanoTime() < until) Thread.sleep(5);
            var value = sample(metrics);
            assertEquals("UNAVAILABLE", value.get("status"));
            assertEquals(1L, value.get("failures"));
            assertEquals("IOException", value.get("errorType"));
            assertFalse(value.containsKey("jvm"));
            assertFalse(value.containsKey("mysql"));
            assertFalse(value.toString().contains("credential"));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sample(ProcessObservability metrics) {
        return (Map<String, Object>) metrics.metrics().get("sample");
    }
}

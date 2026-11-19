package com.slg.core.metrics;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class LatencyMetricTest {
    @Test void boundaryFailuresAndSnapshotOwnership() {
        var metric = new LatencyMetric();
        metric.record(100_000, true);
        metric.record(100_001, false);
        metric.record(-1, true);
        var snapshot = metric.snapshot();
        assertEquals(3L, snapshot.get("count"));
        assertEquals(1L, snapshot.get("failures"));
        assertEquals(200_001L, snapshot.get("sumNanos"));
        assertEquals(100_001L, snapshot.get("maxNanos"));
        assertEquals(2, ((long[]) snapshot.get("buckets"))[0]);
        ((long[]) snapshot.get("buckets"))[0] = 99;
        ((long[]) snapshot.get("boundsNanos"))[0] = 0;
        assertEquals(2, ((long[]) metric.snapshot().get("buckets"))[0]);
        assertEquals(100_000, ((long[]) metric.snapshot().get("boundsNanos"))[0]);
    }

    @Test void concurrentRecordingIsCompleteAfterWritersFinish() throws Exception {
        var metric = new LatencyMetric();
        try (var pool = Executors.newFixedThreadPool(4)) {
            for (int thread = 0; thread < 4; thread++) pool.submit(() -> {
                for (int i = 0; i < 10_000; i++) metric.record(i, i % 2 == 0);
            });
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
        var snapshot = metric.snapshot();
        assertEquals(40_000L, snapshot.get("count"));
        assertEquals(20_000L, snapshot.get("failures"));
        assertEquals(40_000L, Arrays.stream((long[]) snapshot.get("buckets")).sum());
        assertEquals(199_980_000L, snapshot.get("sumNanos"));
    }
}

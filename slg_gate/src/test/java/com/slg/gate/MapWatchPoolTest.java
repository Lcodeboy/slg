package com.slg.gate;

import com.slg.rpc.RpcTransport;
import io.grpc.Channel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** 只验证传输槽的并发守恒；gRPC 终态何时归还槽由地图客户端的传输测试验证。 */
@Timeout(10)
class MapWatchPoolTest {
    @Test void physicalCapacityCapsRequestedLimitAndReleasedSlotCanBeReused() {
        var first = new ControlledGrpcChannel();
        var second = new ControlledGrpcChannel();
        var pool = new MapWatchPool(List.of(first, second), 1, 100);
        assertEquals(2, pool.limit());
        var one = pool.acquire();
        var two = pool.acquire();
        assertSame(first, one.channel());
        assertSame(second, two.channel());
        assertThrows(RejectedExecutionException.class, pool::acquire);
        assertEquals(1L, pool.metrics().get("watchPoolRejected"));
        assertEquals(List.of(1, 1), pool.metrics().get("watchSlotsByChannel"));

        one.close();
        var replacement = pool.acquire();
        assertSame(first, replacement.channel());
        assertSame(second, two.channel(), "释放另一条流不能迁移仍在运行的订阅");
        one.close(); // 已归还的旧 lease 不能释放刚刚复用的槽。
        assertThrows(RejectedExecutionException.class, pool::acquire);
        assertEquals(2, pool.metrics().get("watchSlots"));
        replacement.close();
        two.close();
        assertEquals(0, pool.metrics().get("watchSlots"));
    }

    @Test void smallerRequestedLimitBoundsTheWholePool() {
        var pool = new MapWatchPool(channels(3), 2, 3);
        var held = new ArrayList<MapWatchPool.Lease>();
        for (int i = 0; i < 3; i++) held.add(pool.acquire());
        assertEquals(3, pool.limit());
        assertEquals(6, pool.metrics().get("watchTransportCapacity"));
        assertEquals(List.of(1, 1, 1), pool.metrics().get("watchSlotsByChannel"));
        assertThrows(RejectedExecutionException.class, pool::acquire);
        held.forEach(MapWatchPool.Lease::close);
    }

    @Test void unevenCancellationFillsTheLightestAvailableConnections() {
        var channels = channels(3);
        var pool = new MapWatchPool(channels, 3, 9);
        var held = new ArrayList<MapWatchPool.Lease>();
        for (int i = 0; i < 9; i++) held.add(pool.acquire());
        for (var lease : held) {
            if (lease.channel() == channels.getFirst()) lease.close();
        }
        held.get(1).close();
        held.get(4).close();
        assertEquals(List.of(0, 1, 3), pool.metrics().get("watchSlotsByChannel"));

        var first = pool.acquire();
        var second = pool.acquire();
        var third = pool.acquire();
        assertSame(channels.get(0), first.channel());
        assertSame(channels.get(1), second.channel(), "平局轮转，不把所有新流集中到同一条连接");
        assertSame(channels.get(0), third.channel());
        assertEquals(List.of(2, 2, 3), pool.metrics().get("watchSlotsByChannel"));
        first.close(); second.close(); third.close();
        held.forEach(MapWatchPool.Lease::close);
        assertEquals(List.of(0, 0, 0), pool.metrics().get("watchSlotsByChannel"));
    }

    @Test void concurrentAdmissionAndDuplicateReleaseKeepBothLimits() throws Exception {
        var pool = new MapWatchPool(channels(3), 2, 6);
        var ready = new CountDownLatch(8);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(8);
        var accepted = new ArrayList<MapWatchPool.Lease>();
        try {
            var attempts = new ArrayList<Future<MapWatchPool.Lease>>();
            for (int i = 0; i < 64; i++) attempts.add(executor.submit(() -> {
                ready.countDown();
                if (!start.await(2, TimeUnit.SECONDS)) throw new AssertionError("Admission start timed out");
                try { return pool.acquire(); }
                catch (RejectedExecutionException full) { return null; }
            }));
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            start.countDown();
            for (var attempt : attempts) {
                var lease = attempt.get(2, TimeUnit.SECONDS);
                if (lease != null) accepted.add(lease);
            }
            assertEquals(6, accepted.size());
            assertEquals(6, pool.metrics().get("watchSlots"));
            assertEquals(List.of(2, 2, 2), pool.metrics().get("watchSlotsByChannel"));
            assertEquals(58L, pool.metrics().get("watchPoolRejected"));

            var releases = new ArrayList<Future<?>>();
            for (var lease : accepted) {
                for (int i = 0; i < 8; i++) releases.add(executor.submit(lease::close));
            }
            for (var release : releases) release.get(2, TimeUnit.SECONDS);
            assertEquals(0, pool.metrics().get("watchSlots"));
            assertEquals(List.of(0, 0, 0), pool.metrics().get("watchSlotsByChannel"));
            accepted.clear();
            for (int i = 0; i < 6; i++) accepted.add(pool.acquire());
            assertThrows(RejectedExecutionException.class, pool::acquire,
                    "重复释放不能使池随后超额受理");
        } finally {
            start.countDown();
            accepted.forEach(MapWatchPool.Lease::close);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test void callerCannotChangeChannelsOrPreviouslyReportedOccupancy() {
        var channels = new ArrayList<Channel>(channels(2));
        var original = channels.getFirst();
        var pool = new MapWatchPool(channels, 1, 2);
        var before = pool.metrics();
        channels.clear();
        try (var lease = pool.acquire()) {
            assertSame(original, lease.channel());
            assertEquals(List.of(0, 0), before.get("watchSlotsByChannel"));
            assertEquals(List.of(1, 0), pool.metrics().get("watchSlotsByChannel"));
        }
    }

    @Test void invalidConfigurationCannotClaimNonexistentTransportCapacity() {
        var channel = new ControlledGrpcChannel();
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(null, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(List.of(), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(channels(129), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(Arrays.asList(channel, null), 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(List.of(channel, channel), 1, 2));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(List.of(channel), 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(List.of(channel),
                RpcTransport.MAX_CONCURRENT_CALLS_PER_CONNECTION + 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapWatchPool(List.of(channel), 1, 0));
        assertDoesNotThrow(() -> new MapWatchPool(channels(128),
                RpcTransport.MAX_CONCURRENT_CALLS_PER_CONNECTION, Integer.MAX_VALUE));
    }

    private static List<Channel> channels(int count) {
        var channels = new ArrayList<Channel>(count);
        for (int i = 0; i < count; i++) channels.add(new ControlledGrpcChannel());
        return channels;
    }
}

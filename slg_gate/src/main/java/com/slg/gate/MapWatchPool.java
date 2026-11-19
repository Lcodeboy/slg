package com.slg.gate;

import com.slg.rpc.RpcTransport;
import io.grpc.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

/**
 * 固定地图长流连接池：同时限制每条连接与全池占用，避免在 HTTP/2 并发流名额后无界等待。
 * 池只分配传输槽，不拥有 Channel 生命周期，也不重试、迁移或重建订阅；地图一致性仍由
 * subscriptionId、generation 与连续水位保证。调用方必须在实际传输结束后归还 lease，
 * 本地已取消但仍等待 onClose 的流继续计入占用，不能被新订阅提前复用。
 */
final class MapWatchPool {
    private final List<Channel> channels;
    private final int streamsPerChannel, transportCapacity, limit;
    private final int[] heldByChannel;
    // 以下字段及 Lease.released 均由本对象的监视器保护；选择连接与登记占用是一次准入决定。
    private int held, next;
    private long rejected;

    MapWatchPool(List<? extends Channel> channels, int streamsPerChannel, int requestedLimit) {
        if (channels == null || channels.isEmpty() || channels.size() > 128)
            throw new IllegalArgumentException("Map watch pool requires 1..128 channels");
        if (streamsPerChannel < 1 || streamsPerChannel > RpcTransport.MAX_CONCURRENT_CALLS_PER_CONNECTION)
            throw new IllegalArgumentException("Invalid map watch streams per channel");
        if (requestedLimit < 1) throw new IllegalArgumentException("Map watch limit must be positive");
        var identities = Collections.newSetFromMap(new IdentityHashMap<Channel, Boolean>());
        for (Channel channel : channels) {
            // 同一实例重复登记并不会增加 HTTP/2 容量，却会错误翻倍本地预算，启动时即拒绝。
            if (channel == null || !identities.add(channel))
                throw new IllegalArgumentException("Map watch channels must be non-null and distinct instances");
        }
        this.channels = List.copyOf(channels);
        this.streamsPerChannel = streamsPerChannel;
        this.transportCapacity = channels.size() * streamsPerChannel;
        this.limit = Math.min(requestedLimit, transportCapacity);
        this.heldByChannel = new int[channels.size()];
    }

    /**
     * 只在有空槽时受理，满额立即失败。优先最轻连接，平局轮转，防止不均匀取消后仍把新流
     * 交给已满连接。已有 lease 始终绑定原 Channel，不以重新路由掩盖断流或接续旧快照。
     */
    synchronized Lease acquire() {
        if (held >= limit) {
            rejected++;
            throw new RejectedExecutionException("Map watch transport capacity exhausted");
        }
        int selected = -1, lightest = streamsPerChannel;
        for (int offset = 0; offset < channels.size(); offset++) {
            int index = (next + offset) % channels.size();
            if (heldByChannel[index] < lightest) {
                selected = index;
                lightest = heldByChannel[index];
                if (lightest == 0) break;
            }
        }
        // limit 不超过物理槽总量，且受理/释放共用同一把锁；有全局余量时必有至少一条空槽。
        if (selected < 0) throw new IllegalStateException("Map watch pool accounting is inconsistent");
        heldByChannel[selected]++;
        held++;
        next = (selected + 1) % channels.size();
        return new Lease(selected);
    }

    int limit() { return limit; }

    /** 一次锁内复制计数，诊断能核对总槽数与分连接之和；返回值不暴露可变池状态。 */
    synchronized Map<String, Object> metrics() {
        var byChannel = new ArrayList<Integer>(heldByChannel.length);
        for (int count : heldByChannel) byChannel.add(count);
        return Map.of("watchChannels", channels.size(), "watchStreamsPerChannel", streamsPerChannel,
                "watchTransportCapacity", transportCapacity, "watchSlots", held,
                "watchSlotsByChannel", List.copyOf(byChannel), "watchPoolRejected", rejected);
    }

    final class Lease implements AutoCloseable {
        private final int index;
        private boolean released;

        private Lease(int index) { this.index = index; }

        Channel channel() { return channels.get(index); }

        /** 取消、失败与远端终态可能竞争；同一传输槽最多归还一次，不能把预算释放成负数。 */
        @Override public void close() {
            synchronized (MapWatchPool.this) {
                if (released) return;
                released = true;
                heldByChannel[index]--;
                held--;
            }
        }
    }
}

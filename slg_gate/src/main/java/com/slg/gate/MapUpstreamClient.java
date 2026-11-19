package com.slg.gate;

import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.MapQueryReply;
import com.slg.rpc.v1.MapQueryRequest;
import com.slg.rpc.v1.MapRpcGrpc;
import com.slg.rpc.v1.MapViewUpdate;
import com.slg.rpc.v1.MapWatchRequest;
import com.slg.rpc.v1.SessionKey;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Map;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 地图专用的有界 RPC 入口：查询使用独立 Channel，Watch 使用固定连接池，当前目标仍是 GAME。
 * 每条订阅固定一个传输槽，断流不在池内重放或拼接；客户端须以新订阅 ID 取完整快照。
 * 池满立即拒绝，不把额外 Watch 排入无界等待队列；玩法与会话控制拥有各自的连接和预算。
 */
final class MapUpstreamClient implements AutoCloseable {
    private final Channel queryChannel;
    private final MapWatchPool watchPool;
    private final int queryLimit, watchLimit, timeoutMillis;
    private final Semaphore queries;
    private final Set<Watch> streams = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<MapQueryReply>> pending = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ThreadPoolExecutor callbacks = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(256), Thread.ofPlatform().daemon().name("map-rpc-callback-", 0).factory());
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("map-rpc-deadline").factory());

    MapUpstreamClient(Channel channel, GatewaySettings settings) {
        this(channel, List.of(channel), settings); // 仅供可控制 Channel 的测试夹具共用。
    }

    MapUpstreamClient(Channel queryChannel, List<? extends Channel> watchChannels, GatewaySettings settings) {
        this.queryChannel = java.util.Objects.requireNonNull(queryChannel);
        watchPool = new MapWatchPool(watchChannels, settings.mapWatchStreamsPerChannel(), settings.maxConnections());
        queryLimit = Math.min(64, settings.gameInflight());
        watchLimit = watchPool.limit();
        timeoutMillis = settings.rpcTimeoutMillis();
        queries = new Semaphore(queryLimit);
        deadlines.setRemoveOnCancelPolicy(true);
    }

    CompletableFuture<MapQueryReply> query(MapQueryRequest request, String token, SessionKey binding) {
        if (closed.get() || !queries.tryAcquire()) return CompletableFuture.failedFuture(busy());
        var result = new CompletableFuture<MapQueryReply>();
        var done = new AtomicBoolean();
        var transport = new AtomicReference<ClientCall<MapQueryRequest, MapQueryReply>>();
        var deadline = new AtomicReference<ScheduledFuture<?>>();
        pending.add(result);
        result.whenComplete((reply, error) -> {
            if (!done.compareAndSet(false, true)) return;
            var timeout = deadline.get(); if (timeout != null) timeout.cancel(false);
            pending.remove(result);
            queries.release();
            var wire = transport.get();
            if ((error != null || result.isCancelled()) && wire != null) cancel(wire);
        });
        try {
            if (closed.get()) { result.cancel(false); return result; }
            var wire = queryChannel.newCall(MapRpcGrpc.getQueryMethod(), CallOptions.DEFAULT
                    .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .withExecutor(work -> execute(work, () -> result.completeExceptionally(busy()))));
            transport.set(wire);
            var timeout = deadlines.schedule(() -> result.completeExceptionally(
                    Status.DEADLINE_EXCEEDED.withDescription("Map query timed out").asRuntimeException()),
                    timeoutMillis, TimeUnit.MILLISECONDS);
            deadline.set(timeout);
            if (result.isDone()) { timeout.cancel(false); cancel(wire); return result; }
            wire.start(new ClientCall.Listener<>() {
                private MapQueryReply response;
                @Override public void onMessage(MapQueryReply value) {
                    if (result.isDone()) return;
                    if (response != null || value.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES
                            || !value.hasSnapshot()) {
                        result.completeExceptionally(new IllegalStateException("Invalid map query response"));
                    } else response = value;
                }
                @Override public void onClose(Status status, Metadata trailers) {
                    if (!status.isOk()) result.completeExceptionally(status.asRuntimeException());
                    else if (response == null) result.completeExceptionally(new IllegalStateException("Missing map view"));
                    else result.complete(response);
                }
            }, RpcMetadata.headers(token, binding));
            wire.request(2); // unary 的第二个响应会被识别为协议错误，不允许无限缓存。
            wire.sendMessage(request);
            wire.halfClose();
        } catch (RuntimeException failure) { result.completeExceptionally(failure); }
        return result;
    }

    /**
     * 每条流仅请求一条地图消息。接收方把它真正写入 TCP 后才能 requestNext，
     * 因此慢 Unity 不会让 gRPC 回调不断向 Netty EventLoop 塞入无界任务。
     */
    Watch watch(MapWatchRequest request, String token, SessionKey binding, Consumer<MapViewUpdate> update, Consumer<Throwable> failed) {
        if (closed.get()) throw busy();
        var watch = new Watch(request.getSubscriptionId(), failed, watchPool.acquire());
        streams.add(watch);
        try {
            if (closed.get()) { watch.close(); throw busy(); }
            // 长订阅不沿用三秒 unary deadline；由会话到期、断连、取消和两端背压结束。
            var wire = watch.lease.channel().newCall(MapRpcGrpc.getWatchMethod(), CallOptions.DEFAULT
                    .withExecutor(work -> execute(work, () -> watch.fail(busy()))));
            boolean started = watch.start(wire, new ClientCall.Listener<>() {
                @Override public void onMessage(MapViewUpdate value) {
                    if (watch.finished.get()) return;
                    if (!value.getSubscriptionId().equals(watch.id)
                            || value.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES) {
                        watch.fail(new IllegalStateException("Invalid map subscription response"));
                        return;
                    }
                    if (!watch.acceptFirstMessage()) return;
                    try { update.accept(value); }
                    catch (RuntimeException failure) { watch.fail(failure); }
                }
                @Override public void onClose(Status status, Metadata trailers) {
                    // onClose 才证明本次 ClientCall 已终止。即使本地早已取消，也不能提前复用它的槽。
                    watch.lease.close();
                    // 正常结束同样表示这个地图订阅失效；客户端必须显式以新 ID 重建。
                    watch.fail(status.isOk() ? new IllegalStateException("Map subscription ended") : status.asRuntimeException());
                }
            }, RpcMetadata.headers(token, binding));
            if (started) {
                wire.request(1);
                wire.sendMessage(request);
                wire.halfClose();
            }
            return watch;
        } catch (RuntimeException | Error failure) { watch.close(); throw failure; }
    }

    private void execute(Runnable work, Runnable reject) {
        try { callbacks.execute(work); }
        catch (RejectedExecutionException full) {
            reject.run();
            // 仍运行 gRPC 框架终态任务；业务 observer 会先检查 finished，避免遗留 transport 状态。
            work.run();
        }
    }

    private static RejectedExecutionException busy() { return new RejectedExecutionException("Map RPC capacity exhausted"); }
    private static void cancel(ClientCall<?, ?> call) {
        try { call.cancel("Map operation closed", null); }
        catch (RuntimeException ignored) {
            // 坏传输不能中断其余调用的关闭；已启动 Watch 仍保留槽，直到 onClose 确认终止。
        }
    }

    Map<String, Object> metrics() {
        var result = new LinkedHashMap<String, Object>(watchPool.metrics());
        result.put("queryLimit", queryLimit);
        result.put("queries", queryLimit - queries.availablePermits());
        result.put("watchLimit", watchLimit);
        result.put("watches", streams.size());
        result.put("callbackQueue", callbacks.getQueue().size());
        return Map.copyOf(result);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        streams.forEach(Watch::close);
        pending.forEach(value -> value.cancel(false));
        deadlines.shutdownNow();
        callbacks.shutdown();
    }

    final class Watch implements AutoCloseable {
        private final String id;
        private final Consumer<Throwable> failed;
        private final MapWatchPool.Lease lease;
        private final AtomicBoolean finished = new AtomicBoolean();
        private volatile ClientCall<MapWatchRequest, MapViewUpdate> wire;
        private ScheduledFuture<?> firstMessageTimeout;
        private boolean receivedFirst, started;
        Watch(String id, Consumer<Throwable> failed, MapWatchPool.Lease lease) {
            this.id = id; this.failed = failed; this.lease = lease;
        }
        /**
         * start 与本地终止协调，避免关闭先归还槽后又启动一条无主流。newCall/start 抛错时没有
         * 可依赖的终态回调，直接取消并归还；正常启动后，无论超时或主动取消，都等 onClose 归还。
         */
        private synchronized boolean start(ClientCall<MapWatchRequest, MapViewUpdate> call,
                                           ClientCall.Listener<MapViewUpdate> listener, Metadata headers) {
            wire = call;
            if (finished.get()) { cancel(call); return false; }
            armFirstMessageTimeout();
            started = true;
            try { call.start(listener, headers); }
            catch (RuntimeException | Error failure) {
                cancel(call);
                lease.close();
                throw failure;
            }
            if (finished.get()) { cancel(call); return false; }
            return true;
        }
        /**
         * 首包等待覆盖连接建立、HTTP/2 并发流排队、服务端订阅准入及初始快照。
         * 一条 Watch 只登记一个定时任务，数量受 watchLimit 限制；首包到达或结束即取消并移出定时队列。
         */
        synchronized void armFirstMessageTimeout() {
            if (finished.get()) return;
            firstMessageTimeout = deadlines.schedule(this::expireFirstMessage, timeoutMillis, TimeUnit.MILLISECONDS);
        }
        private synchronized void expireFirstMessage() {
            if (!receivedFirst && !finished.get()) fail(Status.DEADLINE_EXCEEDED
                    .withDescription("Map initial snapshot timed out; subscribe again").asRuntimeException());
        }
        private synchronized boolean acceptFirstMessage() {
            if (finished.get()) return false;
            if (!receivedFirst) {
                receivedFirst = true;
                if (firstMessageTimeout != null) firstMessageTimeout.cancel(false);
            }
            return true;
        }
        void requestNext() {
            var call = wire;
            if (!finished.get() && call != null) {
                try { call.request(1); }
                catch (RuntimeException failure) { fail(failure); }
            }
        }
        private void fail(Throwable failure) {
            if (finish()) {
                try { failed.accept(failure); }
                catch (RuntimeException ignored) { /* 调用方已离线时仍应完成传输与名额回收。 */ }
            }
        }
        private boolean finish() {
            if (!finished.compareAndSet(false, true)) return false;
            synchronized (this) {
                if (firstMessageTimeout != null) firstMessageTimeout.cancel(false);
                // 尚未 start 的调用不可能再启动，故可直接归还；已启动的流保留槽直至传输终态。
                if (!started) lease.close();
            }
            streams.remove(this);
            var call = wire;
            if (call != null) cancel(call);
            return true;
        }
        @Override public void close() { finish(); }
    }
}

package com.slg.gate;

import com.google.protobuf.MessageLite;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.core.metrics.LatencyMetric;
import com.slg.rpc.v1.*;
import io.grpc.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Gateway 的连接控制通道。认证只执行一次 Prepare/Open，既不自动重试，也不逐条业务访问 UC/SQL。
 * 所有连接共享批量 Check：每 100ms 至多一批 256 个、全局最多一个在途，不为每玩家建立长期 RPC。
 * GAME 负责真正的执行隔离；这里的租约和踢线负责及时释放 TCP/地图资源，不是写入安全的唯一防线。
 */
final class SessionUpstreamClient implements AutoCloseable {
    private static final int BATCH = 256, TICK_MILLIS = 100, LEASE_MILLIS = 30_000;
    private final Channel channel;
    private final int capacity, timeoutMillis, openLimit;
    private final Semaphore slots, openings, closings = new Semaphore(32);
    private final Object monitor = new Object();
    // 取出一批后放到尾部，实现有界公平轮转，不按连接数量创建周期定时器。
    private final LinkedHashMap<SessionKey, Lease> active = new LinkedHashMap<>();
    private final Set<CompletableFuture<?>> pending = ConcurrentHashMap.newKeySet();
    private final ScheduledThreadPoolExecutor clock = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("session-rpc-timer").factory());
    private final ThreadPoolExecutor callbacks = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(256), Thread.ofPlatform().daemon().name("session-rpc-callback-", 0).factory());
    private final AtomicBoolean closed = new AtomicBoolean(), checking = new AtomicBoolean();
    private final LatencyMetric checkLatency = new LatencyMetric();
    private final LongAdder checkBatches = new LongAdder();
    private volatile long lastCheckCompletedNanos;
    private volatile int lastBatchSize;

    SessionUpstreamClient(Channel channel, GatewaySettings settings) {
        this.channel = channel;
        capacity = settings.maxConnections();
        // 这里只按低延迟控制面估算理想轮转，绝不代表该连接数已验收。RTT变长会消耗余量，
        // 因此另暴露check耗时/进度，实际超出租约始终失败关闭，不能自动延长授权。
        // 预留三倍租约余量。若以后要增加单 Gateway 容量，应先增加可验证的批量控制吞吐，
        // 不能接受百万连接配置后，让一次正常轮转已经超过 30 秒租约。
        requireCapacity(capacity);
        timeoutMillis = settings.rpcTimeoutMillis();
        openLimit = Math.min(64, settings.gameInflight());
        slots = new Semaphore(capacity);
        openings = new Semaphore(openLimit);
        clock.setRemoveOnCancelPolicy(true);
        clock.scheduleWithFixedDelay(this::tick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
    }

    static void requireCapacity(int capacity) {
        if ((long) ((capacity + BATCH - 1) / BATCH) * TICK_MILLIS > LEASE_MILLIS / 3)
            throw new IllegalArgumentException("Gateway connection limit exceeds bounded session renewal budget (25600)");
    }

    CompletableFuture<Lease> open(String token, String account, Consumer<String> revoked) {
        if (closed.get() || !slots.tryAcquire()) return CompletableFuture.failedFuture(busy());
        if (!openings.tryAcquire()) { slots.release(); return CompletableFuture.failedFuture(busy()); }
        long began = System.nanoTime(), deadline = began + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        var result = new CompletableFuture<Lease>();
        var current = new AtomicReference<CompletableFuture<?>>();
        var slotOwner = new AtomicInteger(); // 0=认证拥有，1=Lease拥有，2=已释放。
        pending.add(result);
        result.whenComplete((lease, failure) -> {
            pending.remove(result);
            openings.release();
            if (slotOwner.compareAndSet(0, 2)) slots.release();
            var call = current.get();
            if (failure != null && call != null) call.cancel(false);
        });
        var prepared = unary(SessionRpcGrpc.getPrepareMethod(), SessionPrepareRequest.getDefaultInstance(), token, deadline);
        current.set(prepared);
        prepared.whenComplete((preparation, failure) -> {
            if (result.isDone()) return;
            if (failure != null) { result.completeExceptionally(failure); return; }
            if (!uuid(preparation.getGameInstance()) || !uuid(preparation.getTicketId()) || preparation.getObservedGeneration() < 0) {
                result.completeExceptionally(invalid()); return;
            }
            String attempt = UUID.randomUUID().toString();
            var request = SessionOpenRequest.newBuilder().setGameInstance(preparation.getGameInstance())
                    .setExpectedGeneration(preparation.getObservedGeneration()).setAttemptId(attempt)
                    .setTicketId(preparation.getTicketId()).build();
            var opened = unary(SessionRpcGrpc.getOpenMethod(), request, token, deadline);
            current.set(opened);
            if (result.isDone()) { opened.cancel(false); return; }
            opened.whenComplete((binding, error) -> {
                if (error != null) { result.completeExceptionally(error); return; }
                if (!binding.hasKey() || !account.equals(binding.getKey().getAccountId())
                        || !binding.getKey().getGameInstance().equals(preparation.getGameInstance())
                        || !uuid(binding.getKey().getBindingId()) || binding.getKey().getGeneration() <= 0
                        || binding.getExpiresAt() <= 0 || binding.getLeaseMillis() <= 0 || binding.getLeaseMillis() > LEASE_MILLIS) {
                    result.completeExceptionally(invalid()); return;
                }
                var lease = new Lease(binding, began + TimeUnit.MILLISECONDS.toNanos(binding.getLeaseMillis()), revoked, slotOwner);
                synchronized (monitor) {
                    if (result.isDone() || closed.get() || !lease.valid()) {
                        // TCP 已经关闭或回复太晚：远端可能已经绑定，尽力精确关闭；丢包时由租约兜底。
                        releaseRemote(binding.getKey());
                        result.completeExceptionally(Status.CANCELLED.asRuntimeException());
                        return;
                    }
                    if (!slotOwner.compareAndSet(0, 1)) { releaseRemote(lease.key()); return; }
                    active.put(lease.key(), lease);
                    if (!result.complete(lease)) {
                        // 与调用方 cancel 竞争：名额可能已经由上面的终态回调释放，不能释放两次。
                        lease.close();
                    }
                }
            });
        });
        if (result.isDone()) prepared.cancel(false);
        return result;
    }

    private void tick() {
        if (closed.get() || !checking.compareAndSet(false, true)) return;
        List<Lease> selected = new ArrayList<>(BATCH), expired = new ArrayList<>();
        synchronized (monitor) {
            var iterator = active.entrySet().iterator();
            while (iterator.hasNext() && selected.size() + expired.size() < BATCH) {
                Lease lease = iterator.next().getValue();
                iterator.remove();
                if (lease.valid()) selected.add(lease); else expired.add(lease);
            }
            selected.forEach(lease -> active.put(lease.key(), lease));
        }
        expired.forEach(lease -> lease.revoke("SESSION_EXPIRED"));
        if (selected.isEmpty()) { checking.set(false); return; }
        long began = System.nanoTime();
        var request = SessionCheckRequest.newBuilder();
        selected.forEach(lease -> request.addBindings(lease.key()));
        unary(SessionRpcGrpc.getCheckMethod(), request.build(), null,
                began + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)).whenComplete((reply, failure) -> {
            boolean acknowledged = false;
            try {
                // 故障不把旧租约重新延长。临时失联可以等下一轮，截止后请求/推送必须失败关闭。
                if (failure != null) return;
                var requested = new HashSet<SessionKey>();
                selected.forEach(lease -> requested.add(lease.key()));
                var current = new HashSet<>(reply.getCurrentList());
                if (reply.getLeaseMillis() != LEASE_MILLIS || current.size() != reply.getCurrentCount()
                        || !requested.containsAll(current)) {
                    selected.forEach(lease -> lease.revoke("SESSION_UNAVAILABLE")); return;
                }
                long until = began + TimeUnit.MILLISECONDS.toNanos(reply.getLeaseMillis());
                acknowledged = true;
                for (Lease lease : selected) {
                    if (current.contains(lease.key())) lease.renew(until);
                    else lease.revoke("SESSION_REPLACED");
                }
            } finally {
                lastCheckCompletedNanos = System.nanoTime();
                lastBatchSize = selected.size();
                checkBatches.increment();
                checkLatency.record(lastCheckCompletedNanos - began, acknowledged);
                checking.set(false);
            }
        });
    }

    /** 断开只是精确释放当前连接；异步关闭故障不能删除后续新会话，超预算则交给租约回收。 */
    private void releaseRemote(SessionKey key) {
        if (closed.get() || !closings.tryAcquire()) return;
        unary(SessionRpcGrpc.getCloseMethod(), SessionCloseRequest.newBuilder().setBinding(key).build(), null,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis))
                .whenComplete((ignored, error) -> closings.release());
    }

    private <Q extends MessageLite, R extends MessageLite> CompletableFuture<R> unary(
            MethodDescriptor<Q, R> method, Q request, String token, long deadlineNanos) {
        var result = new CompletableFuture<R>();
        long remaining = deadlineNanos - System.nanoTime();
        if (closed.get() || remaining <= 0) {
            result.completeExceptionally(Status.DEADLINE_EXCEEDED.asRuntimeException()); return result;
        }
        var transport = new AtomicReference<ClientCall<Q, R>>();
        var timeout = new AtomicReference<ScheduledFuture<?>>();
        pending.add(result);
        result.whenComplete((reply, error) -> {
            pending.remove(result);
            var scheduled = timeout.get(); if (scheduled != null) scheduled.cancel(false);
            var wire = transport.get(); if (error != null && wire != null) cancel(wire);
        });
        try {
            var wire = channel.newCall(method, CallOptions.DEFAULT.withDeadlineAfter(remaining, TimeUnit.NANOSECONDS)
                    .withExecutor(work -> {
                        try { callbacks.execute(work); }
                        catch (RejectedExecutionException overloaded) {
                            result.completeExceptionally(busy());
                            work.run(); // gRPC 终态仍须排空；observer 先检查 result，不在退化线程执行绑定业务。
                        }
                    }));
            transport.set(wire);
            timeout.set(clock.schedule(() -> result.completeExceptionally(Status.DEADLINE_EXCEEDED.asRuntimeException()),
                    remaining, TimeUnit.NANOSECONDS));
            if (result.isDone()) { cancel(wire); return result; }
            wire.start(new ClientCall.Listener<>() {
                private R response;
                @Override public void onMessage(R value) {
                    if (result.isDone()) return;
                    if (response != null || value.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES)
                        result.completeExceptionally(invalid());
                    else response = value;
                }
                @Override public void onClose(Status status, Metadata trailers) {
                    if (!status.isOk()) result.completeExceptionally(status.asRuntimeException());
                    else if (response == null) result.completeExceptionally(invalid());
                    else result.complete(response);
                }
            }, RpcMetadata.headers(token, null));
            wire.request(2); wire.sendMessage(request); wire.halfClose();
        } catch (RuntimeException error) { result.completeExceptionally(error); }
        return result;
    }

    private static void cancel(ClientCall<?, ?> call) {
        try { call.cancel("Session operation ended", null); } catch (RuntimeException ignored) { }
    }
    private static boolean uuid(String value) {
        try { return UUID.fromString(value).toString().equals(value) && !value.equals("00000000-0000-0000-0000-000000000000"); } catch (IllegalArgumentException invalid) { return false; }
    }
    private static RuntimeException invalid() { return Status.INTERNAL.withDescription("Invalid session response").asRuntimeException(); }
    private static RuntimeException busy() { return Status.RESOURCE_EXHAUSTED.withDescription("Session control capacity exhausted").asRuntimeException(); }

    Map<String, Object> metrics() {
        synchronized (monitor) {
            return Map.ofEntries(Map.entry("active", active.size()), Map.entry("capacity", capacity),
                    Map.entry("opening", openLimit - openings.availablePermits()), Map.entry("checking", checking.get()),
                    Map.entry("callbackQueue", callbacks.getQueue().size()), Map.entry("check", checkLatency.snapshot()),
                    Map.entry("checkBatches", checkBatches.sum()), Map.entry("lastBatchSize", lastBatchSize),
                    Map.entry("lastCheckAgeMillis", lastCheckCompletedNanos == 0 ? -1L
                            : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastCheckCompletedNanos)),
                    Map.entry("minimumSweepMillis", (long) ((active.size() + BATCH - 1) / BATCH) * TICK_MILLIS));
        }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        List<Lease> leases;
        synchronized (monitor) { leases = List.copyOf(active.values()); }
        leases.forEach(lease -> lease.revoke("SESSION_UNAVAILABLE"));
        pending.forEach(call -> call.cancel(false));
        clock.shutdownNow(); callbacks.shutdown();
    }

    final class Lease implements AutoCloseable {
        private final SessionBinding binding;
        private final Consumer<String> revoked;
        // gRPC 回调先失效 Lease，踢线通知再排到 TCP EventLoop。资格与首个原因必须一次发布，
        // 否则旧帧/周期巡检会在通知前只看到已失效，把真实接管误报为租约到期。
        private final AtomicReference<String> terminalReason = new AtomicReference<>();
        private final AtomicInteger slotOwner;
        private volatile long until;
        Lease(SessionBinding binding, long until, Consumer<String> revoked, AtomicInteger slotOwner) {
            this.binding = binding; this.until = until; this.revoked = revoked; this.slotOwner = slotOwner;
        }
        SessionKey key() { return binding.getKey(); }
        boolean valid() { return invalidReason() == null; }
        /** null 表示当前仍有效；已知终态优先于时钟推断，读者无需等待异步踢线回调。 */
        String invalidReason() {
            String reason = terminalReason.get();
            if (reason != null) return reason;
            return System.nanoTime() - until < 0 ? null : "SESSION_EXPIRED";
        }
        private void renew(long deadline) {
            if (terminalReason.get() == null && System.nanoTime() - until < 0) until = deadline;
            else revoke("SESSION_EXPIRED"); // 迟到续租回复不得复活已经过期的 TCP 资格。
        }
        private void revoke(String reason) {
            if (finish(reason)) try { revoked.accept(reason); } catch (RuntimeException disconnected) { }
        }
        private boolean finish(String reason) {
            // close、到期和控制面撤销只允许首个终态释放资源；迟到通知不能改写接管原因。
            if (!terminalReason.compareAndSet(null, reason)) return false;
            synchronized (monitor) { active.remove(key()); }
            if (slotOwner.compareAndSet(1, 2)) slots.release();
            releaseRemote(key());
            return true;
        }
        @Override public void close() { finish("SESSION_CLOSED"); }
    }
}

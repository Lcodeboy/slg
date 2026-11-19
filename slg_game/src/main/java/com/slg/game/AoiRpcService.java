package com.slg.game;

import com.slg.aoi.AoiRuntime;
import com.slg.aoi.MapStateSource;
import com.slg.core.auth.SessionClaims;
import com.slg.core.auth.TokenService;
import com.slg.core.http.RateLimiter;
import com.slg.core.world.Viewport;
import com.slg.core.world.RouteIndex;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;

/**
 * GAME 内的独立地图入口。网络线程只验签和受理；角色归属读取、视野查询和输出均有单独预算，
 * 不进入玩家邮箱。以后 AOI 独立部署时保留此协议，只更换只读数据源和 Gateway 的目标地址。
 */
final class AoiRpcService extends MapRpcGrpc.MapRpcImplBase implements AutoCloseable {
    @FunctionalInterface interface RealmAccess { boolean allows(String account) throws Exception; }
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final AoiRuntime aoi;
    private final TokenService tokens;
    private final SessionDirectory sessions;
    private final BooleanSupplier ready;
    private final RealmAccess access;
    private final RateLimiter rate = new RateLimiter(120, 60, 100_000);
    private final ThreadPoolExecutor admission = pool("aoi-access-", 2, 128);
    private final ThreadPoolExecutor outputs = pool("aoi-rpc-output-", 2, 128);
    private final ScheduledExecutorService expiry = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("aoi-session-expiry").factory());
    private final Set<Watch> watches = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    AoiRpcService(AoiRuntime aoi, TokenService tokens, BooleanSupplier ready, RealmAccess access, SessionDirectory sessions) {
        this.aoi = aoi;
        this.tokens = tokens;
        this.ready = ready;
        this.access = access;
        this.sessions = sessions;
        // 长流没有固定的业务 RPC 超时。即使地图不再变化，也须及时释放到期会话及停止中的区服订阅。
        expiry.scheduleWithFixedDelay(() -> watches.forEach(Watch::checkSession), 1, 1, TimeUnit.SECONDS);
    }

    private static ThreadPoolExecutor pool(String name, int workers, int capacity) {
        return new ThreadPoolExecutor(workers, workers, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(capacity), Thread.ofPlatform().daemon().name(name, 0).factory());
    }

    @Override public void query(MapQueryRequest request, StreamObserver<MapQueryReply> output) {
        Context context = Context.current();
        SessionKey binding = RpcMetadata.SESSION.get();
        SessionClaims session;
        Viewport viewport;
        try {
            session = authenticate();
            // 请求关联属于 Gateway 的固定消息头；内部 unary RPC 自身已关联调用和响应，
            // AOI 只接收实际视野参数，避免正文再维护第二份 requestId。
            viewport = viewport(request.hasViewport(), request.getViewport());
        } catch (RuntimeException failure) { output.onError(status(failure).asRuntimeException()); return; }
        submitAccess(context, session, binding, output, () -> {
            var future = aoi.query(viewport);
            Context.CancellationListener cancelled = ignored -> future.cancel(false);
            context.addListener(cancelled, Runnable::run);
            future.whenComplete((view, failure) -> {
                context.removeListener(cancelled);
                if (context.isCancelled()) return;
                if (failure != null) { output.onError(status(failure).asRuntimeException()); return; }
                if (!current(session, binding)) {
                    output.onError(Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED").asRuntimeException());
                    return;
                }
                var snapshot = MapViewSnapshot.newBuilder().setRealmId(view.identity().realmId())
                        .setDatasetId(view.identity().datasetId()).setGeneration(view.generation())
                        .setAppliedSeq(view.appliedSeq());
                view.routes().forEach(route -> snapshot.addRoutes(wire(route)));
                view.objects().forEach(object -> snapshot.addObjects(MapObjectWire.encode(object)));
                var reply = MapQueryReply.newBuilder().setSnapshot(snapshot).build();
                if (reply.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES - 128) {
                    output.onError(Status.RESOURCE_EXHAUSTED.withDescription("Viewport result exceeds byte budget").asRuntimeException());
                    return;
                }
                // 查询和编码在独立池完成，期间可能已经换到另一 Gateway；不能把迟到查询交给旧会话。
                if (!current(session, binding)) {
                    output.onError(Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED").asRuntimeException());
                    return;
                }
                output.onNext(reply);
                output.onCompleted();
            });
        });
    }

    @Override public void watch(MapWatchRequest request, StreamObserver<MapViewUpdate> output) {
        Context context = Context.current();
        SessionKey binding = RpcMetadata.SESSION.get();
        SessionClaims session;
        Viewport viewport;
        try {
            session = authenticate();
            if (!ID.matcher(request.getSubscriptionId()).matches()) throw new IllegalArgumentException("Valid subscription identifier required");
            viewport = viewport(request.hasViewport(), request.getViewport());
        } catch (RuntimeException failure) { output.onError(status(failure).asRuntimeException()); return; }
        var stream = new Watch(request.getSubscriptionId(), session, binding, context,
                (ServerCallStreamObserver<MapViewUpdate>) output);
        stream.output.setOnCancelHandler(stream::cancel);
        stream.output.setOnReadyHandler(stream::schedule);
        submitAccess(context, session, binding, output, () -> {
            if (context.isCancelled() || closed.get()) { stream.cancel(); return; }
            if (!stream.activate()) return;
            // 初始快照与后续增量在 AOI 的串行通道衔接。取消可能先于订阅创建完成，完成后须再次关闭。
            aoi.subscribe(viewport, stream).whenComplete((subscription, failure) -> {
                if (failure != null) stream.fail(status(failure));
                else stream.attach(subscription);
            });
        });
    }

    private SessionClaims authenticate() {
        SessionClaims session;
        try { session = tokens.verify(RpcMetadata.TOKEN.get()); }
        catch (IllegalArgumentException bad) { throw Status.UNAUTHENTICATED.withDescription("Valid session required").asRuntimeException(); }
        if (closed.get() || !ready.getAsBoolean()) throw Status.UNAVAILABLE.withDescription("Realm not ready").asRuntimeException();
        if (!current(session, RpcMetadata.SESSION.get()))
            throw Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED").asRuntimeException();
        if (!rate.allow(session.accountId())) throw Status.RESOURCE_EXHAUSTED.withDescription("Map request rate exceeded").asRuntimeException();
        return session;
    }

    /** 账号令牌不代表区服授权。首次查询/订阅在独立读取池核对本区角色，不加载到玩家驻留目录。 */
    private void submitAccess(Context context, SessionClaims session, SessionKey binding, StreamObserver<?> output, Runnable work) {
        try {
            admission.execute(() -> {
                if (context.isCancelled()) return;
                try {
                    if (closed.get() || !ready.getAsBoolean()) throw Status.UNAVAILABLE.withDescription("Realm not ready").asRuntimeException();
                    if (!current(session, binding)) throw Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED").asRuntimeException();
                    if (!access.allows(session.accountId())) throw Status.PERMISSION_DENIED.withDescription("Role required in this realm").asRuntimeException();
                    if (!current(session, binding)) throw Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED").asRuntimeException();
                    if (!context.isCancelled()) work.run();
                } catch (Exception failure) { if (!context.isCancelled()) output.onError(status(failure).asRuntimeException()); }
            });
        } catch (RejectedExecutionException full) {
            output.onError(Status.RESOURCE_EXHAUSTED.withDescription("Map access queue full").asRuntimeException());
        }
    }

    private boolean current(SessionClaims claims, SessionKey binding) {
        return tokens.isCurrent(claims) && sessions.isCurrent(claims, binding);
    }

    private static Viewport viewport(boolean present, MapViewport viewport) {
        if (!present) throw new IllegalArgumentException("Viewport required");
        return new Viewport(viewport.getMinX(), viewport.getMinY(), viewport.getMaxX(), viewport.getMaxY());
    }

    private static Status status(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause() != null)
            failure = failure.getCause();
        if (failure instanceof StatusRuntimeException rpc) return rpc.getStatus();
        if (failure instanceof IllegalArgumentException) return Status.INVALID_ARGUMENT.withDescription("Invalid viewport or identifier");
        if (failure instanceof RejectedExecutionException || failure instanceof RouteIndex.QueryLimit)
            return Status.RESOURCE_EXHAUSTED.withDescription("AOI budget exceeded; retry or narrow viewport");
        // 不把原卷路径、解析异常或内部对象写给客户端；服务端健康信息记录消费停留位置。
        return Status.UNAVAILABLE.withDescription("AOI not ready; retry after recovery");
    }

    private static MapRouteView wire(MapStateSource.Route route) {
        return MapRouteView.newBuilder().setMarchId(route.marchId()).setVersion(route.version())
                .setState(route.direction() == MapStateSource.Direction.OUTBOUND
                        ? MarchState.MARCH_STATE_OUTBOUND : MarchState.MARCH_STATE_RETURNING)
                .setFromX(route.fromX()).setFromY(route.fromY()).setTargetX(route.targetX()).setTargetY(route.targetY())
                .setDepartAt(route.departAt()).setArriveAt(route.arriveAt()).build();
    }

    /**
     * 每条 RPC 最多保留一份待写增量和一份正在编码的增量。onReady 只调度，不在 Netty 线程查询地图。
     * 慢连接超过这份预算就要求重新订阅；绝不删除某个增量后继续冒充连续流，也不反压 GAME 提交。
     */
    private final class Watch implements AoiRuntime.ViewUpdateSink {
        final String id;
        final SessionClaims session;
        final SessionKey binding;
        final Context context;
        final ServerCallStreamObserver<MapViewUpdate> output;
        final AtomicBoolean scheduled = new AtomicBoolean();
        private boolean stopped;
        private AoiRuntime.Update pending;
        private AoiRuntime.Subscription subscription;

        Watch(String id, SessionClaims session, SessionKey binding, Context context, ServerCallStreamObserver<MapViewUpdate> output) {
            this.id = id; this.session = session; this.binding = binding; this.context = context; this.output = output;
        }

        void attach(AoiRuntime.Subscription value) {
            checkSession();
            boolean discard;
            synchronized (this) {
                discard = stopped || context.isCancelled();
                if (!discard) subscription = value;
            }
            if (discard) value.close();
        }

        boolean activate() {
            checkSession();
            synchronized (this) {
                if (stopped || context.isCancelled() || closed.get()) return false;
                watches.add(this);
                return true;
            }
        }

        @Override public boolean offer(AoiRuntime.Update value) {
            checkSession();
            synchronized (this) {
                if (stopped || pending != null || context.isCancelled()) return false;
                pending = value;
            }
            schedule();
            return true;
        }

        void schedule() {
            synchronized (this) { if (stopped || pending == null || !output.isReady()) return; }
            if (!scheduled.compareAndSet(false, true)) return;
            try { outputs.execute(this::flush); }
            catch (RejectedExecutionException full) {
                scheduled.set(false);
                fail(Status.RESOURCE_EXHAUSTED.withDescription("AOI_RESYNC_REQUIRED: output budget exceeded"));
            }
        }

        private void flush() {
            try {
                for (;;) {
                    AoiRuntime.Update value;
                    synchronized (this) {
                        if (stopped || pending == null || !output.isReady()) return;
                        value = pending; pending = null;
                    }
                    checkSession();
                    var update = MapViewUpdate.newBuilder().setSubscriptionId(id)
                            .setRealmId(value.identity().realmId()).setDatasetId(value.identity().datasetId())
                            .setGeneration(value.generation()).setFromSeq(value.fromSeq()).setThroughSeq(value.throughSeq())
                            .setSnapshot(value.snapshot()).addAllRemovedIds(value.removedIds())
                            .addAllRemovedObjectIds(value.removedObjectIds());
                    value.upserts().forEach(route -> update.addUpserts(wire(route)));
                    // 路线与占矿变化共享同一个 from/through 水位；编码成同一包后才交给 Gateway，
                    // 不能先发送“路线消失”，再用另一条无关消息表示“矿点被占用”。
                    value.objectUpserts().forEach(object -> update.addObjectUpserts(MapObjectWire.encode(object)));
                    var message = update.build();
                    if (message.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES - 128) {
                        resetRequired("Viewport result exceeds byte budget"); return;
                    }
                    boolean replaced;
                    synchronized (this) {
                        if (stopped || context.isCancelled()) return;
                        // 已进入网络缓冲的旧包不能撤回；客户端必须丢弃旧连接和旧订阅 ID 的包。
                        // 这里再次核验输出资格，防止编码期间发生的跨网关替换继续产生新输出。
                        replaced = !current(session, binding);
                        if (!replaced) output.onNext(message);
                    }
                    if (replaced) { fail(Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED")); return; }
                }
            } catch (RuntimeException failure) { fail(status(failure)); }
            finally { scheduled.set(false); schedule(); }
        }

        void checkSession() {
            if (!current(session, binding)) fail(Status.UNAUTHENTICATED.withDescription("SESSION_REPLACED"));
            else if (closed.get() || !ready.getAsBoolean()) fail(Status.UNAVAILABLE.withDescription("Realm not ready"));
        }

        @Override public void resetRequired(String reason) {
            fail(Status.RESOURCE_EXHAUSTED.withDescription("AOI_RESYNC_REQUIRED: " + reason));
        }

        void fail(Status reason) {
            AoiRuntime.Subscription previous;
            synchronized (this) {
                if (stopped) return;
                stopped = true; pending = null;
                previous = subscription; subscription = null;
                if (!context.isCancelled()) {
                    try { output.onError(reason.asRuntimeException()); }
                    catch (RuntimeException disconnected) { /* 传输已结束也必须继续释放 AOI 订阅。 */ }
                }
            }
            watches.remove(this);
            // 不持有传输锁调用 AOI：输出预算耗尽时 AOI 可能反向通知 reset，锁嵌套会造成关闭死锁。
            if (previous != null) previous.close();
        }

        void cancel() {
            AoiRuntime.Subscription previous;
            synchronized (this) {
                if (stopped) return;
                stopped = true; pending = null;
                previous = subscription; subscription = null;
            }
            watches.remove(this);
            if (previous != null) previous.close();
        }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        watches.forEach(watch -> watch.fail(Status.UNAVAILABLE.withDescription("AOI shutting down")));
        expiry.shutdownNow(); admission.shutdownNow(); outputs.shutdownNow();
        boolean interrupted = false;
        for (ExecutorService executor : new ExecutorService[]{expiry, admission, outputs}) {
            for (;;) {
                try { if (executor.awaitTermination(1, TimeUnit.SECONDS)) break; }
                catch (InterruptedException stopped) { interrupted = true; }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}

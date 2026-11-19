package com.slg.game;

import com.google.protobuf.CodedOutputStream;
import com.slg.aoi.MapSnapshotSource;
import com.slg.aoi.MapStateSource;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** 给 AOI 的区服只读接口。全量重建和增量追赶使用独立预算，均不占玩家邮箱或 Netty 线程。 */
final class MapFeedService extends MapFeedRpcGrpc.MapFeedRpcImplBase implements AutoCloseable {
    private static final int SNAPSHOT_CHUNK_ENTRIES = 128;
    private static final System.Logger LOG = System.getLogger(MapFeedService.class.getName());
    private final MapSnapshotSource source;
    private final MapStateSource.Identity identity;
    private final int realmId;
    private final String datasetId;
    private final byte[] credential;
    private final BooleanSupplier ready;
    // 慢消费者最多持有一份固定读取视图；增量另有两个名额，饱和时退避并重用原游标。
    private final ThreadPoolExecutor snapshots = readers("map-snapshot-", 1);
    private final ThreadPoolExecutor changes = readers("map-changes-", 2);

    MapFeedService(MapSnapshotSource source, String secret, BooleanSupplier ready) {
        if (secret == null || !secret.matches("[A-Za-z0-9_-]{32,128}"))
            throw new IllegalArgumentException("SLG_MAP_FEED_TOKEN must be 32..128 URL-safe characters");
        this.source = Objects.requireNonNull(source);
        this.identity = Objects.requireNonNull(source.identity());
        this.realmId = identity.realmId();
        this.datasetId = identity.datasetId();
        this.credential = secret.getBytes(StandardCharsets.US_ASCII);
        this.ready = Objects.requireNonNull(ready);
    }

    /** 没有下发 AOI 专用凭证时不注册此服务。跨机持有全区视图，必须同时启用 mTLS 防止凭证被窃听。 */
    static MapFeedService configured(MapSnapshotSource source, BooleanSupplier ready,
                                     InetSocketAddress bind, Map<String, String> env) {
        String secret = env.get("SLG_MAP_FEED_TOKEN");
        if (secret == null) return null;
        if (secret.equals(env.get("SLG_TOKEN_SECRET")))
            throw new IllegalArgumentException("Map feed must use a separate service credential");
        if ((bind.isUnresolved() || !bind.getAddress().isLoopbackAddress())
                && env.getOrDefault("SLG_RPC_TLS_CA", "").isBlank())
            throw new IllegalArgumentException("Map feed on a non-loopback interface requires mutual TLS");
        // 通用服务端 builder 在指定 CA 时强制客户端证书，并校验配套服务端证书及私钥。
        return new MapFeedService(source, secret, ready);
    }

    private static ThreadPoolExecutor readers(String name, int count) {
        return new ThreadPoolExecutor(count, count, 0, TimeUnit.SECONDS, new SynchronousQueue<>(),
                Thread.ofPlatform().daemon().name(name, 0).factory());
    }

    @Override public void snapshot(MapSnapshotReq req, StreamObserver<MapSnapshotChunk> output) {
        if (!admit(req.getRealmId(), req.getDatasetId(), output)) return;
        var stream = (ServerCallStreamObserver<MapSnapshotChunk>) output;
        var call = new ReadCall(Context.current(), 60);
        stream.setOnReadyHandler(call::signal);
        stream.setOnCancelHandler(call::signal);
        submit(snapshots, output, () -> {
            var writer = new SnapshotWriter(call, stream);
            requireIdentity();
            // 本地 AOI 与远程供数消费同一公开投影。begin(H) 仅设置块水位，正常返回才可
            // 标记 complete；异常/断流时客户端丢弃全部块，不为提前取得 H 缓存整个区服。
            long high = source.snapshot(call::check, writer);
            requireIdentity();
            writer.finish(high);
        });
    }

    @Override public void changes(MapChangesReq req, StreamObserver<MapChangesPage> output) {
        if (!admit(req.getRealmId(), req.getDatasetId(), output)) return;
        if (req.getAfterSeq() < 0 || req.getLimit() < 1 || req.getLimit() > 128) {
            output.onError(Status.INVALID_ARGUMENT.withDescription("Expected after_seq >= 0 and limit in 1..128").asRuntimeException());
            return;
        }
        var call = new ReadCall(Context.current(), 5);
        submit(changes, output, () -> {
            call.check();
            requireIdentity();
            var page = source.changes(req.getAfterSeq(), req.getLimit());
            if (!identity.equals(page.identity()) || page.afterSeq() != req.getAfterSeq())
                throw new MapStateSource.BadData("Map page differs from its admitted identity or cursor");
            if (page.afterSeq() < page.prunedThrough()) throw new MapStateSource.CursorExpired(page.prunedThrough());
            var result = MapFeedWire.page(page, call::check);
            requireIdentity();
            call.check();
            output.onNext(result);
        });
    }

    private void requireIdentity() throws MapStateSource.BadData {
        if (!identity.equals(source.identity())) throw new MapStateSource.BadData("Map source identity changed");
    }

    private boolean admit(int realm, String dataset, StreamObserver<?> output) {
        String token = RpcMetadata.TOKEN.get();
        Status failure = null;
        if (token == null || !MessageDigest.isEqual(credential, token.getBytes(StandardCharsets.UTF_8)))
            failure = Status.UNAUTHENTICATED.withDescription("Map feed service credential required");
        else if (realm != realmId || !datasetId.equals(dataset))
            failure = Status.FAILED_PRECONDITION.withDescription("Realm or dataset identity mismatch");
        else if (!ready.getAsBoolean())
            failure = Status.UNAVAILABLE.withDescription("Realm is not ready");
        if (failure == null) return true;
        output.onError(failure.asRuntimeException());
        return false;
    }

    @FunctionalInterface
    private interface Read { void run() throws Exception; }

    private static void submit(ThreadPoolExecutor pool, StreamObserver<?> output, Read read) {
        try {
            pool.execute(() -> {
                try {
                    read.run();
                    output.onCompleted();
                } catch (StatusRuntimeException expected) {
                    output.onError(expected);
                } catch (MapStateSource.CursorExpired expired) {
                    output.onError(Status.OUT_OF_RANGE.withDescription("Map prefix pruned; rebuild from Snapshot").asRuntimeException());
                } catch (MapStateSource.CursorAhead ahead) {
                    output.onError(Status.FAILED_PRECONDITION.withDescription("Cursor ahead of this dataset").asRuntimeException());
                } catch (MapStateSource.BadData corrupt) {
                    LOG.log(System.Logger.Level.ERROR, "Map feed found invalid committed data", corrupt);
                    output.onError(Status.DATA_LOSS.withDescription("Committed map state or journal is invalid").asRuntimeException());
                } catch (InterruptedException cancelled) {
                    Thread.currentThread().interrupt();
                    output.onError(Status.CANCELLED.withDescription("Map read cancelled").asRuntimeException());
                } catch (Exception failed) {
                    LOG.log(System.Logger.Level.WARNING, "Map feed read failed", failed);
                    output.onError(Status.UNAVAILABLE.withDescription("Unable to read committed map state").asRuntimeException());
                }
            });
        } catch (RejectedExecutionException busy) {
            output.onError(Status.RESOURCE_EXHAUSTED.withDescription("Map feed read budget exhausted").asRuntimeException());
        }
    }

    /**
     * 有界快照封包器：条数按路线和点对象合计，字节数也合计。名字再长也不能绕过 RPC 上限。
     * 等待可写发生在独立读取线程，只会钉住本次只读快照，不持有 GAME 提交锁。
     */
    private final class SnapshotWriter implements MapSnapshotSource.SnapshotReceiver {
        private final ReadCall call;
        private final ServerCallStreamObserver<MapSnapshotChunk> output;
        private final MapSnapshotChunk.Builder chunk = MapSnapshotChunk.newBuilder().setRealmId(realmId).setDatasetId(datasetId);
        private int bytes;
        private long high = -1;
        private final int payloadBudget;

        SnapshotWriter(ReadCall call, ServerCallStreamObserver<MapSnapshotChunk> output) {
            this.call = call; this.output = output;
            payloadBudget = RpcTransport.MAX_RESPONSE_BYTES
                    - chunk.clone().setHighSeq(Long.MAX_VALUE).setComplete(true).build().getSerializedSize();
        }
        @Override public void begin(long sequence) throws IOException {
            call.check();
            if (high != -1 || sequence < 0) throw new MapStateSource.BadData("Invalid snapshot watermark declaration");
            if (payloadBudget < 0) throw new IOException("Map snapshot identity exceeds wire budget");
            high = sequence;
        }
        @Override public void route(MapStateSource.Route route) throws IOException {
            var wire = MapFeedWire.route(route);
            reserve(CodedOutputStream.computeMessageSize(MapSnapshotChunk.ROUTES_FIELD_NUMBER, wire));
            chunk.addRoutes(wire);
        }
        @Override public void object(MapStateSource.WorldObject object) throws IOException {
            var wire = MapObjectWire.encode(object);
            reserve(CodedOutputStream.computeMessageSize(MapSnapshotChunk.OBJECTS_FIELD_NUMBER, wire));
            chunk.addObjects(wire);
        }
        private void reserve(int size) throws IOException {
            call.check();
            if (high < 0) throw new MapStateSource.BadData("Snapshot item precedes watermark declaration");
            if (size > payloadBudget) throw new IOException("Map object exceeds snapshot byte budget");
            if (chunk.getRoutesCount() + chunk.getObjectsCount() == SNAPSHOT_CHUNK_ENTRIES
                    || bytes + size > payloadBudget) emit(false);
            bytes += size;
        }
        void finish(long sequence) throws IOException {
            if (high < 0 || high != sequence) throw new MapStateSource.BadData("Snapshot completion watermark changed");
            emit(true);
        }
        private void emit(boolean complete) {
            call.awaitReady(output);
            output.onNext(chunk.setHighSeq(high).setComplete(complete).build());
            chunk.clearRoutes().clearObjects();
            bytes = 0;
        }
    }

    /** gRPC 的可写和取消通知只唤醒读线程。即使对端不设 deadline，快照也有最长持有时间。 */
    private static final class ReadCall {
        private final Context context;
        private final long deadline;
        ReadCall(Context context, int seconds) {
            this.context = context;
            long budget = TimeUnit.SECONDS.toNanos(seconds);
            if (context.getDeadline() != null)
                budget = Math.min(budget, context.getDeadline().timeRemaining(TimeUnit.NANOSECONDS));
            deadline = System.nanoTime() + budget;
        }
        void check() {
            if (deadline - System.nanoTime() <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
            if (context.isCancelled() || Thread.currentThread().isInterrupted()) throw Status.CANCELLED.asRuntimeException();
        }
        synchronized void signal() { notifyAll(); }
        synchronized void awaitReady(ServerCallStreamObserver<?> stream) {
            check();
            while (!stream.isReady()) {
                if (stream.isCancelled()) throw Status.CANCELLED.asRuntimeException();
                try { TimeUnit.NANOSECONDS.timedWait(this, deadline - System.nanoTime()); }
                catch (InterruptedException stop) {
                    Thread.currentThread().interrupt();
                    throw Status.CANCELLED.asRuntimeException();
                }
                check();
            }
        }
    }

    @Override public void close() {
        snapshots.shutdownNow();
        changes.shutdownNow();
        // 中断只请求取消，不能证明 JDBC 已归还连接；组合根必须等真实读取结束再关闭数据源。
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try {
            if (!snapshots.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
                    || !changes.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new IllegalStateException("Map feed reads have not drained");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Map feed drain interrupted", interrupted);
        }
    }
}

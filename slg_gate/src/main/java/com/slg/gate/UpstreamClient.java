package com.slg.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.MessageLite;
import com.slg.core.http.HttpResult;
import com.slg.core.protocol.Json;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import io.grpc.CallOptions;
import io.grpc.ClientInterceptors;
import io.grpc.MethodDescriptor;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 网关唯一的内部调用入口：UC、GAME 业务、地图查询、会话控制各用独立 Channel，地图 Watch 使用固定池。
 * 同一目标的多个 Watch 连接分摊长期 HTTP/2 stream，池大小不随玩家即时在线数增长；
 * 查询不借 Watch 槽，池满不影响玩法和会话控制。任何写请求都不自动重试。
 * 取消或超时只结束等待，不证明远端事务未提交，重试必须保持原 requestId 和参数。
 */
final class UpstreamClient implements AutoCloseable {
    private enum Lane { UC, GAME }
    private final Channel uc, game;
    private final MapUpstreamClient maps;
    private final SessionUpstreamClient sessions;
    private final List<ManagedChannel> ownedChannels;
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("rpc-deadline").factory());
    private final ThreadPoolExecutor callbacks;
    private final Map<Lane, Budget> budgets = new EnumMap<>(Lane.class);
    private final Set<Call<?>> calls = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int timeoutMillis;

    UpstreamClient(String uc, String game) { this(uc, game, GatewaySettings.defaults()); }
    UpstreamClient(String uc, String game, GatewaySettings settings) {
        this(channels(uc, game, settings), settings);
    }
    /**
     * 仅测试夹具允许让可控制的假 Channel 共用，以观察调用次序；生产构造始终创建独立查询与 Watch 池。
     * Channel 生命周期由测试负责，不能把这个注入入口用于生产连接池配置。
     */
    UpstreamClient(Channel uc, Channel game, GatewaySettings settings) { this(uc, game, game, game, settings); }
    UpstreamClient(Channel uc, Channel game, Channel map, GatewaySettings settings) { this(uc, game, map, game, settings); }
    UpstreamClient(Channel uc, Channel game, Channel map, Channel session, GatewaySettings settings) {
        this(new Channels(uc, game, map, session, List.of(map), List.of()), settings);
    }
    private UpstreamClient(Channels channels, GatewaySettings settings) {
        this.uc = channels.uc(); this.game = channels.game(); this.ownedChannels = channels.owned();
        SessionUpstreamClient openedSessions = null;
        try {
            openedSessions = new SessionUpstreamClient(channels.session(), settings);
            maps = new MapUpstreamClient(channels.mapQuery(), channels.mapWatches(), settings);
            sessions = openedSessions;
        } catch (RuntimeException | Error failure) {
            if (openedSessions != null) openedSessions.close();
            ownedChannels.forEach(ManagedChannel::shutdownNow);
            deadlines.shutdownNow();
            throw failure;
        }
        timeoutMillis = settings.rpcTimeoutMillis();
        budgets.put(Lane.UC, new Budget(settings.ucInflight()));
        budgets.put(Lane.GAME, new Budget(settings.gameInflight()));
        deadlines.setRemoveOnCancelPolicy(true);
        int threads = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors()));
        int capacity = 2 * (settings.ucInflight() + settings.gameInflight()) + 32;
        callbacks = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(capacity), Thread.ofPlatform().daemon().name("rpc-callback-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }
    private record Channels(Channel uc, Channel game, Channel mapQuery, Channel session,
                            List<Channel> mapWatches, List<ManagedChannel> owned) { }

    /** 只在启动时建固定数量的 Channel；任何一条构造失败都回收已建连接，不留下半个池继续接流。 */
    private static Channels channels(String uc, String game, GatewaySettings settings) {
        SessionUpstreamClient.requireCapacity(settings.maxConnections());
        var owned = new ArrayList<ManagedChannel>();
        try {
            Channel accounts = openChannel(uc, owned);
            Channel commands = openChannel(game, owned);
            Channel queries = openChannel(game, owned);
            Channel sessions = openChannel(game, owned);
            var watches = new ArrayList<Channel>();
            for (int i = 0; i < settings.mapWatchChannels(); i++) watches.add(openChannel(game, owned));
            return new Channels(accounts, commands, queries, sessions, List.copyOf(watches), List.copyOf(owned));
        } catch (RuntimeException | Error failure) {
            owned.forEach(ManagedChannel::shutdownNow);
            throw failure;
        }
    }
    private static ManagedChannel openChannel(String target, List<ManagedChannel> owned) {
        ManagedChannel channel = RpcTransport.channel(target);
        owned.add(channel);
        return channel;
    }

    /** HTTP 只有账号和区服发现；在 UC 回调池内把 Protobuf 结果转换为 HTTP 的 JSON。 */
    CompletableFuture<Reply> request(String path, String method, String body, String token) {
        if ("/api/realms".equals(path) && "GET".equals(method))
            return invoke(Lane.UC, AccountRpcGrpc.getListRealmsMethod(), EmptyRequest.getDefaultInstance(), token, null, this::realmsReply);
        boolean register = "/api/auth/register".equals(path);
        if (!"POST".equals(method) || !(register || "/api/auth/login".equals(path)))
            return invalidHttp(404, "NOT_FOUND", "Endpoint not found");
        AuthRequest message;
        try {
            JsonNode auth = Json.read(body, JsonNode.class);
            if (auth == null || !auth.isObject()) throw new IllegalArgumentException();
            JsonNode username = auth.get("username");
            JsonNode password = auth.get("password");
            if (username == null || !username.isTextual() || password == null || !password.isTextual())
                throw new IllegalArgumentException();
            message = AuthRequest.newBuilder().setUsername(username.textValue()).setPassword(password.textValue()).build();
        } catch (IllegalArgumentException invalid) {
            return invalidHttp(400, "INVALID_REQUEST", "Valid request required");
        }
        if (message.getSerializedSize() > RpcTransport.MAX_REQUEST_BYTES)
            return invalidHttp(413, "PAYLOAD_TOO_LARGE", "RPC request exceeds 64 KiB");
        return invoke(Lane.UC, register ? AccountRpcGrpc.getRegisterMethod() : AccountRpcGrpc.getLoginMethod(),
                message, null, null, this::authReply);
    }

    private static CompletableFuture<Reply> invalidHttp(int status, String code, String message) {
        return CompletableFuture.completedFuture(new Reply(status, Json.write(HttpResult.fail(code, message))));
    }

    private Reply authReply(AuthReply reply) {
        if (reply.hasError()) return httpError(reply.getStatus(), reply.getError());
        if (!reply.hasSession() || (reply.getStatus() != 200 && reply.getStatus() != 201))
            throw new IllegalArgumentException("Incomplete authentication response");
        var session = reply.getSession();
        return httpReply(reply.getStatus(), HttpResult.ok(Map.of("accountId", session.getAccountId(),
                "token", session.getToken(), "expiresAt", session.getExpiresAt())));
    }

    private Reply realmsReply(RealmsReply reply) {
        if (reply.hasError()) return httpError(reply.getStatus(), reply.getError());
        if (!reply.hasDirectory() || reply.getStatus() != 200)
            throw new IllegalArgumentException("Incomplete realm directory response");
        var realms = reply.getDirectory().getRealmsList().stream().map(info -> Map.of(
                "realmId", info.getRealmId(), "name", info.getName(), "status", info.getStatus())).toList();
        return httpReply(200, HttpResult.ok(Map.of("realms", realms)));
    }

    private Reply httpError(int status, BusinessError error) {
        if (status < 400 || status > 599 || error.getCode().isBlank())
            throw new IllegalArgumentException("Invalid account service error");
        return httpReply(status, HttpResult.fail(error.getCode(), error.getMessage()));
    }

    /** 只在账号 HTTP 出口构建 JSON。玩法调用直接透传二进制回包。 */
    private Reply httpReply(int status, HttpResult<?> response) {
        String body = Json.write(response);
        if (body.getBytes(StandardCharsets.UTF_8).length > RpcTransport.MAX_RESPONSE_BYTES)
            throw new IllegalArgumentException("Account response exceeds limit");
        return new Reply(status, body);
    }

    /** 玩法正文透传给 GAME。核对两个标识及结果类型后才回给 Unity，避免把其他命令的回包当作本次成功。 */
    CompletableFuture<PlayReply> play(PlayRequest request, String token, SessionKey binding) {
        return invoke(Lane.GAME, GameRpcGrpc.getPlayMethod(), request, token, binding, reply -> {
            checkStatus(reply.getStatus());
            if (reply.getMsgId() != request.getMsgId() || !reply.getRequestId().equals(request.getRequestId())
                    || (!reply.hasBody() && !reply.hasError()) || (reply.hasError() && reply.getError().getCode().isBlank())
                    || (reply.hasBody() && reply.getStatus() != 200))
                throw new IllegalArgumentException("Invalid typed GAME reply");
            return reply;
        });
    }

    private static void checkStatus(int status) {
        if (status < 200 || status > 599) throw new IllegalArgumentException("Invalid upstream status");
    }

    /** 两条调用通道共用同一套有界准入、截止时间和取消逻辑。每次只发送一次，结果未知交由原回执判定。 */
    private <T extends MessageLite, W extends MessageLite, R> CompletableFuture<R> invoke(Lane lane, MethodDescriptor<T, W> method,
            T message, String token, SessionKey binding, Function<W, R> mapResult) {
        Budget budget = budgets.get(lane);
        if (closed.get() || !budget.permits.tryAcquire()) {
            budget.rejected.increment();
            return CompletableFuture.failedFuture(new CallFailure("UPSTREAM_BUSY", 503,
                    "Upstream capacity exhausted; request was not sent"));
        }
        Call<R> call = new Call<>(budget, lane);
        calls.add(call);
        try {
            if (closed.get()) { call.cancel(true); return call; }
            // gRPC deadline 限制传输与服务端排队；本地定时器也覆盖回调池被占满的情况。
            CallOptions options = CallOptions.DEFAULT.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .withExecutor(call::executeCallback);
            call.setDeadline(deadlines.schedule(call::timeout, timeoutMillis, TimeUnit.MILLISECONDS));
            if (call.finished.get()) return call;
            Channel channel = lane == Lane.GAME ? game : uc;
            if (token != null) {
                Metadata metadata = RpcMetadata.headers(token, binding);
                channel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
            }
            ClientCalls.asyncUnaryCall(channel.newCall(method, options), message, call.observer(mapResult));
        } catch (RuntimeException failure) {
            call.fail(new CallFailure("UPSTREAM_UNAVAILABLE_UNKNOWN", 503, call.unknownMessage()));
        }
        return call;
    }

    Map<String, Object> metrics() {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        budgets.forEach((lane, budget) -> result.put(lane.name().toLowerCase(java.util.Locale.ROOT), budget.snapshot()));
        result.put("transport", "grpc");
        result.put("callbackQueue", callbacks.getQueue().size());
        result.put("map", maps.metrics());
        result.put("session", sessions.metrics());
        return result;
    }
    /** 地图与玩法分开传输连接、在途许可、排队和回调线程，只连接同一个 GAME 地址。 */
    MapUpstreamClient maps() { return maps; }
    SessionUpstreamClient sessions() { return sessions; }
    static CallFailure failure(Throwable error) {
        Throwable root = error;
        while ((root instanceof CompletionException || root instanceof ExecutionException) && root.getCause() != null)
            root = root.getCause();
        return root instanceof CallFailure known ? known : new CallFailure("UPSTREAM_UNAVAILABLE_UNKNOWN", 503,
                "Result is unknown; retry the identical command with the same requestId");
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        maps.close();
        sessions.close();
        calls.forEach(call -> call.cancel(true));
        deadlines.shutdownNow();
        // 一次发出整个池的关闭，不按连接逐个等待；各流的 onClose 负责归还自己的传输槽。
        ownedChannels.forEach(ManagedChannel::shutdownNow);
        // 已接收的 gRPC SerializingExecutor 必须排空终态回调；不能用 shutdownNow 丢弃队列。
        // 全部 Call 已取消，observer 会跳过业务转换；后到任务由拒绝分支执行框架清理。
        callbacks.shutdown();
    }
    record Reply(int status, String body) { }
    static final class CallFailure extends RuntimeException {
        private final String code;
        private final int status;
        CallFailure(String code, int status, String message) { super(message); this.code = code; this.status = status; }
        String code() { return code; }
        int status() { return status; }
    }
    private static final class Budget {
        final int limit;
        final Semaphore permits;
        final LongAdder rejected = new LongAdder(), timeouts = new LongAdder(), completed = new LongAdder(), cancelled = new LongAdder();
        Budget(int limit) { this.limit = limit; permits = new Semaphore(limit); }
        Map<String, Object> snapshot() { return Map.of("limit", limit, "inFlight", limit - permits.availablePermits(),
                "rejected", rejected.sum(), "timeouts", timeouts.sum(), "completed", completed.sum(), "cancelled", cancelled.sum()); }
    }

    private final class Call<R> extends CompletableFuture<R> {
        final Budget budget;
        final Lane lane;
        final AtomicBoolean finished = new AtomicBoolean();
        final AtomicReference<ClientCallStreamObserver<?>> transport = new AtomicReference<>();
        final AtomicReference<ScheduledFuture<?>> deadline = new AtomicReference<>();
        Call(Budget budget, Lane lane) {
            this.budget = budget; this.lane = lane;
        }
        String unknownMessage() {
            return lane == Lane.UC ? "Account operation result is unknown; try login before repeating registration"
                    : "Result is unknown; retry the identical command with the same requestId";
        }
        void executeCallback(Runnable work) {
            try { callbacks.execute(work); }
            catch (RejectedExecutionException failure) {
                // 先终结业务等待，再排空框架任务。不能吞掉 gRPC SerializingExecutor，
                // 否则其 RUNNING 状态不会恢复，onClose 和调用清理永远无法执行。
                // observer 的 finished 检查确保这个退化路径不再做业务 JSON 转换。
                fail(new CallFailure("UPSTREAM_UNAVAILABLE_UNKNOWN", 503, unknownMessage()));
                work.run();
            }
        }
        <T, W extends MessageLite> ClientResponseObserver<T, W> observer(Function<W, R> mapResult) {
            return new ClientResponseObserver<>() {
                private W response;
                @Override public void beforeStart(ClientCallStreamObserver<T> stream) {
                    transport.set(stream);
                    if (finished.get()) stream.cancel("Caller no longer waiting", null);
                }
                @Override public void onNext(W value) { if (!finished.get()) response = value; }
                @Override public void onError(Throwable failure) {
                    if (Status.fromThrowable(failure).getCode() == Status.Code.DEADLINE_EXCEEDED) timeout();
                    else fail(new CallFailure("UPSTREAM_UNAVAILABLE_UNKNOWN", 503, unknownMessage()));
                }
                @Override public void onCompleted() {
                    if (finished.get()) return;
                    try {
                        if (response == null || response.getSerializedSize() > RpcTransport.MAX_RESPONSE_BYTES)
                            throw new IllegalArgumentException("Missing or oversized upstream response");
                        succeed(mapResult.apply(response));
                    } catch (RuntimeException invalid) {
                        fail(new CallFailure("UPSTREAM_UNAVAILABLE_UNKNOWN", 503, unknownMessage()));
                    }
                }
            };
        }
        void setDeadline(ScheduledFuture<?> value) {
            deadline.set(value);
            if (finished.get()) value.cancel(false);
        }
        boolean finish(boolean cancelTransport) {
            if (!finished.compareAndSet(false, true)) return false;
            var timer = deadline.get(); if (timer != null) timer.cancel(false);
            try {
                if (cancelTransport) {
                    var stream = transport.get(); if (stream != null) stream.cancel("Caller no longer waiting", null);
                }
            } catch (RuntimeException ignored) {
                // 传输已损坏时仍须释放本地许可，不能让清理异常改变已经选定的终态。
            } finally {
                calls.remove(this); budget.permits.release();
            }
            return true;
        }
        void succeed(R reply) {
            if (finish(false)) { budget.completed.increment(); super.complete(reply); }
        }
        void fail(CallFailure failure) { if (finish(true)) super.completeExceptionally(failure); }
        void timeout() {
            if (finish(true)) {
                budget.timeouts.increment();
                super.completeExceptionally(new CallFailure("UPSTREAM_TIMEOUT_UNKNOWN", 504, unknownMessage()));
            }
        }
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            if (!finish(true)) return false;
            budget.cancelled.increment(); return super.cancel(mayInterruptIfRunning);
        }
    }
}

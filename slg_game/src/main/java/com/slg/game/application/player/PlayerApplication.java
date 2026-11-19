package com.slg.game.application.player;

import com.slg.game.application.commit.*;
import com.slg.game.application.execution.*;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerDetails;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * 每区唯一的玩家用例门面和模块装配点；建角、行军、查询及 Session Open 共用一套执行资格。
 * 门面只做准入、不可变命令和 pin 生命周期，玩法裁决分别放在具名 handler 与纯领域规则中。
 * 不创建 DataSource、第二套邮箱或旧存储回退；新增玩法通过启动时的 PlayerUseCaseModule 注册。
 */
public final class PlayerApplication {
    public static final String CREATE = "player.create.v1";
    public static final String GET = "player.get.v1";
    public static final String RECEIPT = "player.receipt.v1";
    private static final Pattern REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final PlayerOperationContext context;
    private final BoundedOperationExecutor operations;
    private final OperationLimits limits;
    private final Set<String> writes, queries, controls;
    private final Set<String> systemWrites, systemQueries;

    public PlayerApplication(RealmIdentity realm, PlayerDirectory directory, RealmReceiptReader receipts,
                             BoundedReadExecutor reads, ReadBudget readBudget, PlayerPayloadCodec codec,
                             Predicate<PlayerAccess> access, CommitCoordinator commits, CommitPayloadSizer sizer,
                             OperationLimits limits, Map<String, QualifiedQueryHandler> controlHandlers) {
        this(realm, directory, receipts, reads, readBudget, codec, access, commits, sizer, limits, controlHandlers, List.of());
    }

    public PlayerApplication(RealmIdentity realm, PlayerDirectory directory, RealmReceiptReader receipts,
                             BoundedReadExecutor reads, ReadBudget readBudget, PlayerPayloadCodec codec,
                             Predicate<PlayerAccess> access, CommitCoordinator commits, CommitPayloadSizer sizer,
                             OperationLimits limits, Map<String, QualifiedQueryHandler> controlHandlers,
                             List<PlayerUseCaseModule> modules) {
        this.limits = Objects.requireNonNull(limits);
        context = new PlayerOperationContext(realm, directory, receipts, reads, readBudget, codec, access);
        var handlers = new HashMap<String, OperationHandler>();
        var queryHandlers = new HashMap<String, QualifiedQueryHandler>();
        var names = new HashSet<String>();
        register(handlers, Map.of(CREATE, new CreatePlayerHandler(context)), names);
        register(queryHandlers, PlayerQueries.handlers(context), names);
        var systemHandlers = new HashMap<String, OperationHandler>();
        var systemQueryHandlers = new HashMap<String, QualifiedQueryHandler>();
        for (var module : List.copyOf(modules)) {
            register(handlers, module.writeHandlers(context), names);
            register(queryHandlers, module.queryHandlers(context), names);
            register(systemHandlers, module.systemWriteHandlers(context), names);
            register(systemQueryHandlers, module.systemQueryHandlers(context), names);
        }
        writes = Set.copyOf(handlers.keySet()); queries = Set.copyOf(queryHandlers.keySet());
        systemWrites = Set.copyOf(systemHandlers.keySet()); systemQueries = Set.copyOf(systemQueryHandlers.keySet());
        handlers.putAll(systemHandlers); queryHandlers.putAll(systemQueryHandlers);
        controls = Set.copyOf(controlHandlers.keySet());
        register(queryHandlers, controlHandlers, names);
        // 全部注册校验成功后才创建执行线程，重名不会留下半套正在运行的应用资源。
        operations = new BoundedOperationExecutor(realm, commits, sizer, handlers, limits, queryHandlers);
    }

    private static <T> void register(Map<String, T> target, Map<String, ? extends T> source, Set<String> names) {
        for (var entry : source.entrySet()) {
            String name = Objects.requireNonNull(entry.getKey());
            if (name.isBlank() || !names.add(name)) throw new IllegalArgumentException("Duplicate or blank player operation");
            target.put(name, Objects.requireNonNull(entry.getValue()));
        }
    }

    /** 单区账号唯一角色；建角前及永久角色来自其他建角区时都使用同一稳定去重域。 */
    public static String scope(long accountId) {
        if (accountId <= 0) throw new IllegalArgumentException("Positive account required");
        return "account:" + accountId;
    }

    /** 不可变装配预算，供需要多阶段交接的用例在启动时核验最低容量。 */
    public OperationLimits limits() { return limits; }

    public PlayerCall<CommitResult> createPlayer(PlayerAccess actor, String requestId, String name, Duration wait) {
        return executeWrite(actor, requestId, CREATE, context.codec().encodeInvocation(new PlayerInvocation(actor, name)), Set.of(), wait);
    }
    public PlayerCall<PlayerDetails> getPlayer(PlayerAccess actor, String requestId, Duration wait) {
        return executeQuery(actor, requestId, GET, context.codec().encodeInvocation(new PlayerInvocation(actor, "")), Set.of(), wait,
                value -> context.codec().decodeDetails(value.bytes()));
    }
    public PlayerCall<RealmReceiptReader.Observation> getReceipt(PlayerAccess actor, String requestId,
                                                               String targetRequestId, Duration wait) {
        requireRequestId(targetRequestId);
        return executeQuery(actor, requestId, RECEIPT,
                context.codec().encodeInvocation(new PlayerInvocation(actor, targetRequestId)), Set.of(), wait,
                value -> context.codec().decodeObservation(value.bytes()));
    }

    /** 完整冲突范围在这里固定；handler 只校验范围，不允许读完状态后追加对象锁。 */
    public PlayerCall<CommitResult> executeWrite(PlayerAccess actor, String requestId, String type, byte[] payload,
                                                Set<ObjectKey> objects, Duration wait) {
        if (!writes.contains(type)) throw new IllegalArgumentException("Unregistered player write");
        var command = command(actor, requestId, type, payload, objects, wait);
        var pin = context.directory().pin(actor.accountId());
        try {
            var handle = operations.execute(command);
            return PlayerCalls.hold(handle.completion(), handle::cancelBeforeStart, pin, Function.identity());
        } catch (Throwable failure) { pin.close(); throw failure; }
    }

    public <T> PlayerCall<T> executeQuery(PlayerAccess actor, String requestId, String type, byte[] payload,
                                         Set<ObjectKey> objects, Duration wait, Function<OperationValue, T> decode) {
        if (!queries.contains(type)) throw new IllegalArgumentException("Unregistered player query");
        Objects.requireNonNull(decode);
        var command = command(actor, requestId, type, payload, objects, wait);
        var pin = context.directory().pin(actor.accountId());
        try {
            var handle = operations.executeQuery(command);
            return PlayerCalls.hold(handle.result(), handle::cancelBeforeStart, pin, decode);
        } catch (Throwable failure) { pin.close(); throw failure; }
    }

    /** 注册控制操作共享账号资格，但不加载资产、生成业务事务或持久会话信息。 */
    public QueryHandle executeControl(long accountId, String requestId, String type, byte[] payload, Duration wait) {
        if (!controls.contains(type)) throw new IllegalArgumentException("Unregistered player control");
        requireRequestId(requestId);
        return operations.executeQuery(new OperationCommand(new OperationScope(new RequestKey("session:" + accountId, requestId),
                Set.of(), CommitRequest.Origin.CLIENT, wait, wait, Set.of(accountId)), type, payload));
    }

    public PlayerOutcome outcome(CommitReceipt receipt) {
        Objects.requireNonNull(receipt);
        String scope = receipt.request().scope();
        long account;
        try {
            if (!scope.startsWith("account:")) throw new IllegalArgumentException();
            account = Long.parseLong(scope.substring("account:".length()));
            if (!scope.equals(scope(account))) throw new IllegalArgumentException();
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid player receipt identity");
        }
        return context.validateReceipt(receipt, receipt.request(), account);
    }
    public boolean retryRepair(long accountId, String requestId) { return operations.retryRepair(new RequestKey(scope(accountId), requestId)); }
    /** 运维恢复已知提交的系统发布隔离；只定位原请求，不重新提交业务。 */
    public boolean retryRepair(RequestKey request) { return operations.retryRepair(request); }
    public BoundedOperationExecutor.Stats stats() { return operations.stats(); }

    /** 操作及内部通知结束后才释放目录；提交器、冷读、写权和 DataSource 由组合根随后排空。 */
    public CompletionStage<Void> drain() {
        return operations.drain().thenCompose(ignored -> context.directory().drain()).toCompletableFuture().minimalCompletionStage();
    }

    private OperationCommand command(PlayerAccess actor, String id, String type, byte[] payload, Set<ObjectKey> objects, Duration wait) {
        requireRequestId(id); context.requireAccess(actor);
        var scope = new OperationScope(new RequestKey(scope(actor.accountId()), id), objects, CommitRequest.Origin.CLIENT,
                wait, wait, Set.of(actor.accountId()));
        return new OperationCommand(scope, type, payload);
    }

    /** 后台写沿用同一账号/世界资格与 pin；来源和白名单独立于客户端，不接受伪造在线身份。 */
    public PlayerCall<CommitResult> executeSystemWrite(long account, RequestKey request, CommitRequest.Origin origin,
                                                      String type, byte[] payload, Set<ObjectKey> objects, Duration wait) {
        if (!systemWrites.contains(type)) throw new IllegalArgumentException("Unregistered system write");
        var command = systemCommand(account, request, origin, type, payload, objects, wait);
        var pin = context.directory().pin(account);
        try {
            var handle = operations.execute(command);
            return PlayerCalls.hold(handle.completion(), handle::cancelBeforeStart, pin, Function.identity());
        } catch (Throwable failure) { pin.close(); throw failure; }
    }

    public <T> PlayerCall<T> executeSystemQuery(long account, RequestKey request, CommitRequest.Origin origin,
                                               String type, byte[] payload, Set<ObjectKey> objects, Duration wait,
                                               Function<OperationValue, T> decode) {
        if (!systemQueries.contains(type)) throw new IllegalArgumentException("Unregistered system query");
        Objects.requireNonNull(decode);
        var command = systemCommand(account, request, origin, type, payload, objects, wait);
        var pin = context.directory().pin(account);
        try {
            var handle = operations.executeQuery(command);
            return PlayerCalls.hold(handle.result(), handle::cancelBeforeStart, pin, decode);
        } catch (Throwable failure) { pin.close(); throw failure; }
    }

    private static OperationCommand systemCommand(long account, RequestKey request, CommitRequest.Origin origin,
                                                    String type, byte[] payload, Set<ObjectKey> objects, Duration wait) {
        Objects.requireNonNull(request);
        if (origin != CommitRequest.Origin.DUE_TASK && origin != CommitRequest.Origin.BATTLE_RESULT)
            throw new IllegalArgumentException("Background origin must be due task or battle result");
        if (request.scope().startsWith("account:") || request.scope().startsWith("session:"))
            throw new IllegalArgumentException("Background requests cannot use client or session receipt scopes");
        return new OperationCommand(new OperationScope(request, objects, origin, wait, wait, Set.of(account)), type, payload);
    }
    static void requireRequestId(String id) {
        if (id == null || !REQUEST_ID.matcher(id).matches()) throw new GameException("INVALID_ARGUMENT", "Valid requestId required");
    }
}

package com.slg.game.application.execution;

import com.slg.game.application.commit.*;
import com.slg.game.application.port.CommitPayloadSizer;
import com.slg.game.domain.persistence.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 全范围资格 → 准备 → 不可变提交 → 发布/重载的有界用例执行器。
 * 每个账号、对象和请求均有 FIFO 资格队列：活动项一直留在队头，异步等待只释放线程、不释放资格。
 * 四种来源轮转选择互不冲突的任务；后到任务不能越过任一共同对象上的早到任务。
 *
 * <p>排队只保存 OperationCommand；处理器在装配时注册，不能把活跃实体/SQL Lambda 塞入任务。
 * 总额度包括故障修复和通知积压，通知另用有限线程，客户端取消派生 Future 不改变内部生命周期。
 * publish/reload 失败不伪造回滚；未修复或 UNKNOWN 仍在时 drain 保持未完成。</p>
 *
 * <p>组合根必须为每个 RealmIdentity 只装配一个本执行器；CLIENT、定时任务、战果、发货和修复
 * 都共享它。另建同区实例会绕过进程内资格，因此不得按入口/来源各自创建调度器。</p>
 */
public final class BoundedOperationExecutor {
    private final Object gate = new Object();
    private final RealmIdentity realm;
    private final CommitCoordinator commits;
    private final CommitPayloadSizer sizer;
    private final Map<String, OperationHandler> handlers;
    private final Map<String, QualifiedQueryHandler> queries;
    private final Map<String, Integer> queryResultBudgets;
    private final OperationLimits limits;
    private final ThreadPoolExecutor preparations;
    private final ThreadPoolExecutor publications;
    private final ThreadPoolExecutor notifications;
    private final ScheduledExecutorService clock;
    private final Set<Task> tasks = new HashSet<>();
    private final Map<ObjectKey, Deque<Task>> objectQueues = new HashMap<>();
    private final Map<Long, Deque<Task>> accountQueues = new HashMap<>();
    private final Map<RequestKey, Deque<Task>> requestQueues = new HashMap<>();
    private final EnumMap<CommitRequest.Origin, Deque<Task>> pending = new EnumMap<>(CommitRequest.Origin.class);
    private final Deque<Runnable> continuingPreparations = new ArrayDeque<>();
    private final Deque<Runnable> publishing = new ArrayDeque<>();
    private final Deque<Task> notifying = new ArrayDeque<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final CompletionStage<Void> drainView = drained.minimalCompletionStage();
    private long bytes;
    private long clientBytes;
    private int clients;
    private int preparing;
    private int publishingCount;
    private int notifyingCount;
    private int nextOrigin;
    private boolean stopping;
    private boolean hardIsolation;
    private boolean drainScheduled;

    public BoundedOperationExecutor(RealmIdentity realm, CommitCoordinator commits, CommitPayloadSizer sizer,
                                    Map<String, OperationHandler> handlers, OperationLimits limits) {
        this(realm, commits, sizer, handlers, limits, Map.of());
    }

    public BoundedOperationExecutor(RealmIdentity realm, CommitCoordinator commits, CommitPayloadSizer sizer,
                                    Map<String, OperationHandler> handlers, OperationLimits limits,
                                    Map<String, QualifiedQueryHandler> queries) {
        this.realm = Objects.requireNonNull(realm, "realm");
        this.commits = Objects.requireNonNull(commits, "commits");
        this.sizer = Objects.requireNonNull(sizer, "sizer");
        this.handlers = Map.copyOf(Objects.requireNonNull(handlers, "handlers"));
        this.queries = Map.copyOf(Objects.requireNonNull(queries, "queries"));
        if (this.handlers.isEmpty() && this.queries.isEmpty()) throw new IllegalArgumentException("At least one handler is required");
        var resultBudgets = new HashMap<String, Integer>();
        this.queries.forEach((type, handler) -> {
            int maximum = handler.maxResultBytes();
            if (maximum < 0) throw new IllegalArgumentException("Query result budget must not be negative");
            resultBudgets.put(type, maximum);
        });
        queryResultBudgets = Map.copyOf(resultBudgets);
        this.limits = Objects.requireNonNull(limits, "limits");
        for (var origin : CommitRequest.Origin.values()) pending.put(origin, new ArrayDeque<>());
        preparations = pool(limits.preparationThreads(), "operation-prepare");
        publications = pool(limits.publicationThreads(), "operation-publish");
        notifications = pool(limits.notificationThreads(), "operation-notify");
        clock = Executors.newSingleThreadScheduledExecutor(threads("operation-clock"));
        // 一个固定检查器处理全部等待预算，不为每个操作创建一个长期 timer/通知对象。
        clock.scheduleWithFixedDelay(this::tick, 10, 10, TimeUnit.MILLISECONDS);
    }

    /** 不等待资格、队列腾位或数据库；正常满额/停写返回确定拒绝，绝不在调用线程执行用例。 */
    public OperationHandle execute(OperationCommand command) {
        Objects.requireNonNull(command, "command");
        if (!handlers.containsKey(command.type())) throw new IllegalArgumentException("Unregistered operation type");
        try { return admit(command, false); }
        catch (QueryRejectedException rejection) { return rejected(command, rejection.reason()); }
    }

    /** 只读/会话控制共享写操作的资格与容量，但不构造决定、不调用提交器、不生成伪造回执。 */
    public QueryHandle executeQuery(OperationCommand command) {
        Objects.requireNonNull(command, "command");
        if (!queries.containsKey(command.type())) throw new IllegalArgumentException("Unregistered query type");
        try { return admit(command, true); }
        catch (QueryRejectedException rejection) { return rejectedQuery(command, rejection); }
    }

    private Task admit(OperationCommand command, boolean query) {
        synchronized (gate) {
            if (stopping) throw new QueryRejectedException(CommitResult.Reason.STOPPING);
            if (hardIsolation || availableWriter().isEmpty()) throw new QueryRejectedException(CommitResult.Reason.ISOLATED);
            if ((long) command.scope().objects().size() + command.scope().accounts().size() > limits.maxKeysPerOperation())
                throw new QueryRejectedException(CommitResult.Reason.QUEUE_FULL);
            int resultBudget = query ? queryResultBudgets.get(command.type()) : 0;
            // 结果尚未产生就预留最大值，避免多个查询同时返回后突破内存边界；通知结束才归还额度。
            long charge = command.retainedBytes() + (query ? 32L + resultBudget : 0);
            boolean client = command.scope().origin() == CommitRequest.Origin.CLIENT;
            if (tasks.size() == limits.maxOperations() || !fits(charge, client)
                    || client && clients >= limits.maxOperations() - limits.systemReservedOperations())
                throw new QueryRejectedException(CommitResult.Reason.QUEUE_FULL);
            var task = new Task(command, charge, query, resultBudget);
            tasks.add(task);
            bytes += charge;
            if (client) { clients++; clientBytes += charge; }
            for (ObjectKey key : command.scope().objects()) objectQueues.computeIfAbsent(key, ignored -> new ArrayDeque<>()).addLast(task);
            for (Long account : command.scope().accounts()) accountQueues.computeIfAbsent(account, ignored -> new ArrayDeque<>()).addLast(task);
            requestQueues.computeIfAbsent(command.scope().request(), ignored -> new ArrayDeque<>()).addLast(task);
            pending.get(command.scope().origin()).addLast(task);
            if (command.scope().waitBudget().isZero()) {
                if (preparing < limits.preparationThreads() && eligible(task)) startLocked(task);
                else settleLocked(task, new CommitResult.NotCommitted(CommitResult.Reason.QUEUE_TIMEOUT));
            }
            pumpLocked();
            return task;
        }
    }

    /** 只重试已知存储结果的失效/重载（包括确定拒绝后的 discard）；不能跳过 UNKNOWN 核对。 */
    public boolean retryRepair(RequestKey request) {
        Objects.requireNonNull(request, "request");
        synchronized (gate) {
            var queue = requestQueues.get(request);
            Task task = queue == null ? null : queue.peekFirst();
            if (task == null || task.phase != OperationPhase.ISOLATED || task.result == null
                    || task.barrier == null || hardIsolation) return false;
            task.reload = true;
            task.phase = OperationPhase.REPAIRING;
            publishing.addLast(() -> publish(task));
            pumpLocked();
            return true;
        }
    }

    /**
     * 停止新操作，取消尚未取得资格的等待项；已准备任务仍需提交收敛及发布。
     * 组合根应先等待本 drain，再 drain 提交器，最后关闭存储资源，不能颠倒顺序截断准备中的业务。
     */
    public CompletionStage<Void> drain() {
        synchronized (gate) {
            stopping = true;
            rejectWaitingLocked(CommitResult.Reason.STOPPING);
            pumpLocked();
            return drainView;
        }
    }

    public Stats stats() {
        synchronized (gate) {
            int waiting = 0, committing = 0, isolated = 0, preparingTasks = 0, publishingTasks = 0;
            for (Task task : tasks) {
                if (task.phase == OperationPhase.WAITING) waiting++;
                if (task.phase == OperationPhase.COMMITTING) committing++;
                if (task.phase == OperationPhase.ISOLATED) isolated++;
                if (task.phase == OperationPhase.PREPARING) preparingTasks++;
                if (task.phase == OperationPhase.PUBLISHING || task.phase == OperationPhase.REPAIRING) publishingTasks++;
            }
            return new Stats(tasks.size(), bytes, waiting, preparingTasks, committing, publishingTasks,
                    isolated, notifying.size() + notifyingCount, stopping, hardIsolation);
        }
    }

    public record Stats(int operations, long retainedBytes, int waiting, int preparing, int committing,
                        int publishing, int isolated, int notifications, boolean stopping, boolean hardIsolation) {}

    private Optional<WriterToken> availableWriter() {
        var writer = commits.currentWriter();
        return writer.filter(token -> token.realm().equals(realm));
    }
    private boolean fits(long additional, boolean client) {
        return additional > 0 && additional <= limits.maxRetainedBytes() - bytes
                && (!client || additional <= limits.maxRetainedBytes() - limits.systemReservedBytes() - clientBytes);
    }
    private boolean eligible(Task task) {
        if (requestQueues.get(task.requestKey()).peekFirst() != task) return false;
        for (ObjectKey key : task.command.scope().objects()) if (objectQueues.get(key).peekFirst() != task) return false;
        for (Long account : task.command.scope().accounts()) if (accountQueues.get(account).peekFirst() != task) return false;
        return true;
    }

    private void pumpLocked() {
        expireWaitingLocked(System.nanoTime());
        // 外部 Future 的回调只入内部有界续接队列，不在 JDBC/调用方线程执行校验、提交、发布或用户代码。
        while (preparing < limits.preparationThreads() && !continuingPreparations.isEmpty())
            dispatchPreparationLocked(continuingPreparations.removeFirst());
        if (!stopping && !hardIsolation) {
            if (availableWriter().isEmpty()) rejectWaitingLocked(CommitResult.Reason.ISOLATED);
            else while (preparing < limits.preparationThreads()) {
                Task next = nextEligibleLocked();
                if (next == null) break;
                startLocked(next);
            }
        }
        while (publishingCount < limits.publicationThreads() && !publishing.isEmpty()) {
            Runnable step = publishing.removeFirst();
            publishingCount++;
            publications.execute(() -> {
                try { step.run(); }
                finally { synchronized (gate) { publishingCount--; pumpLocked(); } }
            });
        }
        while (notifyingCount < limits.notificationThreads() && !notifying.isEmpty()) {
            Task task = notifying.removeFirst();
            notifyingCount++;
            notifications.execute(() -> notifyResult(task));
        }
        if (stopping && tasks.isEmpty() && !drainScheduled) {
            drainScheduled = true;
            clock.shutdown();
            preparations.shutdown();
            publications.shutdown();
            // 完成公开阶段可能触发调用方回调，只在通知线程完成，不在 gate 下运行用户代码。
            notifications.execute(() -> { drained.complete(null); notifications.shutdown(); });
        }
    }

    private Task nextEligibleLocked() {
        var origins = CommitRequest.Origin.values();
        for (int i = 0; i < origins.length; i++) {
            int index = (nextOrigin + i) % origins.length;
            for (Task task : pending.get(origins[index])) if (eligible(task)) {
                nextOrigin = (index + 1) % origins.length;
                return task;
            }
        }
        return null;
    }

    private void startLocked(Task task) {
        pending.get(task.command.scope().origin()).remove(task);
        task.phase = OperationPhase.PREPARING;
        dispatchPreparationLocked(() -> prepare(task));
    }

    private void dispatchPreparationLocked(Runnable step) {
        preparing++;
        preparations.execute(() -> {
            try { step.run(); }
            finally { synchronized (gate) { preparing--; pumpLocked(); } }
        });
    }

    private void continuePreparation(Runnable step) {
        // 每个已准入任务最多等待一个阶段回调，续接数量因此受 maxOperations 限制；不再创建线程池任务链。
        synchronized (gate) { continuingPreparations.addLast(step); pumpLocked(); }
    }

    private void continuePublication(Runnable step) {
        synchronized (gate) { publishing.addLast(step); pumpLocked(); }
    }

    private void prepare(Task task) {
        try {
            synchronized (gate) {
                if (hardIsolation) { settleLocked(task, new CommitResult.NotCommitted(CommitResult.Reason.ISOLATED)); return; }
            }
            var writer = availableWriter();
            if (writer.isEmpty()) { settle(task, new CommitResult.NotCommitted(CommitResult.Reason.ISOLATED)); return; }
            task.initialWriter = writer.get();
            if (task.query) {
                var stage = Objects.requireNonNull(queries.get(task.command.type()).execute(task.command), "query stage");
                stage.whenComplete((value, failure) -> continuePreparation(() -> queryCompleted(task, value, failure)));
            } else {
                var stage = Objects.requireNonNull(handlers.get(task.command.type()).prepareAsync(task.command), "preparation stage");
                stage.whenComplete((decision, failure) -> continuePreparation(() -> prepared(task, decision, failure)));
            }
        } catch (Throwable failure) {
            if (task.query) settleQuery(task, null, failure);
            else settle(task, preparationRejection(failure));
        }
    }

    private void prepared(Task task, CommitDecision decision, Throwable preparationFailure) {
        try {
            if (preparationFailure != null) {
                settle(task, preparationRejection(preparationFailure));
                return;
            }
            Objects.requireNonNull(decision, "decision");
            task.command.scope().validate(decision);
            task.preparationSucceeded = true;
            var request = new CommitRequest(task.initialWriter, decision, task.command.scope().origin(), task.command.scope().commitQueueBudget());
            long additional = sizer.measure(request);
            synchronized (gate) {
                if (hardIsolation) { settleLocked(task, new CommitResult.NotCommitted(CommitResult.Reason.ISOLATED)); return; }
                if (!fits(additional, task.client)) {
                    settleLocked(task, new CommitResult.NotCommitted(CommitResult.Reason.QUEUE_FULL));
                    return;
                }
                bytes += additional;
                if (task.client) clientBytes += additional;
                task.charge += additional;
                task.prepared = request;
            }
            // token 在准备期间改变时拒绝旧草案；绝不换成新 epoch 盲重投已经裁决的请求。
            if (availableWriter().filter(task.initialWriter::equals).isEmpty()) {
                settle(task, new CommitResult.NotCommitted(CommitResult.Reason.OWNER_LOST));
                return;
            }
            synchronized (gate) { task.phase = OperationPhase.COMMITTING; task.submitStarted = true; }
            CommitHandle handle = commits.submit(request);
            handle.settlement().whenComplete((result, failure) -> continuePreparation(() -> settled(task, result, failure)));
        } catch (Throwable failure) {
            // 进入 submit 之前明确没有数据库尝试；之后抛错不能猜成未提交，必须隔离并保留资格。
            if (task.submitStarted) isolateUnknown(task);
            else settle(task, new CommitResult.NotCommitted(CommitResult.Reason.INVALID_DECISION));
        }
    }

    private static CommitResult.NotCommitted preparationRejection(Throwable failure) {
        // 只有应用明确识别的规划过期才允许原 ID 重新规划；其它准备错误不能伪造业务回执。
        while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null)
            failure = failure.getCause();
        return new CommitResult.NotCommitted(failure instanceof OperationStateChangedException
                ? CommitResult.Reason.VERSION_CONFLICT : CommitResult.Reason.INVALID_DECISION);
    }

    private void queryCompleted(Task task, OperationValue value, Throwable failure) {
        if (failure != null) { settleQuery(task, null, failure); return; }
        if (value == null || value.size() > task.resultBudget) {
            settleQuery(task, null, new IllegalArgumentException("Query result exceeds its registered byte budget"));
            return;
        }
        synchronized (gate) {
            if (hardIsolation || availableWriter().isEmpty()) {
                settleLocked(task, new CommitResult.NotCommitted(CommitResult.Reason.ISOLATED));
                pumpLocked();
                return;
            }
        }
        if (availableWriter().filter(task.initialWriter::equals).isEmpty()) {
            settle(task, new CommitResult.NotCommitted(CommitResult.Reason.OWNER_LOST));
            return;
        }
        settleQuery(task, value, null);
    }

    private void settled(Task task, CommitResult result, Throwable failure) {
        if (failure != null || result == null) { isolateUnknown(task); return; }
        synchronized (gate) {
            if (task.phase != OperationPhase.COMMITTING) return;
            task.result = result;
            if (result instanceof CommitResult.NotCommitted) settleLocked(task, result);
            else {
                var committed = (CommitResult.Committed) result;
                boolean receiptMatches;
                try { task.command.scope().validateReceipt(task.prepared.decision(), committed.receipt()); receiptMatches = true; }
                catch (IllegalArgumentException mismatch) { receiptMatches = false; }
                if (!receiptMatches) {
                    task.phase = OperationPhase.ISOLATED;
                    task.result = null;
                    hardIsolation = true;
                } else {
                    task.reload = !committed.applied();
                    task.phase = task.reload ? OperationPhase.REPAIRING : OperationPhase.PUBLISHING;
                    publishing.addLast(() -> publish(task));
                }
            }
            pumpLocked();
        }
        if (task.phase == OperationPhase.ISOLATED) ensureBarrier(task);
    }

    private void publish(Task task) {
        try {
            var handler = handlers.get(task.command.type());
            CompletionStage<Void> stage;
            if (task.discard) stage = handler.discardAsync(task.command, (CommitResult.NotCommitted) task.result);
            else {
                var committed = (CommitResult.Committed) task.result;
                stage = task.reload ? handler.reloadAsync(task.command, task.prepared.decision(), committed)
                        : handler.publishAsync(task.command, task.prepared.decision(), committed);
            }
            Objects.requireNonNull(stage, "publication stage")
                    .whenComplete((ignored, failure) -> continuePublication(() -> published(task, failure)));
        } catch (Throwable failure) { publicationFailed(task); }
    }

    private void published(Task task, Throwable failure) {
        if (failure != null) { publicationFailed(task); return; }
        try {
            if (task.barrier != null) {
                try { task.barrier.releaseAfterReload(); }
                catch (Throwable releaseFailure) {
                    // 释放确认异常不能证明屏障仍在；立即停止本入口，并尝试建立新的独立隔离。
                    synchronized (gate) { hardIsolation = true; task.barrier = null; }
                    ensureBarrier(task);
                    throw releaseFailure;
                }
            }
            synchronized (gate) {
                task.discardCompleted = task.discard;
                settleLocked(task, task.result);
                pumpLocked();
            }
        } catch (Throwable releaseFailure) {
            ensureBarrier(task);
            synchronized (gate) { task.phase = OperationPhase.ISOLATED; pumpLocked(); }
        }
    }

    private void publicationFailed(Task task) {
        boolean barrierEstablished = ensureBarrier(task);
        synchronized (gate) {
            if (!task.reload && !task.discard && barrierEstablished) {
                task.reload = true;
                task.phase = OperationPhase.REPAIRING;
                publishing.addLast(() -> publish(task));
            } else task.phase = OperationPhase.ISOLATED;
            pumpLocked();
        }
    }

    private boolean ensureBarrier(Task task) {
        synchronized (gate) { if (task.barrier != null) return true; }
        try {
            var barrier = Objects.requireNonNull(commits.isolatePublication(), "publication barrier");
            synchronized (gate) { task.barrier = barrier; }
            return true;
        } catch (Throwable failure) {
            synchronized (gate) { hardIsolation = true; }
            return false;
        }
    }

    private void isolateUnknown(Task task) {
        ensureBarrier(task);
        synchronized (gate) { task.phase = OperationPhase.ISOLATED; pumpLocked(); }
    }

    private void settle(Task task, CommitResult result) {
        synchronized (gate) { settleLocked(task, result); pumpLocked(); }
    }

    private void settleLocked(Task task, CommitResult result) {
        if (task.phase == OperationPhase.SETTLED) return;
        if (task.query) {
            settleQueryLocked(task, null, new QueryRejectedException(((CommitResult.NotCommitted) result).reason()));
            return;
        }
        task.result = result;
        if (result instanceof CommitResult.NotCommitted && task.preparationSucceeded && !task.discardCompleted) {
            task.discard = true;
            task.phase = OperationPhase.REPAIRING;
            publishing.addLast(() -> publish(task));
            return;
        }
        releaseQualificationLocked(task);
    }

    private void settleQuery(Task task, OperationValue value, Throwable failure) {
        synchronized (gate) { settleQueryLocked(task, value, failure); pumpLocked(); }
    }

    private void settleQueryLocked(Task task, OperationValue value, Throwable failure) {
        if (task.phase == OperationPhase.SETTLED) return;
        task.queryValue = value;
        task.queryFailure = failure;
        releaseQualificationLocked(task);
    }

    private void releaseQualificationLocked(Task task) {
        pending.get(task.command.scope().origin()).remove(task);
        for (ObjectKey key : task.command.scope().objects()) remove(objectQueues, key, task);
        for (Long account : task.command.scope().accounts()) remove(accountQueues, account, task);
        remove(requestQueues, task.requestKey(), task);
        task.phase = OperationPhase.SETTLED;
        notifying.addLast(task);
    }

    private static <K> void remove(Map<K, Deque<Task>> queues, K key, Task task) {
        Deque<Task> queue = queues.get(key);
        queue.remove(task);
        if (queue.isEmpty()) queues.remove(key);
    }

    private void notifyResult(Task task) {
        try {
            if (!task.query) task.completion.complete(task.result);
            else if (task.queryFailure == null) task.queryCompletion.complete(task.queryValue);
            else task.queryCompletion.completeExceptionally(task.queryFailure);
        }
        finally {
            synchronized (gate) {
                notifyingCount--;
                tasks.remove(task);
                bytes -= task.charge;
                if (task.client) { clients--; clientBytes -= task.charge; }
                task.prepared = null;
                task.command = null;
                task.barrier = null;
                task.queryValue = null;
                task.queryFailure = null;
                pumpLocked();
            }
        }
    }

    private void rejectWaitingLocked(CommitResult.Reason reason) {
        for (Task task : List.copyOf(tasks)) if (task.phase == OperationPhase.WAITING)
            settleLocked(task, new CommitResult.NotCommitted(reason));
    }

    private void tick() {
        synchronized (gate) {
            if (drainScheduled) return;
            try { pumpLocked(); }
            catch (Throwable failure) {
                hardIsolation = true;
                rejectWaitingLocked(CommitResult.Reason.ISOLATED);
                pumpLocked();
            }
        }
    }

    private void expireWaitingLocked(long now) {
        for (Task task : List.copyOf(tasks)) if (task.phase == OperationPhase.WAITING
                && now - task.admittedAt >= task.waitNanos)
            settleLocked(task, new CommitResult.NotCommitted(CommitResult.Reason.QUEUE_TIMEOUT));
    }

    private ThreadPoolExecutor pool(int count, String name) {
        return new ThreadPoolExecutor(count, count, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(limits.maxOperations()), threads(name), new ThreadPoolExecutor.AbortPolicy());
    }
    private static ThreadFactory threads(String name) {
        var sequence = new AtomicInteger();
        return action -> { var thread = new Thread(action, name + "-" + sequence.incrementAndGet()); thread.setDaemon(true); return thread; };
    }
    private static long nanos(Duration duration) {
        try { return duration.toNanos(); } catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
    }

    private static OperationHandle rejected(OperationCommand command, CommitResult.Reason reason) {
        RequestKey request = command.scope().request();
        CompletionStage<CommitResult> result = CompletableFuture.<CommitResult>completedFuture(new CommitResult.NotCommitted(reason)).minimalCompletionStage();
        return new OperationHandle() {
            @Override public RequestKey requestKey() { return request; }
            @Override public OperationPhase phase() { return OperationPhase.SETTLED; }
            @Override public CompletionStage<CommitResult> completion() { return result; }
            @Override public boolean cancelBeforeStart() { return false; }
        };
    }

    private static QueryHandle rejectedQuery(OperationCommand command, QueryRejectedException failure) {
        RequestKey request = command.scope().request();
        CompletionStage<OperationValue> result = CompletableFuture.<OperationValue>failedFuture(failure).minimalCompletionStage();
        return new QueryHandle() {
            @Override public RequestKey requestKey() { return request; }
            @Override public OperationPhase phase() { return OperationPhase.SETTLED; }
            @Override public CompletionStage<OperationValue> result() { return result; }
            @Override public boolean cancelBeforeStart() { return false; }
        };
    }

    private final class Task implements OperationHandle, QueryHandle {
        OperationCommand command;
        final RequestKey request;
        final boolean client;
        final boolean query;
        final int resultBudget;
        final long admittedAt = System.nanoTime();
        final long waitNanos;
        final CompletableFuture<CommitResult> completion = new CompletableFuture<>();
        final CompletionStage<CommitResult> view = completion.minimalCompletionStage();
        final CompletableFuture<OperationValue> queryCompletion = new CompletableFuture<>();
        final CompletionStage<OperationValue> queryView = queryCompletion.minimalCompletionStage();
        volatile OperationPhase phase = OperationPhase.WAITING;
        volatile boolean submitStarted;
        CommitRequest prepared;
        WriterToken initialWriter;
        CommitResult result;
        OperationValue queryValue;
        Throwable queryFailure;
        CommitCoordinator.PublicationBarrier barrier;
        boolean reload;
        boolean preparationSucceeded;
        boolean discard;
        boolean discardCompleted;
        long charge;

        Task(OperationCommand command, long charge, boolean query, int resultBudget) {
            this.command = command;
            request = command.scope().request();
            this.charge = charge;
            this.query = query;
            this.resultBudget = resultBudget;
            client = command.scope().origin() == CommitRequest.Origin.CLIENT;
            waitNanos = Math.min(nanos(command.scope().waitBudget()), limits.maxWait().toNanos());
        }
        @Override public RequestKey requestKey() { return request; }
        @Override public OperationPhase phase() { return phase; }
        @Override public CompletionStage<CommitResult> completion() { return view; }
        @Override public CompletionStage<OperationValue> result() { return queryView; }
        @Override public boolean cancelBeforeStart() {
            synchronized (gate) {
                if (phase != OperationPhase.WAITING) return false;
                settleLocked(this, new CommitResult.NotCommitted(CommitResult.Reason.CANCELLED));
                pumpLocked();
                return true;
            }
        }
    }
}

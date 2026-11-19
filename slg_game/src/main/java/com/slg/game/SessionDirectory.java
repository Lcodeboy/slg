package com.slg.game;

import com.slg.core.auth.AccountIds;
import com.slg.core.auth.SessionClaims;
import com.slg.rpc.v1.SessionBinding;
import com.slg.rpc.v1.SessionKey;
import com.slg.rpc.v1.SessionOpenRequest;
import com.slg.rpc.v1.SessionPreparation;
import io.grpc.Status;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 一个 GAME 实例的连接执行资格。UC token 是可复用的登录凭证，当前绑定才是这条 TCP 的会话。
 * 绑定不包含资产，也不写业务日志；GAME 重启产生全新 instance，旧 proof 无条件失效，客户端
 * 重新认证后仍通过原业务 UUID 找回权威存储回执。不能靠把 generation 从 1 重置来识别重启。
 *
 * 短 monitor 仅保护有界内存表，没有数据库/网络调用。绑定更新必须由 SessionRpcService 放进
 * 唯一 PlayerApplication 的同账号资格，与所有玩法及纯读共享，直到提交收敛和发布结束。
 * 本类的锁只保护连接目录，不能替代业务调度或提前释放仍在核对中的账号资格。
 */
final class SessionDirectory implements AutoCloseable {
    static final int LEASE_MILLIS = 30_000;
    static final int MAX_CHECK_BATCH = 256;
    private static final int MAX_PREPARATIONS = 4096;
    private static final long LEASE_NANOS = TimeUnit.MILLISECONDS.toNanos(LEASE_MILLIS);
    private static final long PREPARATION_NANOS = TimeUnit.SECONDS.toNanos(5);
    private final String instance = UUID.randomUUID().toString();
    private final Clock clock;
    private final LongSupplier nanoTime;
    private final int maximum;
    private final Map<String, Entry> entries = new HashMap<>();
    private final Map<String, Preparation> preparations = new HashMap<>();
    private final Map<String, Set<String>> preparationAccounts = new HashMap<>();
    private final ScheduledExecutorService expiry = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("game-session-expiry").factory());
    private long generation, opened, replaced, renewed, expired, released;
    private boolean closed;

    private record Preparation(SessionClaims claims, long expectedGeneration, long deadline) { }

    private static final class Entry {
        final SessionClaims claims;
        final SessionKey key;
        final String attempt;
        final long expectedGeneration;
        final String ticket;
        long leaseDeadline;
        Entry(SessionClaims claims, SessionKey key, String attempt, long expectedGeneration, String ticket, long leaseDeadline) {
            this.claims = claims; this.key = key; this.attempt = attempt;
            this.expectedGeneration = expectedGeneration; this.ticket = ticket; this.leaseDeadline = leaseDeadline;
        }
    }

    SessionDirectory(Clock clock, int maximum) { this(clock, maximum, System::nanoTime); }

    SessionDirectory(Clock clock, int maximum, LongSupplier nanoTime) {
        this.clock = Objects.requireNonNull(clock);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        if (maximum < 1 || maximum > 1_000_000) throw new IllegalArgumentException("Invalid session capacity");
        this.maximum = maximum;
        // 全表本身受 maximum 限制；无需为每玩家安排定时器或新增长期 RPC stream。
        expiry.scheduleWithFixedDelay(this::removeExpired, 1, 1, TimeUnit.SECONDS);
    }

    synchronized SessionPreparation prepare(SessionClaims claims) {
        requireOpen();
        requireClaims(claims);
        Entry current = current(claims.accountId());
        if (preparations.size() >= MAX_PREPARATIONS)
            throw Status.RESOURCE_EXHAUSTED.withDescription("Session preparation capacity exhausted").asRuntimeException();
        long observed = current == null ? 0 : current.key.getGeneration();
        String ticket = UUID.randomUUID().toString();
        preparations.put(ticket, new Preparation(claims, observed, nanoTime.getAsLong() + PREPARATION_NANOS));
        preparationAccounts.computeIfAbsent(claims.accountId(), ignored -> new HashSet<>()).add(ticket);
        return SessionPreparation.newBuilder().setGameInstance(instance)
                .setObservedGeneration(observed).setTicketId(ticket).build();
    }

    /**
     * CAS 是防止迟到认证覆盖新连接的边界。先 Prepare 后排队等待期间，别的连接可能已经成功：
     * 此时必须让旧 Open 失败，不能自动拿新 observedGeneration 再试。Gateway 应让用户重新连接。
     * 同 attempt 的重试只在该绑定仍是当前项时返回原值；它既不分配新代次，也不续租。
     */
    synchronized SessionBinding open(SessionClaims claims, SessionOpenRequest request) {
        requireOpen();
        requireClaims(claims);
        if (!canonicalUuid(request.getAttemptId()) || !canonicalUuid(request.getTicketId()) || request.getExpectedGeneration() < 0)
            throw Status.INVALID_ARGUMENT.withDescription("Invalid session binding request").asRuntimeException();
        if (!instance.equals(request.getGameInstance())) throw changed();
        Entry current = current(claims.accountId());
        if (current != null && current.attempt.equals(request.getAttemptId())) {
            if (!current.claims.equals(claims) || current.expectedGeneration != request.getExpectedGeneration()
                    || !current.ticket.equals(request.getTicketId())) throw changed();
            return binding(current);
        }
        // 只比较 generation 会遇到空槽 ABA：0→绑定→替换→关闭→0。一次性准备票据在 Open
        // 时消耗，哪怕当前账号已无连接，迟到的旧认证也不能重新分配代次。票据独立有界并在五秒过期。
        Preparation prepared = takePreparation(request.getTicketId());
        if (prepared == null || !prepared.claims().equals(claims)
                || prepared.expectedGeneration() != request.getExpectedGeneration()
                || nanoTime.getAsLong() - prepared.deadline() >= 0) throw changed();
        long observed = current == null ? 0 : current.key.getGeneration();
        if (request.getExpectedGeneration() != observed) throw changed();
        if (current == null && entries.size() >= maximum)
            throw Status.RESOURCE_EXHAUSTED.withDescription("Session capacity exhausted").asRuntimeException();
        if (generation == Long.MAX_VALUE)
            throw Status.UNAVAILABLE.withDescription("Session generation exhausted").asRuntimeException();
        var key = SessionKey.newBuilder().setAccountId(claims.accountId()).setGameInstance(instance)
                .setGeneration(++generation).setBindingId(UUID.randomUUID().toString()).build();
        Entry next = new Entry(claims, key, request.getAttemptId(), request.getExpectedGeneration(),
                request.getTicketId(), nanoTime.getAsLong() + LEASE_NANOS);
        entries.put(claims.accountId(), next);
        // 同账号其他 Prepare 即使还没有消费，也已经观察了旧状态。必须当场作废，不能等当前
        // 连接 Close 后回到空槽，才让旧 expectedGeneration=0 的票据重新成为有效认证。
        invalidatePreparations(claims.accountId());
        opened++;
        if (current != null) replaced++;
        return binding(next);
    }

    /**
     * 命令入队前和真正执行前均检查；这里只读有效性，不靠业务流量隐式续租。
     * 已开始执行的命令可以完成，后继 Open 在同账号邮箱中等待它发布完毕后才生效。
     */
    synchronized boolean isCurrent(SessionClaims claims, SessionKey key) {
        if (closed || claims == null || key == null || !claims.accountId().equals(key.getAccountId())) return false;
        Entry current = current(claims.accountId());
        return current != null && current.key.equals(key) && current.claims.equals(claims);
    }

    /**
     * Gateway 每批至多 256 个租约。只有完整 key 匹配才续租；旧 key 即使批量迟到也不能延长
     * 新连接或复活旧连接。Check/Close 为内部接口，key 含随机 bindingId，应仅经受控内网/mTLS 传输。
     * 这项确认仅表示 GAME 当前承认连接，不能当作客户端已收到任何玩法结果或地图消息的 ACK。
     */
    synchronized List<SessionKey> check(List<SessionKey> requested) {
        requireOpen();
        if (requested.isEmpty() || requested.size() > MAX_CHECK_BATCH)
            throw Status.INVALID_ARGUMENT.withDescription("Session check batch must contain 1..256 bindings").asRuntimeException();
        for (SessionKey key : requested) requireKey(key);
        var currentKeys = new ArrayList<SessionKey>();
        long until = nanoTime.getAsLong() + LEASE_NANOS;
        for (SessionKey key : requested) {
            Entry current = current(key.getAccountId());
            if (current != null && current.key.equals(key)) {
                current.leaseDeadline = until;
                currentKeys.add(key);
                renewed++;
            }
        }
        return List.copyOf(currentKeys);
    }

    /** 断连通知可丢失，租约会兜底；迟到 Close 永远不能清掉另一条连接刚取得的新代次。 */
    synchronized void release(SessionKey key) {
        requireKey(key);
        if (closed) return;
        Entry current = entries.get(key.getAccountId());
        if (current != null && current.key.equals(key)) {
            entries.remove(key.getAccountId());
            invalidatePreparations(key.getAccountId());
            released++;
        }
    }

    private Entry current(String account) {
        Entry value = entries.get(account);
        if (value != null && expired(value)) {
            entries.remove(account);
            invalidatePreparations(account);
            expired++;
            return null;
        }
        return value;
    }

    private boolean expired(Entry entry) {
        long now = clock.instant().getEpochSecond();
        return now < entry.claims.issuedAt() || now >= entry.claims.expiresAt()
                || nanoTime.getAsLong() - entry.leaseDeadline >= 0;
    }

    private synchronized void removeExpired() {
        if (closed) return;
        entries.values().removeIf(value -> {
            if (!expired(value)) return false;
            invalidatePreparations(value.claims.accountId());
            expired++;
            return true;
        });
        long now = nanoTime.getAsLong();
        var tickets = preparations.entrySet().iterator();
        while (tickets.hasNext()) {
            var ticket = tickets.next();
            if (now - ticket.getValue().deadline() >= 0) {
                tickets.remove();
                unlinkPreparation(ticket.getKey(), ticket.getValue());
            }
        }
    }

    /** 账号索引只保留仍待消费的票据；作废一个账号无需扫描其他账号的认证请求。 */
    private Preparation takePreparation(String ticket) {
        Preparation prepared = preparations.remove(ticket);
        if (prepared != null) unlinkPreparation(ticket, prepared);
        return prepared;
    }

    private void unlinkPreparation(String ticket, Preparation prepared) {
        Set<String> accountTickets = preparationAccounts.get(prepared.claims().accountId());
        if (accountTickets == null) return;
        accountTickets.remove(ticket);
        if (accountTickets.isEmpty()) preparationAccounts.remove(prepared.claims().accountId());
    }

    private void invalidatePreparations(String account) {
        Set<String> accountTickets = preparationAccounts.remove(account);
        if (accountTickets != null) for (String ticket : accountTickets) preparations.remove(ticket);
    }

    private SessionBinding binding(Entry entry) {
        long remaining = TimeUnit.NANOSECONDS.toMillis(entry.leaseDeadline - nanoTime.getAsLong());
        // 重复 Open 没有续租。返回实际剩余时长，避免客户端以为又获得了完整三十秒。
        if (remaining <= 0) throw changed();
        return SessionBinding.newBuilder().setKey(entry.key).setExpiresAt(entry.claims.expiresAt())
                .setLeaseMillis((int) Math.min(LEASE_MILLIS, remaining)).build();
    }

    private void requireClaims(SessionClaims claims) {
        long now = clock.instant().getEpochSecond();
        if (claims == null || !AccountIds.isValid(claims.accountId()) || now < claims.issuedAt() || now >= claims.expiresAt())
            throw Status.UNAUTHENTICATED.withDescription("Valid session required").asRuntimeException();
    }

    private void requireOpen() {
        if (closed) throw Status.UNAVAILABLE.withDescription("Session directory closed").asRuntimeException();
    }

    static void requireKey(SessionKey key) {
        if (key == null || !AccountIds.isValid(key.getAccountId()) || !canonicalUuid(key.getGameInstance())
                || key.getGeneration() <= 0 || !canonicalUuid(key.getBindingId()))
            throw Status.INVALID_ARGUMENT.withDescription("Invalid session key").asRuntimeException();
    }

    private static boolean canonicalUuid(String value) {
        try { return UUID.fromString(value).toString().equals(value); }
        catch (IllegalArgumentException invalid) { return false; }
    }

    private static io.grpc.StatusRuntimeException changed() {
        return Status.FAILED_PRECONDITION.withDescription("SESSION_CHANGED").asRuntimeException();
    }

    synchronized Map<String, Object> metrics() {
        return Map.ofEntries(Map.entry("bindings", entries.size()), Map.entry("limit", maximum),
                Map.entry("preparations", preparations.size()), Map.entry("preparationLimit", MAX_PREPARATIONS),
                Map.entry("leaseMillis", LEASE_MILLIS), Map.entry("opened", opened), Map.entry("replaced", replaced),
                Map.entry("renewed", renewed), Map.entry("expired", expired), Map.entry("released", released), Map.entry("closed", closed));
    }

    @Override public void close() {
        synchronized (this) { closed = true; entries.clear(); preparations.clear(); preparationAccounts.clear(); }
        expiry.shutdownNow();
    }
}

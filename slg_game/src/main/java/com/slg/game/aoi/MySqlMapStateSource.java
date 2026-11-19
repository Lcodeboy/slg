package com.slg.game.aoi;

import com.slg.aoi.MapSnapshotSource;
import com.slg.game.application.port.*;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.RealmIdentity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 * MySQL 权威端口到本地 AOI/MapFeed 的阻塞适配器。由两者已有的有界读取线程调用，不能
 * 在玩家线程使用，也不再提交另一个冷读 Future 后阻塞等待。快照始终在本线程打开/关闭；
 * 部署须为地图配置独立读取连接和预算。本类不获得写权、不推进玩法，也不拥有连接池。
 */
public final class MySqlMapStateSource implements MapSnapshotSource {
    private static final int MAX_PAGE_CHANGES = 1024;
    private final RealmStateReader states;
    private final RealmEventReader events;
    private final RealmIdentity realm;
    private final Identity identity;
    private final ReadBudget snapshotBudget;
    private final ReadBudget eventBudget;

    public MySqlMapStateSource(RealmStateReader states, RealmEventReader events, RealmIdentity realm,
                              ReadBudget snapshotBudget, ReadBudget eventBudget) {
        this.states = Objects.requireNonNull(states);
        this.events = Objects.requireNonNull(events);
        this.realm = Objects.requireNonNull(realm);
        this.snapshotBudget = Objects.requireNonNull(snapshotBudget);
        this.eventBudget = Objects.requireNonNull(eventBudget);
        identity = new Identity(realm.realmId(), realm.datasetId().toString());
    }

    @Override public Identity identity() { return identity; }

    /**
     * H、移动行军、城市和矿点共用一个 RR 视图/累计 ReadBudget，不能每种对象或每页重开。
     * begin(H) 只供流式封包标记水位；失败或取消必须丢弃已发块，正常返回 H 才证明扫描完整。
     */
    @Override public long snapshot(Check check, SnapshotReceiver receiver) throws Exception {
        Objects.requireNonNull(check); Objects.requireNonNull(receiver);
        long started = System.nanoTime();
        long timeout = timeoutNanos(snapshotBudget);
        Check bounded = () -> {
            check.run();
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Map snapshot cancelled");
            if (System.nanoTime() - started >= timeout) throw new IOException("Map snapshot read budget exceeded");
        };
        bounded.run();
        try (var snapshot = states.openSnapshot(realm, snapshotBudget)) {
            if (!realm.equals(snapshot.identity())) throw new BadData("Map snapshot dataset identity differs");
            long high = snapshot.highSequence();
            if (high < 0) throw new BadData("Map snapshot watermark is negative");
            bounded.run(); receiver.begin(high); bounded.run();
            snapshot.scanMovingMarches(entry -> callback(() -> {
                bounded.run();
                var route = StructuredMapProjection.route(entry.state());
                if (route == null) throw new BadData("Moving march scan returned a stationary stage");
                receiver.route(route); bounded.run();
            }));
            snapshot.scan(ObjectKind.PLAYER, entry -> callback(() -> {
                bounded.run(); receiver.object(StructuredMapProjection.city(entry.state(), realm.realmId())); bounded.run();
            }));
            var sites = new HashMap<String, ResourceSite>();
            snapshot.scan(ObjectKind.GATHER_SITE, entry -> callback(() -> {
                bounded.run();
                var site = StructuredMapProjection.site(entry.state());
                // 投影先验证固定布局，最多三项；不能用任意持久键把覆盖表变成整区缓存。
                if (sites.put(site.objectId(), site) != null) throw new BadData("Duplicate resource site in map snapshot");
            }));
            for (var initial : StructuredMapProjection.staticSites()) {
                bounded.run(); receiver.object(sites.getOrDefault(initial.objectId(), initial)); bounded.run();
            }
            bounded.run();
            return high;
        } catch (CallbackFailure failed) { throw failed.failure; }
        catch (StorageAccessException failed) { throw translate(failed); }
    }

    /** 完整决定之间切页；资产等私有事件没有公开变化也推进 through，绝不根据最后 DTO 猜水位。 */
    @Override public Page changes(long afterSeq, int limit) throws Exception {
        if (afterSeq < 0 || limit < 1 || limit > 128) throw new IllegalArgumentException("Invalid map event bounds");
        long started = System.nanoTime(), timeout = timeoutNanos(eventBudget);
        checkChanges(started, timeout);
        final RealmEventReader.EventPage page;
        try { page = events.readAfter(realm, afterSeq, limit, eventBudget); }
        catch (StorageAccessException failed) { throw translate(failed); }
        if (!realm.equals(page.identity()) || page.after() != afterSeq)
            throw new BadData("Map event page differs from requested dataset or cursor");
        var routes = new ArrayList<Change>();
        var objects = new ArrayList<ObjectChange>();
        long through = afterSeq;
        for (var event : page.events()) {
            checkChanges(started, timeout);
            var projected = StructuredMapProjection.changes(event, realm.realmId());
            if (projected.size() > MAX_PAGE_CHANGES)
                throw new BadData("Single committed map decision exceeds output budget");
            if (routes.size() + objects.size() + projected.size() > MAX_PAGE_CHANGES) break;
            routes.addAll(projected.routes()); objects.addAll(projected.objects());
            through = event.sequence();
        }
        checkChanges(started, timeout);
        return new Page(identity, afterSeq, page.prunedThrough(), page.high(), through, routes, objects);
    }

    // 默认空 onAdvance 注册是允许的：AOI 周期补读保证追赶，MYSQL-06 可另装配轻量提交唤醒。
    private static Exception translate(StorageAccessException failure) {
        if (failure instanceof RealmEventReader.CursorExpired expired) return new CursorExpired(expired.prunedThrough());
        if (failure instanceof RealmEventReader.CursorAhead ahead) return new CursorAhead(ahead.high());
        return switch (failure.code()) {
            case IDENTITY_MISMATCH, UNSUPPORTED_SCHEMA, INVALID_DATA, CURSOR_EXPIRED ->
                    new BadData("Invalid authority map source: " + failure.code(), failure);
            case UNAVAILABLE, OWNERSHIP_CONFLICT, READ_BUDGET_EXCEEDED ->
                    new IOException("Authority map read unavailable: " + failure.code(), failure);
        };
    }

    private static long timeoutNanos(ReadBudget budget) {
        try { return budget.timeout().toNanos(); } catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
    }
    private static void checkChanges(long started, long timeout) throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Map changes cancelled");
        if (System.nanoTime() - started >= timeout) throw new IOException("Map event projection read budget exceeded");
    }
    @FunctionalInterface private interface Callback { void run() throws Exception; }
    private static void callback(Callback action) {
        try { action.run(); } catch (Exception failure) { throw new CallbackFailure(failure); }
    }
    /** Consumer 不声明 checked 异常；保留取消、传输背压及投影失败原因，不能误报存储断连。 */
    private static final class CallbackFailure extends RuntimeException {
        private final Exception failure;
        private CallbackFailure(Exception failure) { super(failure); this.failure = failure; }
    }
}

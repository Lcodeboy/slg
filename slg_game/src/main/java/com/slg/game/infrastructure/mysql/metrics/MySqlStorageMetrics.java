package com.slg.game.infrastructure.mysql.metrics;

import com.slg.core.metrics.LatencyMetric;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 单区 MySQL 适配器共享的固定维度指标，不按玩家、请求或 SQL 文本分配标签。
 * ownerSharedSql/cursorLockSql 是 MyBatis 调用全耗时，包含网络、服务端执行与可能的锁等待，
 * 不能据此声称测到了纯锁等待。总调用时间还包含取连接、核验、清理和结果判断。
 * commit 的成功表示确认提交；resolve 的成功表示获得确定结论（可以确定未提交）；renew=false 记失败。
 */
public final class MySqlStorageMetrics {
    public enum Operation {
        COMMIT("commit"), RESOLVE_AFTER_FENCE("resolveAfterFence"),
        OWNER_SHARED_SQL("ownerSharedSql"), CURSOR_LOCK_SQL("cursorLockSql"),
        OWNERSHIP_ACQUIRE("ownershipAcquire"), OWNERSHIP_RENEW("ownershipRenew"),
        OWNERSHIP_FENCE("ownershipFence"), OWNERSHIP_REACQUIRE("ownershipReacquire"),
        OWNERSHIP_OBSERVE("ownershipObserve"), OWNERSHIP_RELEASE("ownershipRelease");
        private final String key;
        Operation(String key) { this.key = key; }
    }
    private final Map<Operation, LatencyMetric> timers;
    public MySqlStorageMetrics() {
        var values = new EnumMap<Operation, LatencyMetric>(Operation.class);
        for (var operation : Operation.values()) values.put(operation, new LatencyMetric());
        timers = Map.copyOf(values);
    }
    public void record(Operation operation, long elapsedNanos, boolean success) {
        timers.get(operation).record(elapsedNanos, success);
    }
    /** health 只读取内存累计值；不会执行 SQL、取连接或等待事务。桶快照沿用 LatencyMetric 弱一致约定。 */
    public Map<String, Object> snapshot() {
        var result = new LinkedHashMap<String, Object>();
        for (var operation : Operation.values()) result.put(operation.key, timers.get(operation).snapshot());
        return Map.copyOf(result);
    }
}

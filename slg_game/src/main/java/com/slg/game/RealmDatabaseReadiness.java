package com.slg.game;

import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.WriterToken;
import com.slg.game.infrastructure.mysql.runtime.MySqlStartupCheck;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 探测线程发布、入口只读的数据库观察。它不授予写权；真正事务仍须核验 owner/token。
 * 以探测开始时刻计观察年龄，连探测自身耗时也扣除，不能在慢查询结束后重新赠送租约寿命。
 */
final class RealmDatabaseReadiness {
    private record Observation(MySqlStartupCheck.Snapshot value, long beganNanos) { }
    private final long freshnessNanos;
    private volatile Observation observation;
    private volatile boolean reachable, schemaValid = true;
    private volatile String failure = "";

    RealmDatabaseReadiness(Duration freshness) { freshnessNanos = freshness.toNanos(); }

    void observed(MySqlStartupCheck.Snapshot value, long beganNanos) {
        observation = new Observation(value, beganNanos);
        failure = ""; reachable = true;
    }

    void failed(StorageAccessException.Code code) {
        reachable = false; failure = code.name();
        if (code == StorageAccessException.Code.IDENTITY_MISMATCH || code == StorageAccessException.Code.UNSUPPORTED_SCHEMA
                || code == StorageAccessException.Code.INVALID_DATA) schemaValid = false;
    }

    boolean schemaValid() { return schemaValid; }

    boolean allows(WriterToken writer) {
        var current = observation;
        if (!reachable || !schemaValid || current == null) return false;
        long age = Math.max(0, System.nanoTime() - current.beganNanos());
        var value = current.value();
        long remainingMillis = value.expiresAtMillis() - value.databaseNowMillis();
        return age < freshnessNanos && remainingMillis > 0
                && age / 1_000_000 < remainingMillis && value.ownerActive()
                && writer.epoch() == value.epoch() && writer.instanceId().equals(value.ownerInstance());
    }

    /** 仅组合已缓存的观察，不同步探库；错误只暴露稳定分类，不含连接信息。 */
    Map<String, Object> health() {
        var result = new LinkedHashMap<String, Object>();
        var current = observation;
        long age = current == null ? -1 : Math.max(0, System.nanoTime() - current.beganNanos());
        result.put("reachable", reachable && age >= 0 && age < freshnessNanos);
        result.put("schemaVerified", schemaValid); result.put("probeAgeMillis", age < 0 ? -1 : age / 1_000_000);
        result.put("failure", failure);
        if (current != null) {
            var value = current.value();
            result.put("highSequence", value.highSequence()); result.put("prunedThrough", value.prunedThrough());
            result.put("observedEpoch", value.epoch()); result.put("ownerActive", value.ownerActive());
        }
        return Map.copyOf(result);
    }
}

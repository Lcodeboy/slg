package com.slg.game.domain.persistence;

/**
 * 区服内请求的去重身份。作用域限 1 至 256 个、请求编号限 1 至 128 个非空格可打印
 * ASCII 字符，区分大小写，不做 UUID 转换，以保留客户端及定时任务的稳定编号。
 * 同一数据集中的相同键只能对应同一请求摘要及原始结果。
 */
public record RequestKey(String scope, String requestId) {
    public RequestKey {
        PersistenceChecks.identifier(scope, "scope", 256);
        PersistenceChecks.identifier(requestId, "requestId", 128);
    }
}

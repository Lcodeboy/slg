package com.slg.game.infrastructure.mysql.row;

/** 元数据行仅用于 SQL 适配；UUID 在 Mapper 内转成规范字符串，不泄漏 BINARY(16) 到领域。 */
public final class RealmMetadata {
    private RealmMetadata() { }

    public record Identity(int realmId, String datasetId, int schemaVersion) { }
    public record Owner(long writerEpoch, String ownerInstance, boolean active) { }
    public record Lease(long writerEpoch, String ownerInstance, long expiresAtMillis, long databaseNowMillis) { }
    public record Cursor(long highSeq, long prunedThrough) { }
}

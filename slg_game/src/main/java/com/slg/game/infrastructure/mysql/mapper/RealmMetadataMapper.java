package com.slg.game.infrastructure.mysql.mapper;

import com.slg.game.infrastructure.mysql.row.RealmMetadata;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 区服元数据的显式 SQL。所有锁依附调用方同一 Session/事务，不在 Mapper 中提交。
 * owner → lease 是所有权操作锁序；业务只锁 owner，共享锁直到 COMMIT 才释放。
 */
public interface RealmMetadataMapper {
    @Select("SELECT realm_id, BIN_TO_UUID(dataset_id) AS dataset_id, schema_version "
            + "FROM realm_identity WHERE realm_id=#{realmId}")
    RealmMetadata.Identity identity(@Param("realmId") int realmId);

    @Select("SELECT writer_epoch, BIN_TO_UUID(owner_instance) AS owner_instance, active "
            + "FROM realm_owner WHERE realm_id=#{realmId} FOR SHARE")
    RealmMetadata.Owner lockOwnerShared(@Param("realmId") int realmId);

    @Select("SELECT writer_epoch, BIN_TO_UUID(owner_instance) AS owner_instance, active "
            + "FROM realm_owner WHERE realm_id=#{realmId} FOR UPDATE")
    RealmMetadata.Owner lockOwnerExclusive(@Param("realmId") int realmId);

    /**
     * 普通读不锁 lease，避免续期被整个业务事务拖住。同 epoch 的租约只延长，旧快照至多
     * 保守拒绝；owner 当前读及持续共享锁才是隔离证据。到期只拒绝新业务，不撤销已开始事务。
     */
    @Select("SELECT writer_epoch, BIN_TO_UUID(owner_instance) AS owner_instance, expires_at_millis, "
            + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED) AS database_now_millis "
            + "FROM realm_owner_lease WHERE realm_id=#{realmId}")
    RealmMetadata.Lease lease(@Param("realmId") int realmId);

    /** 恢复观察使用当前读，先持 owner 共享锁再短暂共享锁 lease，不沿用较早的 MVCC 租约。 */
    @Select("SELECT writer_epoch, BIN_TO_UUID(owner_instance) AS owner_instance, expires_at_millis, "
            + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED) AS database_now_millis "
            + "FROM realm_owner_lease WHERE realm_id=#{realmId} FOR SHARE")
    RealmMetadata.Lease lockLeaseShared(@Param("realmId") int realmId);

    @Select("SELECT writer_epoch, BIN_TO_UUID(owner_instance) AS owner_instance, expires_at_millis, "
            + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED) AS database_now_millis "
            + "FROM realm_owner_lease WHERE realm_id=#{realmId} FOR UPDATE")
    RealmMetadata.Lease lockLease(@Param("realmId") int realmId);

    @Select("SELECT high_seq, pruned_through FROM realm_commit_cursor WHERE realm_id=#{realmId}")
    RealmMetadata.Cursor cursor(@Param("realmId") int realmId);

    @Select("SELECT high_seq, pruned_through FROM realm_commit_cursor WHERE realm_id=#{realmId} FOR UPDATE")
    RealmMetadata.Cursor lockCursor(@Param("realmId") int realmId);

    /** NULL 安全比较允许显式初始化的空 owner；仍核对完整旧任期，不能猜测 fence 丢失的结果。 */
    @Update("UPDATE realm_owner SET writer_epoch=#{newEpoch}, owner_instance=UUID_TO_BIN(#{newInstance}), "
            + "active=#{newActive} WHERE realm_id=#{realmId} AND writer_epoch=#{expectedEpoch} "
            + "AND owner_instance <=> UUID_TO_BIN(#{expectedInstance}) AND active=#{expectedActive}")
    int updateOwner(@Param("realmId") int realmId, @Param("expectedEpoch") long expectedEpoch,
                    @Param("expectedInstance") String expectedInstance, @Param("expectedActive") boolean expectedActive,
                    @Param("newEpoch") long newEpoch, @Param("newInstance") String newInstance,
                    @Param("newActive") boolean newActive);

    /** 调用方先锁 owner 再锁 lease。租约时钟完全由数据库提供；0 只用于显式释放。 */
    @Update("UPDATE realm_owner_lease SET writer_epoch=#{epoch}, owner_instance=UUID_TO_BIN(#{instance}), "
            + "expires_at_millis=CASE WHEN #{leaseMillis}=0 THEN 0 ELSE "
            + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)+#{leaseMillis} END "
            + "WHERE realm_id=#{realmId}")
    int updateLease(@Param("realmId") int realmId, @Param("epoch") long epoch,
                    @Param("instance") String instance, @Param("leaseMillis") long leaseMillis);

    /** 锁等待可能跨越到期点，因此 UPDATE 再用数据库时间验证，不能靠前一条 SELECT 的时间复活租约。 */
    @Update("UPDATE realm_owner_lease SET expires_at_millis=GREATEST(expires_at_millis, "
            + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)+#{leaseMillis}) "
            + "WHERE realm_id=#{realmId} AND writer_epoch=#{epoch} "
            + "AND owner_instance=UUID_TO_BIN(#{instance}) "
            + "AND expires_at_millis > CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)")
    int renewLease(@Param("realmId") int realmId, @Param("epoch") long epoch,
                   @Param("instance") String instance, @Param("leaseMillis") long leaseMillis);

    /** 尾部持 cursor 排他锁后只推进一个序号；不分配自增值，回滚自然不留事件洞。 */
    @Update("UPDATE realm_commit_cursor SET high_seq=#{newHighSeq} WHERE realm_id=#{realmId} "
            + "AND high_seq=#{expectedHighSeq} AND #{newHighSeq}=high_seq+1")
    int advanceCursor(@Param("realmId") int realmId, @Param("expectedHighSeq") long expectedHighSeq,
                      @Param("newHighSeq") long newHighSeq);
}

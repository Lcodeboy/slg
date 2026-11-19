package com.slg.game.application.port;

import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.WriterToken;
import java.util.Objects;
import java.util.UUID;

/**
 * 权威写者任期的获取与隔离。令牌不是客户端凭证，也不能只靠内存或服务发现判断有效。
 * 业务事务必须在存储端持有所有权行的共享锁至提交结束；本端口的换任期和释放操作
 * 通过同一行的排他锁等待旧事务终结，不能仅更新路由或超时标志后宣称已完成隔离。
 */
public interface RealmOwnership {
    /**
     * 读取数据集当前 owner 与租约的一致观察，以数据库时间判断租约是否仍有效。
     * 用于 fence/acquire 确认丢失后的恢复分支：识别本实例尚未到期的新任期或其他实例接管，
     * 不续期、不自动 acquire，也不把观察结果升级成 WriterToken。观察之后状态仍可变化，
     * 真正取得写权或证明旧事务不能迟到提交，仍须经过 acquire/reacquire/fence 的排他屏障。
     * 缺失、身份不符、owner/lease 不一致或数据库不可读均抛错，不返回猜测的空 owner。
     */
    OwnershipObservation observe(RealmIdentity identity) throws StorageAccessException;

    /**
     * 只读快照，不是写者授权。epoch=0 仅表示显式初始化后的未获得写权状态；已释放 owner
     * 保留最后 epoch 且 instanceId 为空。active=true 仍可能 leaseValid=false，不能将到期
     * 解释成旧业务已回滚；leaseValid 只在本次数据库观察时成立。
     */
    record OwnershipObservation(RealmIdentity realm, long epoch, UUID instanceId, boolean active, boolean leaseValid) {
        public OwnershipObservation {
            Objects.requireNonNull(realm, "realm");
            if (epoch < 0 || active && (epoch == 0 || instanceId == null)
                    || !active && (instanceId != null || leaseValid))
                throw new IllegalArgumentException("Invalid ownership observation");
        }
    }

    /**
     * 仅在没有有效写者时取得指定实例的新任期；已有有效写者即使属于同一实例也拒绝。
     * 跨实例接管必须确认旧任期失效，并通过排他屏障等待旧事务结束后推进 epoch。
     * 错数据集、身份或所有权冲突明确失败，不得初始化替代数据集或复用旧 epoch。
     */
    WriterToken acquire(RealmIdentity identity, UUID instanceId) throws StorageAccessException;

    /**
     * 仅用于确认丢失后的本实例恢复。expected 必须保留 active 实例身份且观察到租约已过期；
     * 它只是比较条件，不是可用 token，也不能证明调用时租约仍然过期。
     * 实现须在 owner 排他锁内重新核对数据集、epoch、instance、active 与当前数据库租约，
     * 全部仍匹配且已过期才推进任期。观察后发生其他实例接管、释放或续期均明确拒绝，
     * 不能退回普通 acquire；结果确认丢失仍不返回猜测的 token。
     */
    WriterToken reacquire(OwnershipObservation expected) throws StorageAccessException;

    /**
     * 按 previous 的完整身份做 CAS 隔离，返回同一实例、更高 epoch 的替代令牌，供当前
     * 实例核对未知提交；不用于绕过有效写者进行跨实例接管。排他屏障成功后旧令牌无法
     * 新增写入，原来持共享锁的事务均已终结。CAS 失配、超时或确认丢失不得返回隔离成功。
     *
     * <p>fence 自身确认丢失时，应用不能猜测新 epoch 或无限重试旧 token 的 CAS。
     * 首版恢复路径是保持区服隔离、停止该实例续期，observe 确认本实例租约失效后通过
     * reacquire 条件屏障取得更高任期，再核对旧业务回执；观察身份已变化则停止本实例写入，
     * 不能退回普通 acquire 抢回恢复权限。
     * 等待期间旧业务仍为未知，不得释放相关对象或假报排空成功。</p>
     */
    WriterToken fence(WriterToken previous) throws StorageAccessException;

    /**
     * 仅续期仍有效的同一令牌；已失效或被替代返回 false。数据库不可访问应抛错，
     * 不得把无法核实当作续期成功；应用在失去有效写权后停止新增持久化尝试。
     */
    boolean renew(WriterToken token) throws StorageAccessException;

    /**
     * 通过完整令牌比较后释放并等待相关旧事务结束；不得释放更高任期的新写者。
     * 已失效令牌可幂等返回，但存储不可访问或结果无法确认仍应抛错。
     */
    void release(WriterToken token) throws StorageAccessException;
}

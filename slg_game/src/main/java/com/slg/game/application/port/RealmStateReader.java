package com.slg.game.application.port;

import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.VersionedState;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/** 对权威对象进行一致读取；快照不提供写入能力，也不授予对象执行资格。 */
public interface RealmStateReader {
    /**
     * 打开同一连接上的可重复读快照，并用同一普通一致读视图取得提交水位与对象状态。
     * 不能把锁定读取得的当前水位与较旧 MVCC 对象混合。打开时验证数据集与格式，
     * 失败不返回半初始化快照；budget 约束从打开到关闭期间全部读取的累计成本，
     * 行军等聚合的明细行也计入行数和字节预算，不能只统计根对象而隐藏加载成本。
     */
    Snapshot openSnapshot(RealmIdentity identity, ReadBudget budget) throws StorageAccessException;

    /**
     * 快照限创建它的线程使用，必须显式关闭；不能跨异步等待长期持有读取事务。
     * 调用方在整次扫描成功前不得把部分条目发布为完整地图或权威缓存。
     */
    interface Snapshot extends AutoCloseable {
        RealmIdentity identity();

        /** 本快照可见的完整连续提交前缀，不是自增编号的最大分配值。 */
        long highSequence();

        /** 按类型化主键读取；返回状态的 key 必须与请求相同，根行与明细共用本快照。 */
        Optional<VersionedState> find(ObjectKey key) throws StorageAccessException;

        /**
         * 用本区 account_id 唯一索引读取 PlayerState，accountId 必须为正数。
         * 去掉通用 account-role JSON 对象后，登录仍须在本快照核对账号归属；不得在
         * 另一个连接查角色 ID，再与当前视图拼接。重复归属属于坏数据，不能随意选一行。
         */
        Optional<VersionedState> findPlayerByAccount(long accountId) throws StorageAccessException;

        /**
         * 按 ObjectKey 的主键元组顺序扫描封闭类型，字符串为 ASCII 二进制顺序，
         * 坐标和资源/兵种 code 为数值顺序，不能按枚举名称或 ordinal 排序；
         * 不能把复合键拼成字符串后再解析。所有条目属于此快照，
         * 多表聚合必须组装完整才交给消费者，不向上暴露数据库行 DTO 或半份明细。
         * 达到预算、读取失败或条目损坏必须抛错，不能以正常返回表达截断。
         * 消费者抛出的运行时异常向上传播；关闭仍由快照持有者保证。
         */
        void scan(ObjectKind kind, Consumer<StateEntry> consumer) throws StorageAccessException;

        /**
         * 通过归属索引读取指定玩家的结构化状态，避免冷加载一个玩家却扫描全区。
         * PLAYER 按 playerId 匹配，其余类型按 playerId 字段匹配，GATHER_SITE 按
         * occupantPlayerId 匹配且不含空闲矿点；结果仍按本类型主键排序。不得靠过滤
         * 已读出的全区列表实现预算；失败与超限规则同 scan，禁止把截断当作完整资产。
         */
        void scanOwned(ObjectKind kind, String playerId, Consumer<StateEntry> consumer) throws StorageAccessException;

        /**
         * 冷加载当前玩家的活动行军，仅查询 OUTBOUND/GATHERING/BATTLE_PENDING/RETURNING。
         * 必须用归属和状态索引跳过 COMPLETED 历史，不能先 scanOwned 全历史再内存过滤。
         * 返回完整 MarchState 及根版本，按 marchId 二进制顺序输出，主行/明细同属本快照；
         * 超限整次失败。调用方判断行军槽位时还须持玩家执行资格，本扫描不授予集合写权。
         */
        void scanActiveMarches(String playerId, Consumer<StateEntry> consumer) throws StorageAccessException;

        /**
         * 全区地图只读取 OUTBOUND/RETURNING 的完整行军聚合，按 (status.code, marchId) 顺序
         * 分两段分页；每段 marchId 为二进制顺序，切段从空游标开始，但不重开快照或重置预算。
         * 必须用移动状态索引跳过静止阶段和完成历史，不能全表读取后过滤；根及明细仍属于
         * 本快照和累计预算。只读公开投影可以消费它，但不能据此取得行军执行资格。
         */
        void scanMovingMarches(Consumer<StateEntry> consumer) throws StorageAccessException;

        /** 释放快照及其存储资源；关闭后 find/scan 必须拒绝继续读取。 */
        @Override void close() throws StorageAccessException;
    }

    record StateEntry(ObjectKey key, VersionedState state) {
        public StateEntry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(state, "state");
            if (!key.equals(state.state().key()))
                throw new IllegalArgumentException("Snapshot key must identify its typed state");
        }
    }
}

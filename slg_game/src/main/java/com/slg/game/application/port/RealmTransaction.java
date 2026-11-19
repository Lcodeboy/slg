package com.slg.game.application.port;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.domain.persistence.CommitDecision;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.persistence.WriterToken;
import java.util.Objects;

/**
 * 一项完整业务的阻塞事务端口。应用提交器须在有界存储执行器中调用，不在这里等待
 * 网络响应或发布内存。MySQL 适配器实现本端口；返回值区分确定结果与待核对尝试。
 */
public interface RealmTransaction {
    /**
     * 同一事务持有写者所有权行共享锁，核验 writer、原请求回执、全部读依赖与变更版本，
     * 再原子保存类型化状态、待办、事实与回执。资源/兵力/英雄按各自键及版本更新；
     * 行军、战斗的根行与结构化明细共用根版本，不能部分提交或把完整状态写回万能 JSON。
     * 事务末尾锁住连续游标行分配序号，游标及事件
     * 随业务一起提交；不得先分配自增 ID 再把它当作连续已提交水位。
     * 新建对象须在取得游标锁前完成唯一键预留，不能持有游标锁再等待业务对象 INSERT；
     * 事务内占位的来源序号必须在提交前全部最终化，不得泄漏为已提交状态。
     * MyBatis 只属于基础设施适配器：同一工作线程共用一个 Session/连接，CAS 必须在继续
     * 获取 cursor 前取得实际更新行数；Mapper 返回、批次 flush 均不表示 COMMIT 成功。
     *
     * <p>同键同摘要返回原回执且 applied=false；同键不同摘要确定拒绝。新提交只有在
     * 确认成功后才返回 applied=true。已持久化的业务拒绝也是 Committed。
     * COMMIT 确认丢失等不确定结果必须返回 Uncertain，不能返回 ROLLED_BACK，不能靠
     * 自动重试重新计算或覆盖旧决定。异常也不授予调用方直接释放未决对象的权利。</p>
     */
    CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException;

    /**
     * 只用于旧尝试经过隔离后的核对，不执行或重新计算业务。
     *
     * <p>previous 与 replacement 必须属于同一数据集，replacement 的 epoch 更高且仍
     * 有效；隔离操作已经禁止旧 token 后续写入，并等待其全部存储事务终结。调用方还
     * 必须保持该请求的独占核对资格，保证没有使用新 token 的同请求尝试并行进入。
     * 实现须在受 replacement 写权保护的读取范围内核验这些前提并查权威回执。</p>
     *
     * <p>找到相同摘要的原回执返回 Committed(applied=false)；同键不同摘要返回确定的
     * REQUEST_CONFLICT。只有全部隔离证据成立且回执仍不存在时，才能返回确定未提交
     * ROLLED_BACK 或 OWNER_LOST。缺少任一屏障证明必须返回 Uncertain；数据库无法
     * 完成访问可抛异常，但一次空 SELECT、超时或断线均不能证明旧尝试不存在。</p>
     */
    CommitAttempt resolveAfterFence(WriterToken previous, WriterToken replacement, RequestKey request, String digest)
            throws StorageAccessException;

    sealed interface CommitAttempt permits CommitAttempt.Settled, CommitAttempt.Uncertain {
        /** 仅包含业务提交结果已经确定、不会再迟到改变的结果；不表示内存发布已完成。 */
        record Settled(CommitResult result) implements CommitAttempt {
            public Settled { Objects.requireNonNull(result, "result"); }
        }

        /** 应继续核对并保持相关对象资格，不能当作普通失败重投或完成客户端成功响应。 */
        record Uncertain() implements CommitAttempt {}
    }
}

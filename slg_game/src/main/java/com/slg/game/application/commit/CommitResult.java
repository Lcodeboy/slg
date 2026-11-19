package com.slg.game.application.commit;

import com.slg.game.domain.persistence.CommitReceipt;
import java.util.Objects;

/**
 * 已确定的存储结果，刻意不包含 UNKNOWN。普通网络异常或一次查不到回执都不能构造
 * NotCommitted；调用方必须继续核对并保持相关对象的执行资格。
 *
 * <p>持久化的“资源不足”等业务拒绝同样属于 Committed：原结果必须可重放。
 * 本类型不表示客户端已经收到响应，也不表示内存发布已完成。</p>
 */
public sealed interface CommitResult permits CommitResult.Committed, CommitResult.NotCommitted {
    /**
     * applied=true 仅表示本次明确完成了新提交，可由仍持有资格的应用发布原草案。
     * 查重或未知结果核对得到的回执使用 false，应用须核对/重载最新状态，不能把旧草案
     * 覆盖到较新的缓存。receipt 是原业务结果，不由当前规则重新计算。
     */
    record Committed(CommitReceipt receipt, boolean applied) implements CommitResult {
        public Committed { Objects.requireNonNull(receipt, "receipt"); }
    }

    /** 本次尝试确定没有生效，并且不能在此结果返回后迟到提交。 */
    record NotCommitted(Reason reason) implements CommitResult {
        public NotCommitted { Objects.requireNonNull(reason, "reason"); }
    }

    /** 基础设施或版本拒绝原因；不在这里表达资源不足、重复领奖等领域结果。 */
    enum Reason {
        QUEUE_FULL, QUEUE_TIMEOUT, CANCELLED, STOPPING,
        OWNER_LOST, VERSION_CONFLICT, REQUEST_CONFLICT, INVALID_DECISION, ROLLED_BACK,
        /** 在任何 SQL 启动前因未知提交或发布修复屏障拒绝；不用于表达已启动尝试的 UNKNOWN。 */
        ISOLATED
    }
}

package com.slg.game.domain.persistence;

import java.util.Objects;

/**
 * 同一数据集提交流中的完整决定，消费者据此恢复增量，无须重新计算原命令。
 * 正数序号的分配、连续性和实际提交事实由存储端口保证，不能用自增编号或本值的
 * 构造成功替代提交顺序证明；仅构造此值不会发布通知或修改任何对象。
 */
public record CommittedEvent(long sequence, CommitDecision decision) {
    public CommittedEvent {
        PersistenceChecks.positive(sequence, "sequence");
        Objects.requireNonNull(decision, "decision");
    }
}

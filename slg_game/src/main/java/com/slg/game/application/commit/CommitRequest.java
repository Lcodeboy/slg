package com.slg.game.application.commit;

import com.slg.game.domain.persistence.CommitDecision;
import com.slg.game.domain.persistence.WriterToken;
import java.time.Duration;
import java.util.Objects;

/**
 * 一项业务的提交命令。领域决定与执行预算分离：origin 和 queueBudget 不写入业务日志，
 * 不参与请求摘要，也不能因换了执行预算把同一请求变成新业务。
 *
 * <p>queueBudget 从提交器接收请求时按单调时钟计时，只限制持久化尝试开始前的等待；
 * 为零表示仅允许立即执行。SQL 已开始后不得以该预算到期推断回滚。writer 失效时必须
 * 结束或核对原尝试，在重新取得执行资格并复核状态前，不能只替换 token 盲目重投。</p>
 *
 * <p>命令不能包含活跃 Player 引用、SQL Lambda、连接或响应回调。实际编码的字节预算
 * 由提交器核算，不能信任调用方自报长度。缺失引用或负等待预算在入队前拒绝。</p>
 */
public record CommitRequest(WriterToken writer, CommitDecision decision, Origin origin, Duration queueBudget) {
    public CommitRequest {
        Objects.requireNonNull(writer, "writer");
        Objects.requireNonNull(decision, "decision");
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(queueBudget, "queueBudget");
        if (queueBudget.isNegative()) throw new IllegalArgumentException("Queue budget must not be negative");
    }

    /** 用于公平调度和保留额度；所有来源遵守相同的写权、版本和事务约束。 */
    public enum Origin { CLIENT, DUE_TASK, BATTLE_RESULT, ADMIN }
}

package com.slg.game.application.execution;

import com.slg.game.application.commit.CommitResult;
import java.util.Objects;

/** 执行入口的容量/等待/停写拒绝；reason 只复用拒绝分类，不表示尝试过提交事务。 */
public final class QueryRejectedException extends RuntimeException {
    private final CommitResult.Reason reason;
    public QueryRejectedException(CommitResult.Reason reason) {
        super("Qualified query rejected: " + Objects.requireNonNull(reason, "reason"));
        this.reason = reason;
    }
    public CommitResult.Reason reason() { return reason; }
}

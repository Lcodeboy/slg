package com.slg.game.application.execution;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.domain.persistence.RequestKey;

import java.util.concurrent.CompletionStage;

/** 无 release/submit 方法的只读句柄；存储结果及发布/重载完成后才允许完成并响应。 */
public interface OperationHandle {
    RequestKey requestKey();
    OperationPhase phase();
    CompletionStage<CommitResult> completion();
    /** 仅与取得执行资格原子竞争；成功意味着没有进行读取、裁决或提交。 */
    boolean cancelBeforeStart();
}

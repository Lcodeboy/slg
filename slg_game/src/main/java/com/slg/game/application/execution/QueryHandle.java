package com.slg.game.application.execution;

import com.slg.game.domain.persistence.RequestKey;
import java.util.concurrent.CompletionStage;

/** 查询不具有数据库提交结论；取消派生 Future 不释放内部资格，只允许取消尚未开始的项。 */
public interface QueryHandle {
    RequestKey requestKey();
    OperationPhase phase();
    boolean cancelBeforeStart();
    CompletionStage<OperationValue> result();
}

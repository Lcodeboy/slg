package com.slg.game.application.commit;

import com.slg.game.domain.persistence.RequestKey;
import java.util.concurrent.CompletionStage;

/**
 * 提交尝试的只读观测与受控取消句柄。实现必须线程安全，客户端等待与内部执行生命周期
 * 分离；不能把内部可变 CompletableFuture 直接暴露给 RPC 层。
 */
public interface CommitHandle {
    RequestKey requestKey();

    CommitPhase phase();

    /**
     * 只在数据库结果确定后完成；RECONCILING 期间保持未完成。适配器必须返回隔离的
     * CompletionStage 视图，取消或超时其派生 Future 不得终止内部任务。
     *
     * <p>预计的拒绝和确定回滚用 CommitResult 表达；数据库失联、COMMIT 确认丢失不能
     * 通过异常完成冒充未提交。非预期内部异常若导致 stage 异常完成，应用必须隔离相关
     * 对象并进入故障核对，不能在通用 whenComplete/finally 中无条件释放执行资格。</p>
     */
    CompletionStage<CommitResult> settlement();

    /**
     * 与持久化尝试的启动原子竞争。仅当本次调用成功阻止启动时返回 true，并最终产生
     * NotCommitted(CANCELLED)。false 不表示已经提交，也不授予重试权限。
     */
    boolean cancelBeforeStart();
}

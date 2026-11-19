package com.slg.game.application.execution;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.domain.persistence.CommitDecision;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 启动装配时注册的长期用例适配器，不允许按请求注册捕获活跃实体的闭包。
 * 各方法由执行器的独立有界线程池调用；同对象资格始终覆盖准备、存储等待和发布/修复。
 */
public interface OperationHandler {
    /** 只读取已提交状态并计算私有不可变决定；不得写数据库、发布缓存或发送成功响应。 */
    default CommitDecision prepare(OperationCommand command) throws Exception { throw new UnsupportedOperationException("prepareAsync is required"); }

    /** 只用于 applied=true 的新提交。失败不改变数据库成功，执行器将隔离并调用 reload。 */
    default void publish(OperationCommand command, CommitDecision decision, CommitResult.Committed committed) throws Exception {
        throw new UnsupportedOperationException("publishAsync is required");
    }

    /**
     * 回执重放或发布失败时失效并重载整个范围；只有所有相关缓存已可安全服务才返回成功。
     * 不得用旧草案覆盖新状态。失败保留资格及区服隔离，等待显式 retryRepair。
     */
    default void reload(OperationCommand command, CommitDecision decision, CommitResult.Committed committed) throws Exception {
        throw new UnsupportedOperationException("reloadAsync is required");
    }

    /** 冷读交给有界读取端口；返回未完成阶段时释放工作线程，但始终保留完整执行资格。 */
    default CompletionStage<CommitDecision> prepareAsync(OperationCommand command) throws Exception {
        return CompletableFuture.completedFuture(prepare(command));
    }

    /** 异步发布与同步发布具有相同一致性边界；失败必须重载，不能将已提交结果改成回滚。 */
    default CompletionStage<Void> publishAsync(OperationCommand command, CommitDecision decision,
                                              CommitResult.Committed committed) throws Exception {
        publish(command, decision, committed);
        return CompletableFuture.completedFuture(null);
    }

    /** 阶段成功仅表示全部相关缓存已恢复可服务；失败继续隔离并持有资格。 */
    default CompletionStage<Void> reloadAsync(OperationCommand command, CommitDecision decision,
                                             CommitResult.Committed committed) throws Exception {
        reload(command, decision, committed);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 已成功准备但确定未提交时，在释放资格前失效可能过期的缓存（如版本冲突/失去 owner）。
     * 失败保持原 NotCommitted 结论并隔离，不能让后继读到旧缓存，也不能伪造 UNKNOWN。
     */
    default CompletionStage<Void> discardAsync(OperationCommand command, CommitResult.NotCommitted rejected) throws Exception {
        return CompletableFuture.completedFuture(null);
    }
}

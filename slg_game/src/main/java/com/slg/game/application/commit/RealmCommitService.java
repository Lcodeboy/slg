package com.slg.game.application.commit;

import java.util.concurrent.CompletionStage;

/**
 * 应用层有界提交入口。队列装载完整不可变命令，MySQL 连接和 SQL 留在基础设施层。
 * 一项业务使用一个事务；默认不把无关玩家业务合成一个大事务。
 *
 * <p>submit 不等待队列腾位、SQL 或 COMMIT。实现应先验证并计量不可变载荷，再以原子
 * 准入占用任务数/字节预算；超限返回已确定的拒绝句柄，不能丢掉任务或无限排队。
 * CLIENT、到期任务和战果共享冲突顺序，系统任务还须有保留额度及公平调度。</p>
 */
public interface RealmCommitService {
    /**
     * 合法请求总是返回句柄；满额/停止等正常拒绝通过 NotCommitted 表达。
     * 接收任务只表示进入内存调度，不能据此向玩家确认成功。JDBC 仅由有界执行器调用。
     */
    CommitHandle submit(CommitRequest request);

    /**
     * 停止接收新任务，明确取消尚未启动的任务，并等待已启动尝试取得确定结果。
     * 数据库不可用且仍有未知提交时不得伪造排空成功。此阶段只证明存储任务收敛，
     * 组合根应先 drain 用例执行层（准备中的任务还需 submit），再 drain 本提交器、冷读，
     * 最后释放写权和关闭数据库；用例层发布/重载尚未完成时不能关闭资源。
     */
    CompletionStage<Void> drain();
}

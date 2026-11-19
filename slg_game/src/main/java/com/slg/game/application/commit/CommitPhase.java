package com.slg.game.application.commit;

/**
 * 提交器内部尝试的观测状态，不是业务状态机。读取到某个阶段不授予调用方修改或取消
 * 任务的资格；实际取消必须使用 CommitHandle 的原子操作。
 */
public enum CommitPhase {
    /** 尚未进入持久化尝试，可以与启动竞争取消；取消成功后必须保证永不执行。 */
    QUEUED,
    /** 已进入事务执行，客户端超时不能再认定为未提交。 */
    EXECUTING,
    /** 是否提交尚无结论；保持对象资格，不能完成 settlement 或重算业务。 */
    RECONCILING,
    /** 存储结果已确定；用例层仍须完成内存发布或失效处理，才能释放对象资格。 */
    SETTLED
}

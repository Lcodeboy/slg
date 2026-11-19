package com.slg.game.application.read;

/**
 * 在冷读工作线程内计算完整结果；查询成功且快照关闭后才交付。返回不可变且有界的领域值，
 * 不返回视图、惰性迭代器、回调或连接，也不提前发布半份资产。ReadBudget 约束存储读取，
 * 不能限制任意应用代码自行分配的大对象；查询本身不得捕获活跃 Player 或无界输入集合。
 */
@FunctionalInterface
public interface SnapshotQuery<T> {
    T read(SnapshotView snapshot) throws Exception;
}

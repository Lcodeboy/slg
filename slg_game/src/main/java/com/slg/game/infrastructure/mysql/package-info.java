/**
 * MySQL 权威存储的端口适配。结构化根和明细由 repository 组装，mapper/row 只负责显式 SQL
 * 与列映射，codec/type 固定版本及整数枚举；事务与 ownership 分别维护原子性和任期屏障。
 *
 * <p>Session 仅属于当前工作线程或快照，配置 RR、SIMPLE、STATEMENT 级本地缓存并关闭二级
 * 缓存和延迟加载。业务按 owner → 有序根/明细 → cursor 取锁，COMMIT 成功后才交回已提交
 * 结果；确认丢失返回 Uncertain，fence 后从权威回执核对。任何适配器都不发布玩家内存。</p>
 *
 * <p>本包不替代 MYSQL-03 的有界队列/对象资格与 MYSQL-06 的启动恢复/续期生命周期。
 * 正常访问只检查独立部署已写入的身份与 schema，不自动建表、换 dataset 或删除旧数据。</p>
 */
package com.slg.game.infrastructure.mysql;

# 区服日志怎样进入 MySQL

玩家收到出征成功时，兵力预留、行军、到达任务和回执已经写入原卷。GAME 的投影线程随后读取这个决定，把相关对象写到 MySQL，供查询、对账使用。玩家立即查询或召回时，GAME 读取本区已提交状态；整个请求路径都不需要借用投影连接。

## 一个决定如何入库

1. GAME 读取 MySQL 中与本区 `realm_id`、原卷 `volume_id` 绑定的 `last_applied_seq`。首次创建的位置为 0。在启动和失联恢复时，若水位大于 0，先重送该位置的原卷日志，比对存档与完整决定的 SHA-256，再续接历史。
2. 从下一序号开始，使用短生命周期的 RocksDB 快照读取一页连续日志。默认每页最多 64 条，编码大小上限 2 MiB。缺号、坏记录或不支持的对象会阻断投影。读取结束就释放原卷快照，随后才借 SQL 连接。
3. 在 InnoDB 事务里锁住该区游标行，写业务事实并核验对象的预期版本。若同一对象在一页内连续变化，保留每条事实，最新对象只需写该页的最终版本。返回给玩家的确定性拒绝也有序号，即使它没有修改任何对象。
4. 所有数据写入成功后，在同一事务更新游标并提交。若收到重复批次，逐条核对已经存档的前缀，只写新后缀。数据库可能已提交但连接在回包前中断，此时 GAME 先重读 SQL 的持久游标。整个过程都不重新计算奖励或战损。

例如，`projection_checkpoint` 为 6 表示本区 1–6 的决定都已投影。读者可按对象键查询最后的快照，或按序号核对每条决定。多个查询要对齐同一水位时，在同一一致性读事务中读取。

| 表 | 可以回答的问题 |
| --- | --- |
| `projection_checkpoint` | 数据来自哪个区、哪块卷？已完整写到哪个序号？ |
| `projection_event` | 每个序号是什么业务事实、哪个请求、结果是什么？按区服与请求作用域去重。 |
| `projection_object` | 指定对象的最新完整快照、版本及来源序号是什么？用于恢复核对及专用表回填。 |
| `projection_player` | 这个区的账号对应哪个角色？角色名、对象版本和来源序号是什么？ |
| `projection_resource_site` | 给定坐标的采集点由谁占用？给定玩家当前占了哪些点？ |

玩家与矿点的快照行在 InnoDB 事务内触发专用表写入，失败时该批的归档、对象和游标一起回滚。GAME 的玩家命令仍只等本地原卷，投影端每批的 SQL 工作量会随变更对象增加。正式查询字段与索引见[数据契约](player-world-model.md)。[事务与去重](../slg_game/src/main/java/com/slg/game/projection/MySqlProjectionStore.java) · [基础 DDL](../scripts/mysql/schema/001-realm-projection.sql) · [专用表迁移](../scripts/mysql/schema/003-role-identity-projections.sql)

## 配置和恢复

本地 Compose 将 UC 与投影分成两个库。使用新版时，在独立的新投影库执行 001、003，不执行旧身份使用的 002。003 完成后才允许新版 GAME 登记卷和投影；已有 002 的开发库保留在原版本。需要验证全链路时，使用下方一次性容器命令。GAME 账号只需写原投影表，对专用表只授予读取权限。触发器及过程的定义者需要持续保有访问相关表和例程的权限；正式环境应使用受控、长期保留的数据库身份。[MySQL 触发器权限说明](https://dev.mysql.com/doc/refman/8.4/en/create-trigger.html)

| 配置 | 行为 |
| --- | --- |
| `SLG_PROJECTION_ENABLED` | 默认 true。纯临时卷演练可显式设为 false；后续开启会从 MySQL 的连续游标补写保留的历史。 |
| `SLG_PROJECTION_JDBC_URL`、`SLG_PROJECTION_DB_USER`、`SLG_PROJECTION_DB_PASSWORD` | 启用时需要 MySQL JDBC 地址及专用密码；用户名默认 slg_projection。 |
| `SLG_PROJECTION_BATCH_SIZE` | 默认 64，允许 1–128。大批次降低事务开销，也会扩大失败时重做的范围。 |
| `SLG_PROJECTION_MAX_LAG` | 默认 100,000 条决定。达到预算时暂缓新客户端写入，保留已确认回执、读取和系统待办。上线前应按实际记录大小、磁盘预算及追赶速度设定。 |

GAME 的 `GET /health` 返回 `journalSeq` 和 `projection`。后者的 `lastAppliedSeq` 是上次确认的 SQL 序号，`lag` 是落后的决定条数；首次连库前两者可以为 null。重启时若 SQL 离线，按水位 0 保守判断新写预算。已受理行军继续返城，因此后台待办可能让总积压超过客户端准入阈值。

常规区服保留全量业务日志。隔离卷首次实际清理时必须留下 SQL 游标对应的 Jc；投影重连先比对这条归档，再从 c+1 补写。若清理后换入过旧的 SQL 备份，缺少锚点或所需日志时投影会阻断，应恢复匹配的备份后再追赶。[清理边界与强杀验收](journal-retention.md)

- **RETRYING** 表示存储暂不可用。投影线程以 250 毫秒至 5 秒的退避重试，GAME 可在预算内继续回复玩家。恢复后会从已核验的位置追平。
- **BLOCKED** 表示检测到错卷、序号缺口或内容冲突等异常。保留现场，根据 `diagnostic` 和最后确认序号核对原卷及投影库，修复后重启 GAME。直接修改游标会掩盖缺失的决定。
- **CATCHING_UP / CAUGHT_UP** 分别表示正在追赶或已追到采样时的本区水位。健康探针读取内存指标，不向 MySQL 发探测 SQL。

## 复验

`python3 scripts/verify-mysql.py` 进行干净构建、真实 SQL 集成测试、四服务恢复验收和投影停库演练。已有新运行包时可执行 `python3 scripts/verify-mysql.py --projection-only` 单独复跑停库场景。故障只注入脚本为本次测试创建的 MySQL 容器。

停库测试会先确认建角已入库，然后关闭 MySQL，通过真实 Gateway 发出采集行军。在 MySQL 离线期间强杀 GAME，从原卷重启并完成返城。之后启动原 MySQL，对照连续业务事实、任务终态、兵力、600 木材和原请求回执，同时验证积压限流在追平后释放。事务集成测试分别在提交之前和提交之后模拟确认丢失，并直接重投同一批及部分重叠的批次。[停库脚本](../scripts/check_realm_projection.py) · [事务集成测试](../slg_game/src/test/java/com/slg/game/projection/MySqlProjectionStoreIT.java)

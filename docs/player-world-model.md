# 玩家与采集点数据契约

本页确定首版角色和采集点的存储键、字段归属、MySQL 查询方式与升级边界，属于 DES-03 的一部分。GAME 以区服原卷裁决出征和占矿。运营查询以 MySQL 已确认的投影水位为准。

## 身份和主键

| 对象 | 身份及作用范围 | 在哪里确认 |
| --- | --- | --- |
| 账号 | 全局 `accountId`，由 UC 的同一张 MySQL 账号表以 `BIGINT AUTO_INCREMENT PRIMARY KEY` 分配，接口和令牌用十进制字符串传输 | GAME 按已认证账号识别请求发起者。 |
| 玩家 | 永久 `playerId = foundingRealmId + "_" + accountId`。当前一个账号在一个区最多一个角色 | 角色资料和资产变更由 GAME 提交，查询时必须带区号。 |
| 采集点 | `(realmId, x, y)`，两个坐标均在 `0..1199`。当前开放点位为 (150,150)、(650,650)、(1150,1150) | 坐标决定采集点身份，更换占用者时保持同一对象和版本链。 |
| 原卷 | `(realmId, volumeId)` | `projection_checkpoint` 将区和唯一卷 UUID 绑定，阻止同一区号的另一份历史接续之前的游标。 |

例如账号 `100086` 在 17 区和 18 区分别建角，得到 `17_100086` 和 `18_100086`。建角区号是全局分配、永久不复用的号码；自增 ID 允许跳号。所有 UC 实例必须写同一账号主库，不能让不同库各自从 1 起发号再汇总。账号 ID 按规范正十进制文本在 JSON、令牌与 RocksDB 之间传递，防止大整数在客户端失真，也避免 `01` 与 `1` 产生两条记录。

角色快照单存 `accountId`、`foundingRealmId` 和 `currentRealmId`。出生时两个区号相同。冷加载从服务端归属目录取回已经确定的角色 ID；行军、矿主、战报和回执引用这个 ID。即使将来角色从 17 区迁到 20 区，身份仍是 `17_100086`。本版没有转服、合服或多区网关选角接口，也不能将“同一账号已有角色”解释为可覆盖的建角。合服时两个同账号角色并入一区需要先定义保留策略，再调整当前区的唯一归属约束。[角色生命周期与请求](realm-march.md)

## 原卷记录

每区独占一块 RocksDB 卷。下表给出业务键。状态键在磁盘上另加共同的 `S` 前缀。值保存对象版本和 JSON 状态。写入者携带预期版本形成变更，提交时统一校验。空区的日志水位从 0 开始，首次变更的对象从版本 0 进入版本 1。

| 业务键示例 | 保存在值里的内容 | 写入边界 |
| --- | --- | --- |
| `account-role:100086` | `playerId`，例如 `17_100086`。 | 建角时两条记录都必须不存在（预期版本 0），同一批次提交为版本 1。以后按已认证账号读归属，再在同一个快照中读角色，登录不等待 MySQL 投影。 |
| `player:17_100086` | `playerId`、`accountId`、`foundingRealmId`、`currentRealmId`、`name`、`troops`、`heroes`、`activeMarchIds`、`resources`、`wounded`、`dead`。每个英雄记录 `heroId` 和当前 `assignedMarch`。 | 出征同批保存玩家兵力和英雄预留、行军、到达任务、日志与原请求回执。返城时，归还兵力、交付货物、释放英雄和行军槽位属于同一决定。 |
| `resource:150-150` | `x`、`y`、`occupantPlayerId`、`occupantMarchId`、`generation`。已占用时两项身份齐全，代次为正数。释放时两项身份均为空，代次归零。 | 首次实际占用才创建记录。采满或从采集中召回，要核对玩家、行军和任务代次，释放与行军、待办一起提交。释放后保留对象版本。 |

设一次出征发生在区服序号 42，玩家对象从版本 6 升为 7。它到矿后在序号 45 首次取得占用，矿点从版本 0 升为 1，登记采集任务代次 2。这里的 45 指全区第 45 条已提交决定，版本 1 表示这个矿点刚刚首次变更，代次 2 用来识别有效的采集待办。其他玩家仍可在 42 到 45 之间提交自己的决定。投影若将同一对象的多次变化合入一页，会保留每条业务事实，并写出该页最终的对象状态。[玩家记录](../slg_game/src/main/java/com/slg/game/realm/ActivePlayer.java) · [矿点记录](../slg_game/src/main/java/com/slg/game/realm/GatherSite.java) · [区服日志](../slg_game/src/main/java/com/slg/game/storage/RealmJournal.java)

## MySQL 查询契约

`projection_event` 保存按区服连续编号的事实与指纹。`projection_checkpoint` 保存本区已完整入库的连续位置。`projection_object` 保存按类型和对象 ID 寻址的完整快照。玩家和矿点的专用表从已校验的对象快照提取查询字段，随原投影在同一 InnoDB 事务提交。每行的 `object_version` 是该对象的版本，`journal_seq` 是最后一次改变该对象的序号。一个空闲已久的矿点可以停在序号 45，同时区服游标已经到 100。

| 表 | 正式列及索引 | 典型查询 |
| --- | --- | --- |
| `projection_player` | 主键 `(realm_id, player_id)`，当前每区 `account_id` 唯一。显式保存 `account_id BIGINT`、`founding_realm_id`、`current_realm_id`、`name`、`object_version`、`journal_seq`。兵力、英雄等完整内容继续保存在通用快照。 | 按账号查本区角色，按角色查名字和最近变更序号。角色名字可以重复。 |
| `projection_resource_site` | 主键 `(realm_id, x, y)`，另有索引 `(realm_id, occupant_player_id, x, y)`。保存 `occupant_player_id`、`occupant_march_id`、`generation`、`object_version`、`journal_seq`。 | 按坐标查当前矿主，或按玩家查其当前占用的全部矿点。 |

以下示例先查 2 区账号 `100086` 的角色，再查这个角色占用的矿点。第一条使用按账号建立的唯一索引。单格核查可直接使用末条语句。

```sql
SELECT player_id, founding_realm_id, current_realm_id, name, object_version, journal_seq
FROM projection_player
WHERE realm_id = 2 AND account_id = 100086;

SELECT x, y, occupant_march_id, generation, journal_seq
FROM projection_resource_site
WHERE realm_id = 2 AND occupant_player_id = '2_100086'
ORDER BY x, y;

SELECT occupant_player_id, occupant_march_id, generation, object_version, journal_seq
FROM projection_resource_site
WHERE realm_id = 2 AND x = 150 AND y = 150;
```

空闲矿点要区分两个场景。静态配置中的点位可能从未被占用，因此专用表中还没有该行。已经释放的点位则有一行空占用，保留版本和最后变更序号。列举地图上全部合法资源点，应从静态点位取全集，再附上达到相同消费水位的动态占用。

运营需要同时展示玩家和矿点时，在同一个一致性读事务中查询玩家、占用和区服游标，注明数据截止序号。在线玩家查刚刚派出的行军、撤回部队或抢占矿点，始终走 GAME。增设查询表会增加 MySQL 每批解析和维护索引的成本。上线容量验收应测积压追平速度及回填页持锁时间。[容量验收](high-load-design.md)

### 部署和旧数据边界

新部署使用独立的 UC 账号库、格式 8 的 GAME 卷以及应用 001 + 003 的投影库。003 检查角色键、账号和建角区是否相符，并核对当前区与来源卷一致。投影更新只随通过检查的快照、事件及消费水位在同一事务提交。可重复部署相同基线；新库**不执行**历史迁移 002。[投影配置与恢复](mysql-projection.md) · [身份投影 DDL](../scripts/mysql/schema/003-role-identity-projections.sql)

老 UC 表的 UUID 没有可推导的自增值，旧角色 ID 也不能直接加前缀充当新 ID。新版拒绝使用旧账号表和旧身份卷提供游戏服务，旧投影库也不能与新版混写。若决定承接历史数据，必须先在副本上建立账号及角色映射，核对所有行军、占矿、任务、战报、历史回执和 SQL 消费位置，再单独安排切换。只改某张 SQL 表或改卷格式号会破坏引用。隔离新区不得复用旧环境的签名密钥、卷或 UC 发号空间。

## 城市多格占用仍属 WORLD-03

后续城市和设施可有多格范围。草案按 `(realmId, x, y)` 唯一登记占地对象，城市持有独立对象 ID 和位置代次。迁城取旧、新范围的并集，按统一次序协调所有涉格，校验目的地及当前代次，在一个本地批次释放旧格、占用新格并推进城市位置。交叠格继续归该城市所有。若任一新格冲突，整个迁城保持原位置。到达旧坐标的攻击须重查目标身份与位置代次。具体占地范围与到达规则仍需 DES-02 和 WORLD-03 验收。本版查询表仅覆盖当前采集点。[世界任务依赖](production-task-plan.md)

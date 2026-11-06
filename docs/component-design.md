# 服务端组件与调用边界

| 组件 | 入口与执行 | 状态归属 |
| --- | --- | --- |
| Gateway | Netty 分别监听账号 HTTP 和 Unity 游戏 TCP v2。游戏连接先用 TLS 保护令牌，再通过首帧绑定账号。到 UC、GAME 分别复用 gRPC Channel，调用有截止时间与并发预算 | 连接、流控和短期在途请求 |
| UC | 通过 `AccountRpc` 提供注册、登录、区服目录 | MySQL 账号库；HikariCP 管理 JDBC 连接 |
| GAME | `GameRpc.Play` 验签后按账号进入有界邮箱。冷加载、玩家逻辑、到期扫描和战斗派发分别执行 | 一区一写者，RocksDB 原子提交；内存只发布已提交的玩家版本；独立投影池按日志序号更新 MySQL 游戏查询表 |
| Battle Worker | 独立 gRPC 接受固定输入、摘要和规则版本，在有界计算池执行，返回确定性结果 | 无状态。只有 GAME 可以结算和发放资产 |

`slg_rpc` 定义服务协议与传输配置，`slg_core` 提供 HTTP 工具、鉴权及纯算法。NPC 战斗使用独立的类型化计算接口，Worker 不接受玩家令牌。跨机调用强制双向 TLS。Worker 使用自己独立的 `SLG_BATTLE_GAME_CLIENT_CA` 验证 GAME 证书；GAME 使用 `SLG_RPC_TLS_CA` 验证 Worker，并提供 `SLG_RPC_CLIENT_TLS_CERT` / `SLG_RPC_CLIENT_TLS_KEY`。为 GAME 客户端专设签发链，仅向本区允许计算的 GAME 发证。本地启动器通过 `SLG_BATTLE_CLIENT_TLS_CERT` / `SLG_BATTLE_CLIENT_TLS_KEY` 指定这对 GAME 凭证，与网关的客户端凭证分开。[Unity 网关协议](../slg_rpc/src/main/proto/slg_gateway.proto) · [内部服务协议](../slg_rpc/src/main/proto/slg_rpc.proto) · [Worker 协议](../slg_rpc/src/main/proto/slg_battle.proto)

## 玩家请求和后台工作

Gateway 复用目标服务的连接。GAME 首次访问在加载池读原卷，同账号的并发请求共享这一次加载。玩家自己的命令、行军定时器和战斗结算都通过同一玩家邮箱，业务线程之间按账号分流。每次提交验证对象版本，并在区服写锁内分配连续序号。独立的 Worker 可以计算多场战斗，任何一场的 RPC 等待都不占用玩家邮箱。[玩家目录](../slg_game/src/main/java/com/slg/game/realm/ActivePlayers.java) · [区服提交](../slg_game/src/main/java/com/slg/game/storage/RealmJournal.java)

GAME 使用 `SLG_BATTLE_RPC_TARGET` 指定 Worker，默认 `127.0.0.1:9083`。`SLG_REALM_BATTLE_INFLIGHT` 默认 16，限制单 GAME 的计算和待结算总数。`SLG_BATTLE_WORKERS` 默认取 8 与 CPU 数中的较小值，`SLG_BATTLE_QUEUE_CAPACITY` 默认 64。`SLG_BATTLE_RPC_TIMEOUT_MS` 默认 3000。发生 Worker 连接失败或计算队列满时，区服派发短暂退避。一个玩家邮箱满或结果无效时，只延后该战斗。积压时按在途名额连续翻页，RPC 等待不占玩家业务线程。GAME 健康端口返回卷身份、水位、玩家队列、到期任务和战斗派发指标。[战斗派发](../slg_game/src/main/java/com/slg/game/realm/RealmBattlePump.java)

## MySQL 投影

玩家命令与待办只提交 RocksDB。区服投影线程在独立单连接 HikariCP 池中写 MySQL，失败时按原卷重试。GAME 健康端口在 `projection` 中公布连续水位、积压和失败原因。[事务、积压与恢复](mysql-projection.md)

## 部署进度

当前启动方式按配置连接固定目标。生产目标包括 etcd 所有权目录、原卷排他接管、独立 AOI 事件消费和推送流、日志清理及支付服务。上线要求以独立功能和故障验收衡量。[任务清单](production-task-plan.md)

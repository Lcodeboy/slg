# SLG 服务端

这是一个 Java 21 的单区 SLG 服务端。Unity 原生客户端通过 HTTPS 注册、登录及获取区服，通过独立的 TCP/TLS 长连接收发游戏命令。Gateway 复用内部 gRPC 通道调用 UC 和 GAME。UC 使用 MySQL 和 HikariCP 保存账号。每个区服只有一个 GAME 写者，使用 RocksDB 保存玩家、行军、待办、战报与请求回执。GAME 后台按区服日志序号，将事实、对象快照和连续进度写入独立的 MySQL 投影库。Battle Worker 独立运行，只接收固定快照并返回计算结果。

现有玩法覆盖建角、派兵、召回、采集点争用、NPC 战斗和返城。GAME 重启后从登记的原卷继续未完成的任务。MySQL 停机时，区服在积压预算内继续受理游戏操作。地图广播和跨玩家战斗列在后续任务中。[当前接口](docs/api.md) · [实现状态](docs/implementation-status.md) · [生产架构与边界](docs/server-architecture.md) · [按序任务清单](docs/production-task-plan.md)

账号 ID 由 UC 的 MySQL 自增主键分配；角色 ID 由建角区号和账号 ID 组成，例如 `17_100086`，当前区号另存。运营可按区服与账号查询角色，按玩家或坐标查询采集点占用。完整对象快照仍用于对账。[玩家与采集点表结构](docs/player-world-model.md)

## 在 IntelliJ IDEA 生成 gRPC Java 代码

用 IDEA 打开根目录的 `pom.xml` 并选用 JDK 21。在右上角运行配置下拉框选择 **生成 gRPC Java 代码**，点击 ▶。修改 `slg_rpc/src/main/proto` 下的协议后，再运行同一配置即可。

该配置等价于从根项目运行 `./mvnw -pl slg_rpc -am compile`。一次运行生成 Java 协议、GAME 分发、Unity 调用代码和 Python 验收协议。以下目录均位于 `slg_rpc/target/generated-sources` 下。

- `protobuf/java` 包含消息类和 `GameplayRoutes`，后者将消息编号对应的请求解析后交给 GAME 的具名处理方法。
- `protobuf/grpc-java` 包含内部 gRPC 服务端基类和客户端 stub，例如 `GameRpcGrpc`。
- `unity` 包含 C# 消息类和 `SlgGameplayApi.g.cs`。把它们与 `clients/unity/SlgTcpClient.cs` 放在 Unity 的同一程序集，即可使用七个生成的玩法接口。[接口与协议声明](docs/api.md) · [接入与复验](clients/unity/README.md)
- `python` 包含压测和故障演练读取的公开消息类。验收脚本从该目录载入本次构建的操作描述符。

首次生成后，若编辑器里的类名仍标红，打开右侧 Maven 工具窗口，点击 **Reload All Maven Projects**。[共享运行配置](.idea/runConfigurations/Generate_gRPC_Java.xml)

## 新区服数据卷

本次发布使用 TCP v2，并同时更新 Unity、Gateway、UC 和 GAME。首次部署应为新区分配独立的 UC 数据库、投影库、空卷目录和固定卷 UUID。GAME 新建卷格式为 8，完成首次前缀清理后为 9。已有开发原卷仅用于只读观测。以下示例用于全新环境，日常复验可使用文末的一次性 MySQL 脚本。

```sh
./mvnw -DskipTests package
export SLG_REALM_DIR="$PWD/data/realm-v2-1"
export SLG_REALM_ID=1
export SLG_REALM_VOLUME_ID="$(uuidgen)"
mkdir -p "$PWD/data"
mkdir "$SLG_REALM_DIR"
game_jar=slg_game/target/slg_game-1.0-SNAPSHOT.jar
java -jar "$game_jar" volume init "$SLG_REALM_DIR" "$SLG_REALM_ID" "$SLG_REALM_VOLUME_ID"
java -jar "$game_jar" volume check "$SLG_REALM_DIR" "$SLG_REALM_ID" "$SLG_REALM_VOLUME_ID"
```

把这三项的实际值保存到启动环境或 `data/local.env`。后续启动复用原目录和 UUID。缺卷、格式错误、身份不匹配或卷已占用时 GAME 拒绝启动。仅对本次创建的卷执行 `volume check`。[区服卷与恢复语义](docs/realm-march.md)

## 本地运行

准备 Java 21、Python 3、Docker Compose。本地 `slg-mysql-dev` 同时提供 UC 账号库和独立的 `slg_projection` 游戏查询库，默认监听回环地址 13306。启动器读取 `data/mysql.env`，本地共享签名密钥保存于 `data/local.env`。首次配置文件由工具创建，已有环境可以通过 `--env-file` 选择配置。[Compose 配置](compose.mysql.yaml) · [环境示例](.env.mysql.example)

```sh
./scripts/mysql-local.sh start
./scripts/mysql-local.sh provision
./scripts/start-local.sh --no-build
./scripts/status-local.sh
```

| 服务 | 默认端口 | 用途 |
| --- | --- | --- |
| Gateway | 8080 / 8090 | 账号与区服 HTTP / Unity 游戏 TCP，本机回环可用明文 |
| UC | 8081 / 9081 | 健康检查 / 账号 gRPC |
| GAME | 8082 / 9082 | 区服健康检查 / 游戏 gRPC |
| Battle Worker | 9083 | 内部战斗计算 gRPC |

`./scripts/stop-local.sh` 停止本地应用；`./scripts/mysql-local.sh stop` 停止本地 MySQL。上方命令仅适用于新身份基线；UUID 账号表、旧投影与旧身份卷不能与新版混用。[组件与服务间调用](docs/component-design.md)

## 磁盘保护与日志预演

GAME 按原卷可用空间限制新写。`python3 scripts/preview-realm.py` 可以只读查看开发原卷的日志首尾、已清边界和 SQL 消费位置。真实删除只在集成测试新建的卷上执行，由 `python3 scripts/verify-mysql.py` 复验 MySQL 落后、在途强杀和返城幂等。[阈值、清理边界及故障场景](docs/journal-retention.md)

## Unity 接入

客户端从登录响应取令牌，连接游戏端口并完成首帧鉴权，再发送建角、行军等命令。线上账号入口配置 HTTPS，游戏端口在 Gateway 配置 TLS 证书，可接四层 TCP 负载均衡。Unity 需校验游戏域名及证书链。[C# 接入示例](clients/unity/README.md) · [消息协议](slg_rpc/src/main/proto/slg_gateway.proto)

## 验证

运行 Python 游戏客户端、故障验收和压测前，先生成协议并在虚拟环境安装运行库。下文示例使用已激活的环境。

```sh
./mvnw -pl slg_rpc -am compile
python3 -m venv .cache/tcp-python
. .cache/tcp-python/bin/activate
python3 -m pip install -r scripts/requirements.txt
```

```sh
./mvnw clean verify
python3 scripts/check-realm-march.py
python3 scripts/verify-mysql.py
python3 -m unittest discover -s scripts -p 'test_*.py'
```

强杀脚本使用临时端口和专用卷，通过真实网关验证离线采集、召回、NPC 等待 Worker、GAME 强杀重启、伤亡核对和奖励只交付一次。MySQL 集成脚本还会在一次性容器上启动真实 UC，验证四进程中的注册、登录和同一恢复场景。同一脚本还实际停止临时 MySQL，验证 GAME 离线推进行军、按序追平、重复消费和积压保护。只复跑停库场景可在构建后执行 `python3 scripts/verify-mysql.py --projection-only`。[投影配置与排障](docs/mysql-projection.md)

单区 1 万在线、峰值 2,000 条业务命令/秒是待实测的容量目标。[负载验收方案](docs/high-load-design.md)

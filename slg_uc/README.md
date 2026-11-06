# SLG User Center

账号中心通过 gRPC + Protobuf 提供内部注册、登录和区服目录，使用 MySQL 8.4 / InnoDB 与独立 HikariCP 池持久化账号。JDK HttpServer 只保留健康检查。账号密码使用独立随机盐和 PBKDF2-HMAC-SHA256 派生值存储。

客户端继续访问 Gateway 的 HTTP JSON 接口；Gateway 将认证请求转换为 `AccountRpc` 消息，默认调用内部端口 `9081`。`8081` 仅提供健康检查，不再提供注册、登录或区服业务 HTTP 路由。内部协议定义位于 `slg_rpc` 模块。

全部 UC 实例指向同一 MySQL 账号写库。`uc_account.account_id` 是带符号的 `BIGINT AUTO_INCREMENT PRIMARY KEY`，注册由 MySQL 分配正整数，在同一次 INSERT 上取回主键。账号 ID 允许跳号，登录根据用户名查出原来的 ID。接口与签名令牌都将账号 ID 编码为十进制字符串，浏览器可以准确保存超过 JavaScript 安全整数上限的 ID。

本地默认地址指向 `slg-mysql-dev`。新库在首次启动时建表。已有 UUID 账号表会阻止新版 UC 启动，以保留已有账号和 GAME 角色之间的关系。试跑新版时应使用独立的 UC 库、GAME 卷和投影库。若需承接旧数据，切换前必须在副本上建立并复核旧 UUID 到新账号 ID 的唯一映射，连同角色、行军、回执等引用一并完成迁移。启动器默认读取 `data/mysql.env`。

也可在 `./mvnw package` 后单独运行 `java -jar slg_uc/target/slg_uc-1.0-SNAPSHOT.jar`。Java 不会自动读取环境配置文件，须通过进程环境提供数据库密码及至少 32 UTF-8 字节的 `SLG_TOKEN_SECRET`，签名密钥须与 Gateway、GAME 一致。登录会话有效期为 12 小时，令牌只包含账号身份、签发与到期时间，角色归属由 GAME 查询。

| 环境变量 | 默认值 |
| --- | --- |
| `SLG_BIND` | `127.0.0.1` |
| `SLG_UC_PORT` | `8081`，仅 HTTP 健康检查 |
| `SLG_RPC_BIND` | 未设置时沿用 `SLG_BIND` |
| `SLG_UC_RPC_PORT` | `9081`，内部 gRPC 业务 |
| `SLG_UC_JDBC_URL` | MySQL `127.0.0.1:13306/slg_uc`，完整 JDBC 参数见根目录 `.env.mysql.example` |
| `SLG_UC_DB_USER` | 先取服务专属配置，再取 `SLG_DB_USER`，均未设置时为 `slg_uc` |
| `SLG_UC_DB_PASSWORD` | 先取服务专属配置，再取 `SLG_DB_PASSWORD`；MySQL 无内置密码 |
| `SLG_DB_POOL_SIZE` | `8`，范围 `1..64` |
| `SLG_DB_CONNECTION_TIMEOUT_MS` | `5000` 毫秒，范围 `250..60000` |
| `SLG_DB_MINIMUM_IDLE` | 等于 `SLG_DB_POOL_SIZE`，范围 `0..最大连接数` |
| `SLG_DB_VALIDATION_TIMEOUT_MS` | `max(250, min(1000, connectionTimeout / 2))`，至少 250 且不大于获取超时 |
| `SLG_DB_MAX_LIFETIME_MS` | `1800000` 毫秒，范围 `30000..86400000` |
| `SLG_DB_KEEPALIVE_MS` | `120000` 毫秒；0 表示关闭，否则至少 30000 且小于最长寿命 |

只接受 MySQL JDBC 地址；缺少密码、空账号或连接池参数不合法时立即报错。账号库采用 InnoDB 和 `utf8mb4_0900_bin`，连接通过 UC 自有的有界 HikariCP 池复用。池获取超时与连接校验超时不表示整个业务的执行时限。生产环境应将连接最长寿命设置得短于数据库或网络设备的连接回收时间。

MySQL 驱动开启预编译语句缓存与服务端预编译，缓存上限为每连接 250 条、SQL 长度 2048 字符，并复用驱动记录的会话状态。这些设置来自 [HikariCP 官方 MySQL 配置建议](https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration)，实际容量仍须结合工作负载测量。当前自动建表适合开发环境，生产变更应建立版本化迁移流程。

显式 `--env-file` 完整替换默认 MySQL 配置文件；自定义配置优先级为进程环境、选定文件、`data/local.env`，根目录 `.env` 需显式指定才读取。

`GET http://127.0.0.1:8081/health` 返回进程状态，不借用数据库连接执行探测。GAME 对应的健康和 gRPC 端口分别为 `8082`、`9082`。

内部 `AccountRpc.Register` 与 `AccountRpc.Login` 接受 `AuthRequest(username, password)`；`AccountRpc.ListRealms` 接受空消息，并校验 `authorization` 元数据中的 Bearer 令牌。用户名为 3–32 个字母、数字或下划线，统一转小写；密码为 8–128 个字符。注册和登录返回 `AuthReply`，区服目录返回 `RealmsReply`。成功正文和业务错误各占一个 oneof 分支，Gateway 根据 `status` 回复 HTTP 状态。

外部客户端仍向 Gateway 的 `POST /api/auth/register` 和 `POST /api/auth/login` 提交 `{"username":"alice","password":"your-password"}`。注册成功为 HTTP 201，登录成功为 HTTP 200，响应结构为 `{"success":true,"data":{"accountId":"100086","token":"...","expiresAt":0},"error":null}`，其中 `expiresAt` 为 UTC 秒级时间戳。重复用户名为 409、错误凭证为 401、参数错误为 400、过载为 429 或 503；`GET /api/realms` 仍由 Gateway 暴露并要求 Bearer 认证。

UC 按用户名限制认证尝试，并限制同时执行的密码派生任务；Gateway 另按客户连接 IP 限流，避免所有用户经同一个网关访问 UC 时共享很小的登录配额。令牌目前为无状态 HMAC 会话，不包含单会话吊销或踢下线功能；应通过后续账号会话版本机制补充。

内部默认仅供本机开发使用明文 gRPC。生产部署须配置 `SLG_RPC_TLS_CERT`、`SLG_RPC_TLS_KEY`，客户端通过 `SLG_RPC_TLS_CA` 验证服务端；配置双向证书时还需客户端证书和私钥。端口隔离、证书分发、账号入口的 HTTPS 终止及游戏 TCP 入口的 TLS 配置属于部署要求，详见 [组件设计](../docs/component-design.md)。

数据库测试统一使用 `slg_test_support`：优先复用 `scripts/verify-mysql.py` 注入的隔离测试服务器，每次创建随机 schema；无测试环境变量时启动临时 MySQL 8.4 Testcontainers 容器。缺 Docker、配置不完整或数据库不可达都会失败，不跳过测试，也不读取开发库凭证文件。账号重启后登录、密码派生材料和连接归还测试保留，并改为真实 MySQL / gRPC 链路。

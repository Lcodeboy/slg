# SLG Gateway

Gateway 使用 Netty 监听两个端口。Unity 先在账号入口注册、登录并获取区服，随后连接游戏端口。每次重新建立 TCP 连接都要用有效令牌鉴权。GAME 从网关转交的令牌中独立确认角色归属。

| 配置 | 默认值或用途 |
| --- | --- |
| `SLG_BIND` / `SLG_GATE_PORT` | 账号与区服 HTTP，默认 `127.0.0.1:8080`。部署时在可信入口终止 HTTPS |
| `SLG_GATE_TCP_HOST` / `SLG_GATE_TCP_PORT` | 游戏 TCP，默认沿用 `SLG_BIND`，端口 8090 |
| `SLG_GATE_TCP_TLS_CERT` / `SLG_GATE_TCP_TLS_KEY` | 游戏域名证书链及对应 PEM 私钥。非回环监听须成对配置，网关终止 TLS |
| `SLG_UC_RPC_TARGET` / `SLG_GAME_RPC_TARGET` | 默认 `127.0.0.1:9081` / `127.0.0.1:9082` |
| `SLG_TOKEN_SECRET` | Gateway、UC、GAME 共享签名密钥，至少 32 个 UTF-8 字节 |

运行 `./mvnw package` 后可用 `java -jar slg_gate/target/slg_gate-1.0-SNAPSHOT.jar` 启动。HTTP 提供 `GET /health`、`POST /api/auth/register`、`POST /api/auth/login` 和需 Bearer 认证的 `GET /api/realms`。公网登录接口应经 HTTPS 转发到受限的网关 HTTP 端口。游戏端口可由四层负载均衡透传 TLS。限流使用连接对端 IP，经过代理时须按实际可见来源评估共享配额。

## 游戏连接

TCP v2 线上帧为 4 字节大端长度和一条二进制 `GatewayFrame`。客户端正文上限为 65,536 字节，服务端正文上限为 1,048,592 字节。连接建立后 10 秒内发送 `auth`，收到 `auth_ok` 后才开始发命令。每个连接同时允许一个在途普通命令。接到服务器 `ping` 应立即回显带相同 nonce 的 `pong`，包括当前没有游戏操作的时候。网关在写空闲 20 秒时探活，读空闲 60 秒时断开。[协议与业务示例](../docs/api.md) · [Unity 示例](../clients/unity/README.md)

UC 和 GAME 分别复用 gRPC 通道，默认在途预算为 32 和 256。外部两个端口合计最多接纳 12,000 条连接，心跳、命令和认证分别限流。慢接收者、鉴权失败或过期会话会被断开。断连取消下游等待，GAME 可能已提交操作。带兵出征的客户端应保存原请求 ID，重连查询原回执，需要重发时使用相同 ID 和参数。[负载与故障验收](../docs/high-load-design.md)

七种游戏操作共用 `play_request`。Gateway 验证会话、请求 ID 和协议生成的消息号白名单，经 `GameRpc.Play` 透传二进制正文。GAME 按协议选择具体请求类型并返回对应响应。返回时 Gateway 核对请求 ID、消息号与结果分支。账号端口通过独立的 `AccountRpc` 获取会话或区服目录，并在 HTTP 边界转换成 JSON。[游戏及账号接口](../docs/api.md)

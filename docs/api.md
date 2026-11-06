# 客户端 API

Unity 通过 HTTPS 注册、登录并获取区服，再携带登录令牌连接游戏 Gateway。开发机默认账号入口为 `http://127.0.0.1:8080`，游戏地址为 `127.0.0.1:8090`。

| HTTP 接口 | 输入与结果 |
| --- | --- |
| `POST /api/auth/register` | JSON 用户名 `username`、密码 `password`。成功返回 201 和会话。 |
| `POST /api/auth/login` | 相同凭据。成功返回 200 和会话。 |
| `GET /api/realms` | 使用 Bearer 令牌查询区号、名称和开放状态。当前游戏域名、端口由 Unity 启动配置提供。 |
| `GET /health` | Gateway 进程状态。 |

HTTP 响应包含 `success`、`data`、`error`。登录数据中的 `accountId` 是十进制字符串，`token` 用于游戏连接鉴权，`expiresAt` 是 Unix 秒。[账号协议](../slg_rpc/src/main/proto/slg_uc.proto)

## 游戏 TCP v2

1. 生产连接使用 TLS 并校验证书及游戏域名。建连后 10 秒内发送 `GatewayFrame(version=2, auth={token})`，收到 `auth_ok` 后开始游戏操作。
2. 每帧先写 4 字节无符号大端长度，随后写 Protobuf 正文。正文最小 1 字节，Unity 发送上限为 65,536 字节，接收上限为 1,048,592 字节。
3. 每条业务请求使用 `play_request`，包含操作编号 `msg_id`、业务请求 ID `request_id` 和该操作的序列化请求。响应 `play_reply` 回显两个标识，通过 `outcome` 区分操作响应和业务错误。消息号和对应的请求、响应类型来自同一条协议方法声明。
4. 同一 TCP 连接最多有一个在途业务请求。等候业务回包期间收到 `ping` 应立即发送带相同 nonce 的 `pong`。收到连接级 `error` 后结束本次连接。

`request_id` 长度为 1–64，可用 ASCII 字母、数字、连字符和下划线。每次写操作在发送前保存请求 ID 和完整参数，断线重发时使用同一份。读操作也需提供自己的请求 ID。Gateway 验证会话和消息编号后，通过 `GameRpc.Play` 转交原始业务正文，GAME 以认证账号选择玩家邮箱。[TCP 帧](../slg_rpc/src/main/proto/slg_gateway.proto) · [游戏 RPC](../slg_rpc/src/main/proto/slg_rpc.proto) · [Unity 接入](../clients/unity/README.md)

## 七个玩法操作

每个操作都有独立的 `Req`、`Rsp` 类型和生成的 C# 方法。下表使用 C# 属性名。

| 方法 | 请求类型与输入 | 响应类型与数据 |
| --- | --- | --- |
| `CreatePlayerAsync` | `CreatePlayerReq.Name` | `CreatePlayerRsp.Player` 是建角时的玩家快照。 |
| `GetPlayerAsync` | `GetPlayerReq`，空正文 | `GetPlayerRsp.Player` 含当前身份、可用兵、英雄、行军 ID、资源、伤亡和对象版本。 |
| `StartMarchAsync` | `StartMarchReq` 指定目标坐标、兵种和数量，可选任务、阶级和英雄 | `StartMarchRsp.March` 是首次确认出征时的快照，包含行军 ID、出发与到达时间。 |
| `GetMarchAsync` | `GetMarchReq.MarchId` | `GetMarchRsp.March` 是本人该行军的当前阶段、兵力与货物。 |
| `RecallMarchAsync` | `RecallMarchReq.MarchId` | `RecallMarchRsp.March` 是召回提交时的返程快照。 |
| `GetBattleAsync` | `GetBattleReq.BattleId`。NPC 战斗使用行军 ID。 | `GetBattleRsp` 含状态、规则版本和输入摘要。结算后 `Result` 提供胜负、种子、回合数和双方每组兵力损失。 |
| `GetReceiptAsync` | `GetReceiptReq.TargetRequestId` 是想找回的原写请求 ID | `GetReceiptRsp` 返回该角色对应的已提交决定及其区服日志序号。 |

[玩家协议](../slg_rpc/src/main/proto/slg_player.proto) · [行军协议](../slg_rpc/src/main/proto/slg_march.proto) · [战报协议](../slg_rpc/src/main/proto/slg_report.proto) · [回执协议](../slg_rpc/src/main/proto/slg_receipt.proto)

兵力、时间和日志序号均保留 64 位整数精度。行军出发及到达时间使用 Unix 毫秒。省略 `Heroes` 会选择默认英雄，显式传入空 `HeroSelection` 表示无英雄出征。玩家身份由会话确定，例如账号 `100086` 在 17 区建角后取得角色 ID `17_100086`，当前区号保存在 `CurrentRealmId`。[游戏规则](realm-march.md)

## 断线后找回写入结果

建角、出征和召回在 GAME 原卷提交后确认。同一区服、同一角色使用相同 ID 和内容重发会得到原回执，同 ID 改参返回 `REQUEST_CONFLICT`。查询出征回执取得出发时的快照，即使该队伍已返城。

```csharp
var query = new GetReceiptReq { TargetRequestId = originalRequestId };
GetReceiptRsp found = await client.GetReceiptAsync(Guid.NewGuid().ToString("N"), query, cancellation);
if (found.Receipt?.OutcomeCase == WriteReceipt.OutcomeOneofCase.Started) {
    string marchId = found.Receipt.Started.March.MarchId;
    GetMarchRsp current = await client.GetMarchAsync(Guid.NewGuid().ToString("N"),
        new GetMarchReq { MarchId = marchId }, cancellation);
}
```

- `Receipt` 缺失表示本次读取尚无持久决定。另一连接上的原请求可能仍在途，稍后可再查或用原 ID 和内容重发。
- 有回执时，`JournalSeq` 是该决定的区服序号。`Receipt.MsgId` 对应原写操作，`OutcomeCase` 为 `Created`、`Started`、`Recalled` 或 `Error`。失败分支中保留当时的错误码。
- `SERVER_BUSY`、`PLAYER_LOAD_UNAVAILABLE`、`TARGET_LOAD_UNAVAILABLE` 表示未进入业务执行，可退避重试。写入受 `DISK_PRESSURE` 或 `PROJECTION_BACKLOG` 限制时仍可查询已有回执。
- 断线、上游超时或 `RESULT_UNKNOWN` 表示本次操作可能已经提交。重连到原区服并查询原请求，避免更换 ID 重复派兵。[磁盘保护](journal-retention.md) · [MySQL 投影](mysql-projection.md)

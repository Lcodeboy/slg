# Unity 原生客户端接入

## 生成 C# 协议

在服务端根目录运行 `./mvnw -pl slg_rpc -am compile`，也可以在 IDEA 使用 **生成 gRPC Java 代码** 配置。将 `slg_rpc/target/generated-sources/unity` 中的 C# 文件和本目录的 `SlgTcpClient.cs` 放入 Unity 的同一个网络程序集，并引入 `Google.Protobuf` 3.25.9 及其依赖。客户端适用于 .NET Standard 2.1 的原生 Unity 工程。

生成目录包含公开的 Protobuf 消息类和 `SlgTcpClient` 的七个玩法方法。每个方法的输入、输出和编号由同一行协议声明生成。将该目录整组替换到 Unity 工程，避免混用不同发布批次的脚本。Unity、Gateway、UC 和 GAME 同批使用 v2 接口。[操作列表及协议源文件](../../docs/api.md)

## 发起行军

先从账号登录接口取得 `loginToken`。游戏网关的 `gameHost` 和 `gamePort` 由 Unity 启动配置提供。正式连接使用与设备信任证书匹配的游戏域名。下面的代码放在异步游戏会话中。

```csharp
using Slg.Unity;
using Slg.Rpc.V1;
using System;
using System.Threading;

using var connectDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(8));
using var client = await SlgTcpClient.ConnectAsync(gameHost, gamePort, loginToken, connectDeadline.Token);

var request = new StartMarchReq {
    Mission = MarchMission.Gather, TargetX = 150, TargetY = 150,
    Troops = { new TroopGroup { Type = TroopType.Infantry, Count = 120 } }
};
// 一次出征只分配一个 ID。实际客户端在发送前将它和请求内容保存到本地，以便中断后恢复。
string requestId = Guid.NewGuid().ToString("N");
using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try {
    StartMarchRsp receipt = await client.StartMarchAsync(requestId, request, deadline.Token);
    ShowMarch(receipt.March.MarchId, receipt.March.ArriveAt);
} catch (GameplayException error) {
    ShowCommandError(error.Code);
}
```

`StartMarchReq.Heroes` 留空表示使用默认英雄，赋值为 `new HeroSelection()` 表示明确不带英雄。兵力使用整数，时间戳使用 Unix 毫秒。生成的方法返回 GAME 首次确认出征时的快照。想了解行军当前阶段时调用 `GetMarchAsync` 并读取 `GetMarchRsp.March`。

每条连接同时等待一条业务回包。发生业务错误时，`GameplayException` 提供原请求 ID、错误码和状态。收到网关连接级错误时，`GatewayConnectionException.Code` 提供原因。取消在途请求会关闭当前连接，以免把迟到的响应归给后来的操作。

写入前应保存请求 ID 和 `StartMarchReq` 的完整内容（如 `request.ToByteArray()`，需要 `using Google.Protobuf`）。断线、命令截止或 `RESULT_UNKNOWN` 等结果未知时，重新认证到同一区服。可以用原 ID 和原参数重新调用 `StartMarchAsync`，或通过 `GetReceiptAsync` 查询原回执。`REQUEST_CONFLICT` 表示这个 ID 已经绑定到不同内容。[回执查询与其他玩法命令](../../docs/api.md)

## 线格式和连接管理

每帧由 4 字节无符号大端长度和一份 `GatewayFrame` 构成，长度只计算 Protobuf 正文。客户端发送上限 65,536 字节，接收上限 1,048,592 字节。读循环先核对长度，再完整读取正文，同时处理半包、连续多帧和心跳。

| 阶段 | 协议 |
| --- | --- |
| 建连 | `version=2, auth.token`，等待 `auth_ok.account_id` 和 `auth_ok.expires_at`（Unix 秒）。网关给首帧鉴权 10 秒。 |
| 玩法 | 建角、玩家读取、出征、行军查询、召回、战报和原回执查询都使用 `play_request`。生成的客户端填入消息编号并编码该操作的具体请求。成功时按原 `msg_id` 和 `request_id` 取回对应的具体响应，错配将关闭连接。 |
| 心跳 | 网关写空闲 20 秒发 `ping`，客户端立即回相同 nonce 的 `pong`。网关读空闲 60 秒会断开。 |
| 退出或后台 | 游戏退出时释放连接。进入后台前保存尚未确认的写请求。网络线程不调用 Unity API，界面更新由游戏主线程处理。 |

本机开发时可以显式使用 `ConnectLoopbackForDevelopmentAsync("127.0.0.1", port, token, deadline.Token)`。移动真机联调使用 TLS 入口。

## 复跑生成的客户端 API 验收

准备 .NET 8 SDK，激活仓库 README 说明的 Python 验收环境，然后在仓库根目录运行以下命令。

```sh
./mvnw -DskipTests package
dotnet build scripts/unity_march_smoke/UnityMarchSmoke.csproj --artifacts-path .cache/dotnet-march
python3 scripts/check-realm-march.py --unity-probe .cache/dotnet-march/bin/UnityMarchSmoke/debug/UnityMarchSmoke.dll
```

该探针在独立临时卷上通过七个生成的方法完成建角、出征、查询和召回。它派出 120 名步兵，重连后重发，核对原回执、改参冲突及一次性到账的 600 木材。随后召回一支远矿队伍，再派 650 名步兵攻击 NPC 并读取已结算战报。驱动脚本用另一条连接复核三支队伍的终态、战损和最终库存。

要用 UC 实际注册并登录的账号复验，可在构建探针后运行 `python3 scripts/verify-mysql.py --unity-probe .cache/dotnet-march/bin/UnityMarchSmoke/debug/UnityMarchSmoke.dll`。该命令会启动一次性 MySQL，并运行四服务验收。

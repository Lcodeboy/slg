using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Slg.Rpc.V1;
using Slg.Unity;

// 真实本机 TCP 与固定跨语言黄金向量，验证半包、关联、推送、取消和地图流控，不访问开发原卷。
static class Program
{
    private static readonly MapViewport Center = new MapViewport { MinX = 590, MinY = 590, MaxX = 610, MaxY = 610 };
    private static CancellationToken Token;
    private static string Id() => Guid.NewGuid().ToString("D");
    private static TaskCompletionSource<bool> Signal() => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    public static async Task Main()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        Token = timeout.Token;
        GoldenVectorAndInvalidHeaders();
        await IndependentResponses();
        await CancelledQueryDoesNotCloseGameplay();
        await ReplacementIgnoresOldStream();
        await SlowConsumerRequiresResync();
        await GapRequiresResync();
        await GenericErrorAndEmptySuccess();
        await NoncanonicalIdIsRejectedBeforeSend();
        await MismatchedResponseClosesConnection();
        await FragmentedAndCoalescedFrames();
        await ReconnectPreservesRequestIdentity();
        await AuthenticationFailureIsExplicit();
        await IncompleteBindingIsRejected();
        await ReconnectRestoresReceiptAndFreshMap();
        await ConfirmedRejectionSurvivesLateCancellation();
        await CitiesAndMinesShareSnapshotsAndAtomicDeltas();
        await InvalidMixedPacketRequiresResync();
        await CombinedViewBudgetIncludesBothKinds();
        Console.WriteLine("PASS 18 Unity wire scenarios: golden bytes, header validation, independent replies, cancellation, replacement, slow consumer, gaps, typed errors, UUID validation, response identity, fragmented/coalesced frames, reconnect identity, binding validation, cross-connection receipt/map recovery, city/mine snapshots and atomic changes, malformed mixed packets, combined map budgets");
    }

    private static void GoldenVectorAndInvalidHeaders()
    {
        // 与 Java/Python 相同的人工固定字节，防止三端各自 roundtrip 正确但 UUID 端序不一致。
        const string id = "00112233-4455-6677-8899-aabbccddeeff";
        var frame = new WireFrame(WireFrame.Request, 0x01020304, id, 0, ByteString.CopyFrom(0x08, 0x01));
        byte[] bytes = WireCodec.Encode(frame, WireCodec.MaxClientLength);
        Check(Convert.ToHexString(bytes).ToLowerInvariant() ==
            "0000001c03010102030400112233445566778899aabbccddeeff000000000801", "Cross-language golden vector differs");
        var decoded = WireCodec.Decode(bytes[4..], WireCodec.MaxClientLength);
        Check(decoded.RequestId == id && decoded.MsgId == frame.MsgId && decoded.Body.Equals(frame.Body), "Golden vector failed decode");
        var empty = new WireFrame(WireFrame.Response, 17, id, 0, ByteString.Empty);
        Check(WireCodec.Decode(WireCodec.Encode(empty, 100)[4..], 100).Body.IsEmpty, "Empty protobuf success is valid");
        void Invalid(byte[] packet)
        {
            try { WireCodec.Decode(packet[4..], WireCodec.MaxServerLength); throw new Exception("Expected invalid frame"); }
            catch (InvalidDataException) { }
        }
        var changed = (byte[])bytes.Clone(); changed[4] = 2; Invalid(changed);
        changed = (byte[])bytes.Clone(); changed[5] = 4; Invalid(changed);
        changed = (byte[])bytes.Clone(); changed[29] = 1; Invalid(changed);
        changed = (byte[])bytes.Clone(); Array.Clear(changed, 10, 16); Invalid(changed);
        changed = (byte[])bytes.Clone(); changed[5] = WireFrame.Push; Invalid(changed);
        Invalid(bytes[..20]);
        changed = (byte[])bytes.Clone(); changed[5] = WireFrame.Response; changed[29] = 6; Invalid(changed);
        changed = (byte[])bytes.Clone(); changed[5] = WireFrame.Response; changed[29] = 1; Invalid(changed);
        foreach (var invalidLength in new[] { new byte[] { 0, 0, 0, 25 }, new byte[] { 255, 255, 255, 255 } })
        {
            try { WireCodec.ReadLength(invalidLength, WireCodec.MaxServerLength); throw new Exception("Expected bounded length rejection"); }
            catch (InvalidDataException) { }
        }
    }

    private static GatewayAuthOk Welcome(long generation = 1) => new GatewayAuthOk {
        AccountId = "101", ExpiresAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 3600,
        GameInstance = "11223344-5566-7788-99aa-bbccddeeff00", SessionGeneration = generation
    };

    private static async Task Run(Func<SlgTcpClient, Task> exercise, Func<NetworkStream, Task> server,
        GatewayAuthOk welcome = null)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var accept = Task.Run(async () => {
                using var socket = await listener.AcceptTcpClientAsync(Token);
                var stream = socket.GetStream();
                var auth = await Read(stream);
                Check(auth.Kind == WireFrame.Request && auth.MsgId == WireRoutes.AUTH,
                    "Expected registered auth request first");
                Check(GatewayAuth.Parser.ParseFrom(auth.Body).Token == "test-token", "Auth body changed");
                await Reply(stream, auth, welcome ?? Welcome());
                await server(stream);
            }, Token);
            using var client = await SlgTcpClient.ConnectLoopbackForDevelopmentAsync("127.0.0.1",
                ((IPEndPoint)listener.LocalEndpoint).Port, "test-token", Token);
            await Task.WhenAll(exercise(client), accept).WaitAsync(Token);
        }
        finally { listener.Stop(); }
    }

    private static Task IndependentResponses() => Run(async client => {
        var map = client.QueryMapAsync(Id(), Center, Token);
        var game = client.GetPlayerAsync(Id(), new GetPlayerReq(), Token);
        Check((await game).Player.AccountId == "101", "Game reply was stolen by map query");
        Check((await map).AppliedSeq == 7, "Map reply was stolen by game command");
    }, async stream => {
        var one = await Read(stream); var two = await Read(stream);
        var query = one.MsgId == WireRoutes.MAP_QUERY ? one : two;
        var play = one.MsgId == WireRoutes.GET_PLAYER ? one : two;
        Check(query != play, "Expected independent game and AOI requests");
        await WritePlayer(stream, play);
        await Reply(stream, query, new MapQueryReply { Snapshot = Snapshot(7) });
    });

    private static async Task CancelledQueryDoesNotCloseGameplay()
    {
        var accepted = Signal(); var drained = Signal();
        await Run(async client => {
            using var cancel = new CancellationTokenSource();
            var old = client.QueryMapAsync(Id(), Center, cancel.Token);
            await accepted.Task.WaitAsync(Token); cancel.Cancel();
            try { await old; throw new Exception("Expected map cancellation"); } catch (OperationCanceledException) { }
            Check((await client.GetPlayerAsync(Id(), new GetPlayerReq(), Token)).Player.AccountId == "101", "Cancellation closed gameplay");
            await drained.Task.WaitAsync(Token);
            Check((await client.QueryMapAsync(Id(), Center, Token)).AppliedSeq == 9, "Late map reply poisoned next query");
        }, async stream => {
            var old = await Read(stream); accepted.TrySetResult(true);
            await WritePlayer(stream, await Read(stream));
            await Reply(stream, old, new MapQueryReply { Snapshot = Snapshot(8) });
            string pingId = Id();
            await Write(stream, Request(pingId, new GatewayHeartbeatRequest { Nonce = 1 }));
            var pong = await Read(stream);
            Check(pong.Kind == WireFrame.Response && pong.RequestId == pingId &&
                GatewayHeartbeatReply.Parser.ParseFrom(pong.Body).Nonce == 1, "Read loop stopped after cancelled map query");
            drained.TrySetResult(true);
            await Reply(stream, await Read(stream), new MapQueryReply { Snapshot = Snapshot(9) });
        });
    }

    private static async Task ReplacementIgnoresOldStream()
    {
        var cancelled = Signal();
        await Run(async client => {
            using var first = await client.WatchMapAsync("first", Center, Token);
            Check((await first.NextAsync(Token)).Snapshot, "Missing initial map snapshot");
            Check((await first.NextAsync(Token)).RemovedIds[0] == "march", "Missing route removal");
            var second = await client.WatchMapAsync("second", Center, Token);
            Check((await second.NextAsync(Token)).ThroughSeq == 4, "Old viewport contaminated new viewport");
            second.Dispose(); await cancelled.Task.WaitAsync(Token);
        }, async stream => {
            await AcceptWatch(stream, "first");
            var snapshot = Update("first", 2, 2, true); snapshot.Upserts.Add(Route("march", 1));
            await WriteUpdate(stream, snapshot);
            var remove = Update("first", 2, 3, false); remove.RemovedIds.Add("march");
            await WriteUpdate(stream, remove);
            await AcceptWatch(stream, "second");
            await WriteUpdate(stream, Update("first", 3, 999, false));
            await WriteUpdate(stream, Update("second", 4, 4, true));
            await AcceptCancel(stream, "second"); cancelled.TrySetResult(true);
        });
    }

    private static async Task SlowConsumerRequiresResync()
    {
        var cancelled = Signal();
        await Run(async client => {
            using var view = await client.WatchMapAsync("slow", Center, Token);
            await cancelled.Task.WaitAsync(Token);
            try { await view.NextAsync(Token); throw new Exception("Expected overflow to require resync"); }
            catch (MapViewException failure) { Check(failure.Code == "MAP_RESYNC_REQUIRED", "Unexpected slow consumer error"); }
        }, async stream => {
            await AcceptWatch(stream, "slow");
            await WriteUpdate(stream, Update("slow", 0, 0, true));
            for (int i = 1; i <= 8; i++) await WriteUpdate(stream, Update("slow", i - 1, i, false));
            await AcceptCancel(stream, "slow"); cancelled.TrySetResult(true);
        });
    }

    private static async Task GapRequiresResync()
    {
        var cancelled = Signal();
        await Run(async client => {
            using var view = await client.WatchMapAsync("gap", Center, Token);
            await cancelled.Task.WaitAsync(Token);
            try { await view.NextAsync(Token); throw new Exception("Expected sequence gap failure"); }
            catch (MapViewException failure) { Check(failure.Code == "MAP_RESYNC_REQUIRED", "Unexpected gap error"); }
        }, async stream => {
            await AcceptWatch(stream, "gap");
            await WriteUpdate(stream, Update("gap", 5, 5, true));
            await WriteUpdate(stream, Update("gap", 4, 6, false));
            await AcceptCancel(stream, "gap"); cancelled.TrySetResult(true);
        });
    }

    private static Task GenericErrorAndEmptySuccess() => Run(async client => {
        string id = Id();
        try { await client.GetPlayerAsync(id, new GetPlayerReq(), Token); throw new Exception("Expected typed rejection"); }
        catch (GameplayException failure)
        { Check(failure.RequestId == id && failure.Code == "NO_PLAYER" && failure.Status != 0, "Error metadata lost"); }
        Check((await client.GetPlayerAsync(Id(), new GetPlayerReq(), Token)).Player == null, "Empty protobuf reply must decode");
    }, async stream => {
        var request = await Read(stream);
        await Write(stream, new WireFrame(WireFrame.Response, request.MsgId, request.RequestId,
            (uint)WireResultStatus.WireRejected, new BusinessError { Code = "NO_PLAYER", Message = "Create a player first" }.ToByteString()));
        await Reply(stream, await Read(stream), new GetPlayerRsp());
    });

    private static Task NoncanonicalIdIsRejectedBeforeSend() => Run(async client => {
        foreach (string invalid in new[] { "old-label", "00112233445566778899aabbccddeeff", "00112233-4455-6677-8899-AABBCCDDEEFF", WireCodec.ZeroRequestId })
        {
            try { await client.GetPlayerAsync(invalid, new GetPlayerReq(), Token); throw new Exception("Invalid UUID was accepted"); }
            catch (ArgumentException) { }
        }
        Check((await client.GetPlayerAsync(Id(), new GetPlayerReq(), Token)).Player.AccountId == "101", "Invalid local input closed socket");
    }, async stream => { await WritePlayer(stream, await Read(stream)); });

    private static Task MismatchedResponseClosesConnection() => Run(async client => {
        try { await client.GetPlayerAsync(Id(), new GetPlayerReq(), Token); throw new Exception("Mismatched response was accepted"); }
        catch (InvalidDataException) { }
    }, async stream => {
        var request = await Read(stream);
        await Write(stream, new WireFrame(WireFrame.Response, WireRoutes.GET_MARCH, request.RequestId, 0, new GetMarchRsp().ToByteString()));
    });

    private static Task FragmentedAndCoalescedFrames() => Run(async client => {
        using var view = await client.WatchMapAsync("fragmented", Center, Token);
        Check((await view.NextAsync(Token)).Snapshot, "Fragmented snapshot lost");
        Check((await view.NextAsync(Token)).ThroughSeq == 2, "Coalesced delta lost");
        Check((await client.GetPlayerAsync(Id(), new GetPlayerReq(), Token)).Player.AccountId == "101", "Heartbeats broke response parsing");
    }, async stream => {
        var watch = await Read(stream);
        byte[] ack = WireCodec.Encode(Response(watch, new MapWatchReply { SubscriptionId = "fragmented" }), WireCodec.MaxServerLength);
        for (int i = 0; i < ack.Length; i++) await stream.WriteAsync(ack.AsMemory(i, 1), Token);
        var first = WireCodec.Encode(Push(Update("fragmented", 1, 1, true)), WireCodec.MaxServerLength);
        var delta = WireCodec.Encode(Push(Update("fragmented", 1, 2, false)), WireCodec.MaxServerLength);
        string pingId = Id();
        var ping = WireCodec.Encode(Request(pingId, new GatewayHeartbeatRequest { Nonce = 123 }), WireCodec.MaxServerLength);
        byte[] combined = new byte[first.Length + delta.Length + ping.Length];
        Buffer.BlockCopy(first, 0, combined, 0, first.Length); Buffer.BlockCopy(delta, 0, combined, first.Length, delta.Length);
        Buffer.BlockCopy(ping, 0, combined, first.Length + delta.Length, ping.Length);
        await stream.WriteAsync(combined, Token);
        var one = await Read(stream); var two = await Read(stream);
        var pong = one.Kind == WireFrame.Response ? one : two;
        Check(pong.RequestId == pingId && GatewayHeartbeatReply.Parser.ParseFrom(pong.Body).Nonce == 123, "Heartbeat identity changed");
        await WritePlayer(stream, one.Kind == WireFrame.Request ? one : two);
    });

    private static async Task ReconnectPreservesRequestIdentity()
    {
        const string id = "00112233-4455-6677-8899-aabbccddeeff";
        for (int attempt = 0; attempt < 2; attempt++)
            await Run(async client => {
                    Check(client.SessionGeneration == attempt + 1 && client.GameInstance == Welcome().GameInstance,
                        "Auth binding diagnostics were lost");
                    await client.StartMarchAsync(id, new StartMarchReq { TargetX = 150, TargetY = 150 }, Token);
                },
                async stream => {
                    var request = await Read(stream);
                    Check(request.RequestId == id && request.MsgId == WireRoutes.START_MARCH, "Reconnect changed durable request ID");
                    await Reply(stream, request, new StartMarchRsp());
                }, Welcome(attempt + 1));
    }

    private static async Task IncompleteBindingIsRejected()
    {
        var missingInstance = Welcome(); missingInstance.GameInstance = "";
        var missingGeneration = Welcome(); missingGeneration.SessionGeneration = 0;
        var badInstance = Welcome(); badInstance.GameInstance = "not-a-game-instance";
        var noncanonical = Welcome(); noncanonical.GameInstance = noncanonical.GameInstance.ToUpperInvariant();
        var expired = Welcome(); expired.ExpiresAt = 1;
        foreach (var welcome in new[] { missingInstance, missingGeneration, badInstance, noncanonical, expired })
        {
            var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
            try
            {
                var server = Task.Run(async () => {
                    using var socket = await listener.AcceptTcpClientAsync(Token);
                    var stream = socket.GetStream();
                    await Reply(stream, await Read(stream), welcome);
                    // 无效成功回包必须使客户端关闭，不能继续发第一条玩法请求。
                    Check(await stream.ReadAsync(new byte[1], Token) == 0, "Client used an incomplete binding");
                }, Token);
                try
                {
                    using var client = await SlgTcpClient.ConnectLoopbackForDevelopmentAsync("127.0.0.1",
                        ((IPEndPoint)listener.LocalEndpoint).Port, "test-token", Token);
                    throw new Exception("Incomplete GAME binding was accepted");
                }
                catch (InvalidDataException) { }
                await server;
            }
            finally { listener.Stop(); }
        }
    }

    private static async Task ReconnectRestoresReceiptAndFreshMap()
    {
        const string originalId = "00112233-4455-6677-8899-aabbccddeeff";
        var newReady = Signal(); var oldSent = Signal(); var newFinished = Signal();
        await Run(async oldClient => {
            using var oldView = await oldClient.WatchMapAsync("old-connection", Center, Token);
            Check((await oldView.NextAsync(Token)).Snapshot, "Old connection never established its view");
            await Run(async newClient => {
                Check(newClient.AccountId == oldClient.AccountId && newClient.SessionGeneration > oldClient.SessionGeneration,
                    "Reconnect did not expose the new binding");
                var receipt = await newClient.GetReceiptAsync(Id(), new GetReceiptReq { TargetRequestId = originalId }, Token);
                Check(receipt.TargetRequestId == originalId && receipt.Receipt.Started.March.MarchId == "original-march",
                    "Receipt recovery changed the original operation");
                using var newView = await newClient.WatchMapAsync("new-connection", Center, Token);
                var snapshot = await newView.NextAsync(Token);
                Check(snapshot.Snapshot && snapshot.ThroughSeq == 20, "Reconnect used old data instead of a fresh snapshot");
                newReady.TrySetResult(true);
                Check((await newView.NextAsync(Token)).ThroughSeq == 21, "Old connection leaked into the new map queue");
                newFinished.TrySetResult(true);
            }, async stream => {
                var query = await Read(stream);
                Check(query.MsgId == WireRoutes.GET_RECEIPT
                    && GetReceiptReq.Parser.ParseFrom(query.Body).TargetRequestId == originalId,
                    "Reconnect replayed a new write instead of querying the saved UUID");
                await Reply(stream, query, new GetReceiptRsp { TargetRequestId = originalId, JournalSeq = 7,
                    Receipt = new WriteReceipt { MsgId = WireRoutes.START_MARCH,
                        Started = new StartMarchRsp { March = new MarchView { MarchId = "original-march" } } } });
                await AcceptWatch(stream, "new-connection");
                await WriteUpdate(stream, Update("new-connection", 20, 20, true));
                await oldSent.Task.WaitAsync(Token);
                await WriteUpdate(stream, Update("new-connection", 20, 21, false));
                await newFinished.Task.WaitAsync(Token);
            }, Welcome(2));
            // 旧网络缓冲可能先到一条旧增量，随后必须显式结束；这两个包都只属于 oldView。
            try
            {
                await oldView.NextAsync(Token);
                await oldView.NextAsync(Token);
                throw new Exception("Replaced connection did not terminate its own map subscription");
            }
            catch (GatewayConnectionException failure) { Check(failure.Code == "SESSION_REPLACED", "Old session error was lost"); }
        }, async stream => {
            await AcceptWatch(stream, "old-connection");
            await WriteUpdate(stream, Update("old-connection", 10, 10, true));
            await newReady.Task.WaitAsync(Token);
            await WriteUpdate(stream, Update("old-connection", 10, 999, false));
            await Write(stream, Push(new GatewayConnectionError {
                Error = new BusinessError { Code = "SESSION_REPLACED", Message = "Another connection became active" } }));
            oldSent.TrySetResult(true);
            await newFinished.Task.WaitAsync(Token);
        }, Welcome(1));
    }

    private static async Task AuthenticationFailureIsExplicit()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        try
        {
            var server = Task.Run(async () => {
                using var socket = await listener.AcceptTcpClientAsync(Token);
                var stream = socket.GetStream(); var auth = await Read(stream);
                await Write(stream, new WireFrame(WireFrame.Response, auth.MsgId, auth.RequestId,
                    (uint)WireResultStatus.WireAuthRequired,
                    new BusinessError { Code = "UNAUTHORIZED", Message = "Expired session" }.ToByteString()));
            }, Token);
            try
            {
                using var client = await SlgTcpClient.ConnectLoopbackForDevelopmentAsync("127.0.0.1",
                    ((IPEndPoint)listener.LocalEndpoint).Port, "test-token", Token);
                throw new Exception("Invalid token unexpectedly authenticated");
            }
            catch (GatewayConnectionException failure) { Check(failure.Code == "UNAUTHORIZED", "Auth error code lost"); }
            await server;
        }
        finally { listener.Stop(); }
    }

    private static async Task ConfirmedRejectionSurvivesLateCancellation()
    {
        var finished = Signal();
        await Run(async client => {
            // 包已经写上 TCP，但本地 WriteAsync 的 continuation 可以晚于回包读取。以 Stream 包装器
            // 控制这个真实的异步边界，不给生产代码加入测试钩子，也不靠睡眠碰运气。
            var field = typeof(SlgTcpClient).GetField("stream", BindingFlags.Instance | BindingFlags.NonPublic);
            var delayed = new DelayedWriteCompletionStream((Stream)field.GetValue(client));
            field.SetValue(client, delayed);
            using var cancel = new CancellationTokenSource();
            var response = client.GetPlayerAsync(Id(), new GetPlayerReq(), cancel.Token);
            try
            {
                // 接收循环开始读下一帧长度，说明上一回包已完成解码、身份匹配并进入请求 Completion。
                await delayed.ResponseConsumed.Task.WaitAsync(Token);
                cancel.Cancel();
                delayed.ReleaseWrite.TrySetResult(true);
                try { await response; throw new Exception("Expected confirmed rejection"); }
                catch (GameplayException failure)
                { Check(failure.Code == "NO_PLAYER", "Late cancellation changed a confirmed rejection"); }
            }
            finally { delayed.ReleaseWrite.TrySetResult(true); finished.TrySetResult(true); }
        }, async stream => {
            var request = await Read(stream);
            await Write(stream, new WireFrame(WireFrame.Response, request.MsgId, request.RequestId,
                (uint)WireResultStatus.WireRejected, new BusinessError { Code = "NO_PLAYER", Message = "Not created" }.ToByteString()));
            await finished.Task.WaitAsync(Token);
        });
    }

    private static async Task CitiesAndMinesShareSnapshotsAndAtomicDeltas()
    {
        var firstRead = Signal(); var cancelled = Signal(); var finished = Signal();
        await Run(async client => {
            var queried = await client.QueryMapAsync(Id(), Center, Token);
            Check(queried.Objects.Count == 2 && queried.Objects[1].Version == 0, "Query lost the untouched static mine");
            var first = await client.WatchMapAsync("world-before", Center, Token);
            var snapshot = await first.NextAsync(Token);
            Check(snapshot.Snapshot && snapshot.Upserts.Count == 1 && snapshot.ObjectUpserts.Count == 2,
                "First packet did not deliver routes and point objects together");
            firstRead.TrySetResult(true);
            var occupied = await first.NextAsync(Token);
            Check(occupied.RemovedIds.Count == 1 && occupied.ObjectUpserts[0].ResourceSite.OccupantPlayerId == "17_101"
                && occupied.ThroughSeq == 2, "Mine claim and route removal were not one visible decision");
            var released = await first.NextAsync(Token);
            Check(released.Upserts[0].State == MarchState.Returning && released.ObjectUpserts[0].Version == 2
                && released.ObjectUpserts[0].ResourceSite.OccupantPlayerId == "", "Mine release and return route were split");
            first.Dispose(); await cancelled.Task.WaitAsync(Token);
            using var restored = await client.WatchMapAsync("world-after", Center, Token);
            var fresh = await restored.NextAsync(Token);
            Check(fresh.Snapshot && fresh.ThroughSeq == 4 && fresh.Upserts.Count == 0
                && fresh.ObjectUpserts.Count == 1 && fresh.ObjectUpserts[0].ObjectId == "resource:650-650",
                "New viewport did not replace the city and route contents with a fresh object snapshot");
            finished.TrySetResult(true);
        }, async stream => {
            var query = Snapshot(1); query.Objects.Add(City()); query.Objects.Add(Mine(0));
            await Reply(stream, await Read(stream), new MapQueryReply { Snapshot = query });
            await AcceptWatch(stream, "world-before");
            var initial = Update("world-before", 1, 1, true);
            initial.Upserts.Add(Route("travel", 1)); initial.ObjectUpserts.Add(City()); initial.ObjectUpserts.Add(Mine(0));
            await WriteUpdate(stream, initial);
            await firstRead.Task.WaitAsync(Token);
            var claim = Update("world-before", 1, 2, false);
            claim.RemovedIds.Add("travel"); claim.ObjectUpserts.Add(Mine(1, "17_101"));
            await WriteUpdate(stream, claim);
            var release = Update("world-before", 2, 3, false);
            release.Upserts.Add(Route("travel", 3, MarchState.Returning)); release.ObjectUpserts.Add(Mine(2));
            await WriteUpdate(stream, release);
            await AcceptCancel(stream, "world-before"); cancelled.TrySetResult(true);
            await AcceptWatch(stream, "world-after");
            // 已取消订阅的迟到点对象仍只能属于旧 ID，不能混进新视图。
            var late = Update("world-before", 3, 999, false); late.ObjectUpserts.Add(City(99));
            await WriteUpdate(stream, late);
            var fresh = Update("world-after", 4, 4, true); fresh.ObjectUpserts.Add(Mine(2));
            await WriteUpdate(stream, fresh);
            await finished.Task.WaitAsync(Token);
        });
    }

    private static async Task InvalidMixedPacketRequiresResync()
    {
        foreach (int invalid in new[] { 0, 1, 2, 3, 4, 5 })
        {
            var initialRead = Signal(); var cancelled = Signal();
            await Run(async client => {
                using var view = await client.WatchMapAsync("invalid-world", Center, Token);
                Check((await view.NextAsync(Token)).Snapshot, "Missing valid initial mixed snapshot");
                initialRead.TrySetResult(true);
                await cancelled.Task.WaitAsync(Token);
                try { await view.NextAsync(Token); throw new Exception("Invalid object leaked a partial route removal"); }
                catch (MapViewException failure) { Check(failure.Code == "MAP_RESYNC_REQUIRED", "Invalid object did not require resync"); }
            }, async stream => {
                await AcceptWatch(stream, "invalid-world");
                var initial = Update("invalid-world", 1, 1, true);
                initial.Upserts.Add(Route("travel", 1)); initial.ObjectUpserts.Add(City()); initial.ObjectUpserts.Add(Mine(0));
                await WriteUpdate(stream, initial); await initialRead.Task.WaitAsync(Token);
                var bad = Update("invalid-world", 1, 2, false); bad.RemovedIds.Add("travel");
                switch (invalid)
                {
                    case 0: bad.ObjectUpserts.Add(Mine(1)); bad.ObjectUpserts.Add(Mine(1)); break;
                    case 1: bad.ObjectUpserts.Add(Mine(0, "17_101")); break; // v0 矿只能是从未占用的空矿。
                    case 2: var changedType = Mine(1); changedType.ObjectId = "city:17_101"; bad.ObjectUpserts.Add(changedType); break;
                    case 3: bad.ObjectUpserts.Add(new MapObjectView { ObjectId = "resource:650-650", Version = 1, X = 650, Y = 650 }); break;
                    case 4: bad.ObjectUpserts.Add(City(1)); break; // 城市同版后像不能成为新增量。
                    case 5: bad.ObjectUpserts.Add(Mine(1)); bad.RemovedObjectIds.Add("resource:650-650"); break;
                }
                await WriteUpdate(stream, bad);
                await AcceptCancel(stream, "invalid-world"); cancelled.TrySetResult(true);
            });
        }
    }

    private static async Task CombinedViewBudgetIncludesBothKinds()
    {
        var initialRead = Signal(); var cancelled = Signal();
        await Run(async client => {
            using var view = await client.WatchMapAsync("combined-budget", Center, Token);
            var first = await view.NextAsync(Token);
            Check(first.Upserts.Count + first.ObjectUpserts.Count == 1024, "Combined capacity fixture is incomplete");
            initialRead.TrySetResult(true); await cancelled.Task.WaitAsync(Token);
            try { await view.NextAsync(Token); throw new Exception("Per-type budgets allowed oversized combined state"); }
            catch (MapViewException failure) { Check(failure.Code == "MAP_RESYNC_REQUIRED", "Combined budget did not require resync"); }
        }, async stream => {
            await AcceptWatch(stream, "combined-budget");
            var initial = Update("combined-budget", 1, 1, true);
            for (int i = 0; i < 512; i++)
            {
                initial.Upserts.Add(Route("route-" + i, 1));
                var point = Mine(0); point.X = i; point.ObjectId = "resource:" + i + "-650";
                initial.ObjectUpserts.Add(point);
            }
            await WriteUpdate(stream, initial); await initialRead.Task.WaitAsync(Token);
            var tooMany = Update("combined-budget", 1, 2, false);
            var extra = Mine(1); extra.X = 512; extra.ObjectId = "resource:512-650";
            tooMany.ObjectUpserts.Add(extra); await WriteUpdate(stream, tooMany);
            await AcceptCancel(stream, "combined-budget"); cancelled.TrySetResult(true);
        });
    }

    private static MapViewSnapshot Snapshot(long sequence) => new MapViewSnapshot {
        RealmId = 17, DatasetId = "test-dataset", Generation = "test-generation", AppliedSeq = sequence
    };
    private static MapRouteView Route(string id, long version, MarchState state = MarchState.Outbound) => new MapRouteView {
        MarchId = id, Version = version, State = state, FromX = 0, FromY = 0,
        TargetX = 1199, TargetY = 1199, DepartAt = 1, ArriveAt = 2
    };
    private static MapObjectView City(long version = 1) => new MapObjectView {
        ObjectId = "city:17_101", Version = version, X = 0, Y = 0, City = new MapCityView { PlayerId = "17_101", Name = "City" }
    };
    private static MapObjectView Mine(long version, string occupant = "") => new MapObjectView {
        ObjectId = "resource:650-650", Version = version, X = 650, Y = 650,
        ResourceSite = new MapResourceSiteView { ResourceType = "WOOD", OccupantPlayerId = occupant }
    };

    private sealed class DelayedWriteCompletionStream : Stream
    {
        private readonly Stream inner;
        private bool bodyRead;
        internal readonly TaskCompletionSource<bool> ResponseConsumed = Signal(), ReleaseWrite = Signal();
        internal DelayedWriteCompletionStream(Stream inner) { this.inner = inner; }
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
        {
            await inner.WriteAsync(buffer, offset, count, cancellation);
            await ReleaseWrite.Task;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
        {
            if (buffer.Length > 4) bodyRead = true;
            else if (bodyRead && buffer.Length == 4) ResponseConsumed.TrySetResult(true);
            return inner.ReadAsync(buffer, offset, count, cancellation);
        }
        public override bool CanRead => inner.CanRead;
        public override bool CanWrite => inner.CanWrite;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        protected override void Dispose(bool disposing) { if (disposing) inner.Dispose(); base.Dispose(disposing); }
    }

    private static async Task AcceptWatch(NetworkStream stream, string id)
    {
        var frame = await Read(stream);
        Check(frame.MsgId == WireRoutes.MAP_WATCH && MapWatchRequest.Parser.ParseFrom(frame.Body).SubscriptionId == id, "Unexpected watch");
        await Reply(stream, frame, new MapWatchReply { SubscriptionId = id });
    }
    private static async Task AcceptCancel(NetworkStream stream, string id)
    {
        var frame = await Read(stream);
        Check(frame.MsgId == WireRoutes.MAP_CANCEL && MapWatchCancel.Parser.ParseFrom(frame.Body).SubscriptionId == id, "Unexpected cancel");
        await Reply(stream, frame, new MapCancelReply { SubscriptionId = id });
    }
    private static MapViewUpdate Update(string id, long from, long through, bool snapshot) => new MapViewUpdate {
        SubscriptionId = id, RealmId = 17, DatasetId = "test-dataset", Generation = "test-generation",
        FromSeq = from, ThroughSeq = through, Snapshot = snapshot
    };
    private static WireFrame Request(string id, IMessage message) =>
        new WireFrame(WireFrame.Request, WireRoutes.RequestIdFor(message), id, 0, message.ToByteString());
    private static WireFrame Response(WireFrame request, IMessage response)
    {
        Check(WireRoutes.ResponseIdFor(response) == request.MsgId, "Server response type mismatch");
        return new WireFrame(WireFrame.Response, request.MsgId, request.RequestId, 0, response.ToByteString());
    }
    private static WireFrame Push(IMessage message) =>
        new WireFrame(WireFrame.Push, WireRoutes.PushIdFor(message), WireCodec.ZeroRequestId, 0, message.ToByteString());
    private static Task Reply(NetworkStream stream, WireFrame request, IMessage response) => Write(stream, Response(request, response));
    private static Task WriteUpdate(NetworkStream stream, MapViewUpdate update) => Write(stream, Push(update));
    private static Task WritePlayer(NetworkStream stream, WireFrame request) =>
        Reply(stream, request, new GetPlayerRsp { Player = new PlayerView { AccountId = "101" } });
    private static async Task Write(NetworkStream stream, WireFrame frame) =>
        await stream.WriteAsync(WireCodec.Encode(frame, WireCodec.MaxServerLength), Token);
    private static async Task<WireFrame> Read(NetworkStream stream)
    {
        byte[] header = new byte[4]; await stream.ReadExactlyAsync(header, Token);
        int length = WireCodec.ReadLength(header, WireCodec.MaxClientLength);
        byte[] remainder = new byte[length]; await stream.ReadExactlyAsync(remainder, Token);
        return WireCodec.Decode(remainder, WireCodec.MaxClientLength);
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;
using Slg.Rpc.V1;
using Slg.Unity;

// 在独立临时区服用七个生成方法完成采集、召回和 NPC 战斗。令牌从 stdin 获取，不写入命令行。
internal static class Program
{
    private static async Task Main(string[] args)
    {
        int port = int.Parse(args[0]);
        string token = Console.ReadLine();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        CancellationToken ct = deadline.Token;
        Task<SlgTcpClient> Connect() =>
            SlgTcpClient.ConnectLoopbackForDevelopmentAsync("127.0.0.1", port, token, ct);

        string requestId = Guid.NewGuid().ToString("N");
        var gather = Order(150, 120, MarchMission.Gather);
        StartMarchRsp original;
        using (var first = await Connect())
        {
            var created = await first.CreatePlayerAsync("unity-create", new CreatePlayerReq { Name = "Unity" }, ct);
            var before = await ReadPlayer(first, ct);
            Require(before.Troops["INFANTRY"] == 3000 && before.Equals(created.Player),
                "The starting player does not match the creation receipt.");
            original = await first.StartMarchAsync(requestId, gather, ct);
            var march = original.March;
            Require(march.MarchId.Length > 0 && march.State == MarchState.Outbound &&
                march.Mission == MarchMission.Gather && march.TargetX == 150 && march.TargetY == 150 &&
                march.ArriveAt > march.DepartAt && march.Troops.Count == 1 && march.Troops[0].Equals(gather.Troops[0]) &&
                march.HeroIds.Count == 1 && march.HeroIds[0] == "sergey", "Unexpected departure receipt.");
        }

        using var client = await Connect();
        Require(original.Equals(await client.StartMarchAsync(requestId, gather, ct)), "Retry changed the departure receipt.");
        var lookup = await client.GetReceiptAsync("unity-lookup", new GetReceiptReq { TargetRequestId = requestId }, ct);
        Require(lookup.JournalSeq > 0 && lookup.TargetRequestId == requestId &&
            original.Equals(lookup.Receipt?.Started), "Recorded intent is missing its original departure.");
        var changed = gather.Clone();
        changed.Troops[0].Count++;
        try
        {
            await client.StartMarchAsync(requestId, changed, ct);
            throw new InvalidOperationException("Expected REQUEST_CONFLICT.");
        }
        catch (GameplayException error) when (error.Code == "REQUEST_CONFLICT" && error.RequestId == requestId) { }
        var reserved = await ReadPlayer(client, ct);
        Require(reserved.ActiveMarchIds.Count == 1 && reserved.ActiveMarchIds[0] == original.March.MarchId &&
            reserved.Troops["INFANTRY"] == 2880, "Reconnect or retry reserved the army twice.");
        await WaitForReturn(client, original.March.MarchId, ct);
        var gathered = await ReadPlayer(client, ct);
        Require(gathered.Troops["INFANTRY"] == 3000 && gathered.Resources["WOOD"] == 600 &&
            gathered.ActiveMarchIds.Count == 0, "Gathering must return 120 infantry and credit 600 wood once.");

        string recalledId = await CheckRecall(client, gathered, ct);
        string battleId = await CheckBattle(client, gathered.PlayerId, ct);
        // 只输出三个公开的行军 ID；Python 独立查询已完成状态及最终战损。
        Console.WriteLine(original.March.MarchId);
        Console.WriteLine(recalledId);
        Console.WriteLine(battleId);
    }

    private static async Task<string> CheckRecall(SlgTcpClient client, PlayerView gathered, CancellationToken ct)
    {
        var sent = await client.StartMarchAsync("unity-distant", Order(1150, 120, MarchMission.Gather), ct);
        var request = new RecallMarchReq { MarchId = sent.March.MarchId };
        var first = await client.RecallMarchAsync("unity-recall", request, ct);
        Require(first.March.MarchId == request.MarchId && first.March.State == MarchState.Returning,
            "Recalling an outbound march should create its return leg.");
        Require(first.Equals(await client.RecallMarchAsync("unity-recall", request, ct)), "Recall was applied twice.");
        var lookup = await client.GetReceiptAsync("unity-recall-lookup",
            new GetReceiptReq { TargetRequestId = "unity-recall" }, ct);
        Require(first.Equals(lookup.Receipt?.Recalled), "Recall lookup did not preserve the original result.");
        await WaitForReturn(client, request.MarchId, ct);
        var after = await ReadPlayer(client, ct);
        Require(after.Troops.Equals(gathered.Troops) && after.Resources.Equals(gathered.Resources) &&
            after.ActiveMarchIds.Count == 0 && !after.Heroes[0].HasAssignedMarch,
            "The recalled march must return troops and release the hero without gathering more cargo.");
        return request.MarchId;
    }

    private static async Task<string> CheckBattle(SlgTcpClient client, string playerId, CancellationToken ct)
    {
        var attack = Order(120, 650, MarchMission.AttackNpc);
        var sent = await client.StartMarchAsync("unity-npc", attack, ct);
        string id = sent.March.MarchId;
        await WaitForReturn(client, id, ct);
        var report = await client.GetBattleAsync("unity-report", new GetBattleReq { BattleId = id }, ct);
        Require(report.State == BattleReportState.ReportSettled && report.BattleId == id && report.MarchId == id &&
            report.Result?.BattleId == id && report.Result.InputHash == report.InputHash &&
            report.Result.Outcome == BattleOutcome.BattleAttackerWin && report.Result.Attacker.OwnerId == playerId &&
            report.Result.Attacker.Groups.Count == 1, "The completed NPC march must have a matching winning report.");
        var losses = report.Result.Attacker.Groups[0];
        Require(losses.InitialCount == 650 && losses.Survivors + losses.Wounded + losses.Dead == 650,
            "The battle report must account for every deployed infantryman.");
        var after = await ReadPlayer(client, ct);
        Require(after.Troops["INFANTRY"] == 2350 + losses.Survivors && after.Resources["WOOD"] == 1100 &&
            after.Resources["FOOD"] == 500 && after.Resources["GEMS"] == 10 && after.ActiveMarchIds.Count == 0 &&
            !after.Heroes[0].HasAssignedMarch, "NPC return or victory cargo does not match the battle result.");
        Require(sent.Equals(await client.StartMarchAsync("unity-npc", attack, ct)) &&
            after.Equals(await ReadPlayer(client, ct)), "Replaying a settled attack changed player state.");
        return id;
    }

    private static StartMarchReq Order(int target, long infantry, MarchMission mission) => new StartMarchReq {
        TargetX = target, TargetY = target, Mission = mission,
        Troops = { new TroopGroup { Type = TroopType.Infantry, Count = infantry } }
    };

    private static async Task<PlayerView> ReadPlayer(SlgTcpClient client, CancellationToken ct) =>
        (await client.GetPlayerAsync("unity-read", new GetPlayerReq(), ct)).Player;

    private static async Task WaitForReturn(SlgTcpClient client, string id, CancellationToken ct)
    {
        var request = new GetMarchReq { MarchId = id };
        while (true)
        {
            var reply = await client.GetMarchAsync("unity-wait", request, ct);
            Require(reply.March.MarchId == id, "The march query returned a different march.");
            if (reply.March.State == MarchState.Completed) return;
            await Task.Delay(350, ct);
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}

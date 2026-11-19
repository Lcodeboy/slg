using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Slg.Rpc.V1;
using Google.Protobuf;

namespace Slg.Unity
{
    public sealed class MapViewException : Exception
    {
        public string Code { get; }
        internal MapViewException(string code, string message) : base(message) { Code = code; }
    }

    /// <summary>
    /// 只保存当前视野的 ID、类型及版本，不持有 Unity 的渲染对象。路线与城市/矿点共用 1024 个
    /// 可见条目的客户端预算；不能给每类各分配 1024 个后，把组合视野变成无界状态。
    /// 每包先在私有副本中完整验证，再一次切换。某个矿点坏数据不会让同包的行军移除提前生效。
    /// </summary>
    internal sealed class MapPacketState
    {
        internal const int MaxVisibleEntries = 1024;
        private Dictionary<string, long> routes = new Dictionary<string, long>();
        private Dictionary<string, ObjectStamp> objects = new Dictionary<string, ObjectStamp>();
        private struct ObjectStamp
        {
            internal long Version;
            internal MapObjectView.DetailOneofCase Kind;
        }

        internal void Clear() { routes.Clear(); objects.Clear(); }

        internal void Apply(MapViewUpdate value)
        {
            if (value.Snapshot && (value.RemovedIds.Count != 0 || value.RemovedObjectIds.Count != 0))
                throw new InvalidDataException("A complete map snapshot cannot contain removals.");
            if (!value.Snapshot && value.ThroughSeq == value.FromSeq
                && value.Upserts.Count + value.RemovedIds.Count + value.ObjectUpserts.Count + value.RemovedObjectIds.Count != 0)
                throw new InvalidDataException("Map changes require an advancing journal sequence.");
            if ((long)value.Upserts.Count + value.ObjectUpserts.Count > MaxVisibleEntries
                || (long)value.RemovedIds.Count + value.RemovedObjectIds.Count > MaxVisibleEntries)
                throw new InvalidDataException("Map packet exceeds the combined visible entry budget.");
            var nextRoutes = value.Snapshot ? new Dictionary<string, long>() : new Dictionary<string, long>(routes);
            var nextObjects = value.Snapshot ? new Dictionary<string, ObjectStamp>() : new Dictionary<string, ObjectStamp>(objects);
            var touched = new HashSet<string>();
            foreach (var route in value.Upserts)
            {
                ValidateRoute(route, value.ThroughSeq);
                if (!touched.Add(route.MarchId) || nextRoutes.TryGetValue(route.MarchId, out long version) && route.Version <= version)
                    throw new InvalidDataException("Duplicate route or non-increasing map version.");
                nextRoutes[route.MarchId] = route.Version;
            }
            foreach (string id in value.RemovedIds)
                if (!touched.Add(id) || !nextRoutes.Remove(id))
                    throw new InvalidDataException("Duplicate, contradictory or unknown route removal.");
            foreach (var item in value.ObjectUpserts)
            {
                ValidateObject(item, value.ThroughSeq);
                if (!touched.Add(item.ObjectId) || nextObjects.TryGetValue(item.ObjectId, out var old)
                    && (item.Version <= old.Version || item.DetailCase != old.Kind))
                    throw new InvalidDataException("Duplicate object, changed type or non-increasing map version.");
                nextObjects[item.ObjectId] = new ObjectStamp { Version = item.Version, Kind = item.DetailCase };
            }
            foreach (string id in value.RemovedObjectIds)
                if (!touched.Add(id) || !nextObjects.Remove(id))
                    throw new InvalidDataException("Duplicate, contradictory or unknown object removal.");
            if ((long)nextRoutes.Count + nextObjects.Count > MaxVisibleEntries)
                throw new InvalidDataException("Map view exceeds the combined visible entry budget.");
            routes = nextRoutes;
            objects = nextObjects;
        }

        internal static void ValidateSnapshot(MapViewSnapshot value)
        {
            if (value.RealmId <= 0 || string.IsNullOrEmpty(value.DatasetId) || string.IsNullOrEmpty(value.Generation)
                || value.AppliedSeq < 0) throw new InvalidDataException("Incomplete map query snapshot.");
            var packet = new MapViewUpdate { Snapshot = true, ThroughSeq = value.AppliedSeq };
            packet.Upserts.Add(value.Routes);
            packet.ObjectUpserts.Add(value.Objects);
            new MapPacketState().Apply(packet);
        }

        private static void ValidateRoute(MapRouteView value, long sequence)
        {
            if (!Regex.IsMatch(value.MarchId, @"\A[A-Za-z0-9_-]{1,64}\z") || value.Version < 1 || value.Version > sequence
                || !(value.FromX >= 0 && value.FromX <= 1199 && value.FromY >= 0 && value.FromY <= 1199)
                || value.TargetX < 0 || value.TargetX > 1199 || value.TargetY < 0 || value.TargetY > 1199
                || value.DepartAt < 0 || value.ArriveAt <= value.DepartAt
                || value.State != MarchState.Outbound && value.State != MarchState.Returning)
                throw new InvalidDataException("Invalid map route payload.");
        }

        private static bool PlayerId(string value)
        {
            string[] pieces = value.Split('_');
            return pieces.Length == 2 && int.TryParse(pieces[0], out int realm) && realm > 0
                && long.TryParse(pieces[1], out long account) && account > 0
                && value == realm.ToString(System.Globalization.CultureInfo.InvariantCulture) + "_"
                    + account.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        private static void ValidateObject(MapObjectView value, long sequence)
        {
            if (value.Version < 0 || value.Version > sequence || value.X < 0 || value.X > 1199 || value.Y < 0 || value.Y > 1199)
                throw new InvalidDataException("Invalid map object version or coordinate.");
            switch (value.DetailCase)
            {
                case MapObjectView.DetailOneofCase.City:
                    if (value.Version < 1 || !PlayerId(value.City.PlayerId) || value.ObjectId != "city:" + value.City.PlayerId
                        || string.IsNullOrWhiteSpace(value.City.Name)) throw new InvalidDataException("Invalid city payload.");
                    break;
                case MapObjectView.DetailOneofCase.ResourceSite:
                    string expected = "resource:" + value.X.ToString(System.Globalization.CultureInfo.InvariantCulture)
                        + "-" + value.Y.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    if (value.ObjectId != expected || value.ResourceSite.ResourceType != "WOOD"
                        || value.ResourceSite.OccupantPlayerId.Length != 0
                        && (value.Version == 0 || !PlayerId(value.ResourceSite.OccupantPlayerId)))
                        throw new InvalidDataException("Invalid resource-site payload.");
                    break;
                default: throw new InvalidDataException("Unknown map object type; resynchronize with the current protocol.");
            }
        }
    }

    /// <summary>
    /// 一次视野订阅。NextAsync 可由 Unity 主循环消费；接收线程不会执行用户回调。
    /// 最多保存八个包和 4 MiB，消费过慢会明确要求重同步，不静默丢掉路线删除事件。
    /// 订阅始终属于创建它的 SlgTcpClient。跨网关重连必须创建新订阅并等待完整快照，
    /// 不能把旧连接的队列、datasetId、generation 或 throughSeq 作为新连接已经同步的证据。
    /// 同一订阅必须始终属于首包的区服与数据集；身份变化须重建完整快照，不能接续旧水位。
    /// </summary>
    public sealed class MapSubscription : IDisposable
    {
        private readonly SlgTcpClient owner;
        private readonly object state = new object();
        private readonly Queue<MapViewUpdate> updates = new Queue<MapViewUpdate>();
        private readonly MapPacketState contents = new MapPacketState();
        private TaskCompletionSource<MapViewUpdate> waiter;
        private Exception failure;
        private string generation, dataset;
        private int realm, bufferedBytes;
        private long sequence;
        private int disposed;
        public string SubscriptionId { get; }
        internal MapSubscription(SlgTcpClient owner, string id) { this.owner = owner; SubscriptionId = id; }

        public async Task<MapViewUpdate> NextAsync(CancellationToken cancellation)
        {
            cancellation.ThrowIfCancellationRequested();
            TaskCompletionSource<MapViewUpdate> next;
            lock (state)
            {
                if (failure != null) throw failure;
                if (updates.Count > 0)
                {
                    var value = updates.Dequeue();
                    bufferedBytes -= value.CalculateSize();
                    return value;
                }
                if (waiter != null) throw new InvalidOperationException("Only one NextAsync may wait per subscription.");
                next = waiter = new TaskCompletionSource<MapViewUpdate>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            using (cancellation.Register(() => {
                lock (state) { if (waiter == next) { waiter = null; next.TrySetCanceled(); } }
            })) return await next.Task.ConfigureAwait(false);
        }

        internal bool Publish(MapViewUpdate value)
        {
            lock (state)
            {
                if (failure != null) return false;
                if (value.Error != null) { FailLocked(new MapViewException(value.Error.Code, value.Error.Message)); return false; }
                if (generation == null)
                {
                    if (!value.Snapshot || string.IsNullOrEmpty(value.Generation) || string.IsNullOrEmpty(value.DatasetId)
                        || value.RealmId <= 0 || value.ThroughSeq < 0)
                    { FailLocked(new MapViewException("MAP_RESYNC_REQUIRED", "Expected a complete initial map snapshot.")); return false; }
                }
                else if (value.Snapshot || value.Generation != generation || value.DatasetId != dataset || value.RealmId != realm
                    || value.FromSeq != sequence || value.ThroughSeq < value.FromSeq)
                { FailLocked(new MapViewException("MAP_RESYNC_REQUIRED", "Map dataset, generation or sequence changed.")); return false; }
                // 公开路线、城市及矿点共享一条连续视图；完整内容验证通过之前不交给等待者或队列。
                try { contents.Apply(value); }
                catch (InvalidDataException invalid)
                { FailLocked(new MapViewException("MAP_RESYNC_REQUIRED", invalid.Message)); return false; }
                generation = value.Generation; dataset = value.DatasetId; realm = value.RealmId;
                sequence = value.ThroughSeq;
                if (waiter != null)
                {
                    var waiting = waiter; waiter = null;
                    waiting.TrySetResult(value);
                    return true;
                }
                int size = value.CalculateSize();
                if (updates.Count >= 8 || bufferedBytes + size > 4 * 1024 * 1024)
                { FailLocked(new MapViewException("MAP_RESYNC_REQUIRED", "Map consumer is too slow; open a new subscription.")); return false; }
                updates.Enqueue(value); bufferedBytes += size;
                return true;
            }
        }

        internal void Fail(Exception reason) { lock (state) FailLocked(reason); }
        private void FailLocked(Exception reason)
        {
            if (failure != null) return;
            failure = reason; updates.Clear(); contents.Clear(); bufferedBytes = 0;
            waiter?.TrySetException(reason); waiter = null;
        }
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0) return;
            Fail(new ObjectDisposedException(nameof(MapSubscription)));
            owner.CancelMap(this);
        }
    }

    public sealed partial class SlgTcpClient
    {
        private MapSubscription mapSubscription;
        private readonly SemaphoreSlim mapControl = new SemaphoreSlim(1, 1);

        /// <summary>
        /// 地图查询通过生成的普通请求/响应协议发送，但拥有独立于 GAME 的在途名额。
        /// 取消只结束等待，迟到回包仍由唯一读取循环回收，不会误交下一次查询或关闭玩法连接。
        /// </summary>
        public async Task<MapViewSnapshot> QueryMapAsync(string requestId, MapViewport viewport, CancellationToken cancellation)
        {
            if (viewport == null) throw new ArgumentNullException(nameof(viewport));
            try
            {
                var reply = await MapQueryAsync(requestId, new MapQueryRequest { Viewport = viewport }, cancellation)
                    .ConfigureAwait(false);
                if (reply.Snapshot == null)
                {
                    throw new InvalidDataException("Successful map query is missing its snapshot.");
                }
                MapPacketState.ValidateSnapshot(reply.Snapshot);
                return reply.Snapshot;
            }
            catch (GameplayException failure) { throw new MapViewException(failure.Code, failure.Message); }
            catch (InvalidDataException failure) { Close(failure); throw; }
        }

        /// <summary>
        /// 每次拖动地图使用新的 subscriptionId。先登记本地订阅，再发送生成的控制请求，避免 ACK 后紧跟的快照
        /// 在调用方恢复执行之前到达而丢失。ACK 只表示网关接收订阅，真正视野准备完成以第一条 snapshot 推送为准。
        /// 跨网关恢复也调用本方法并使用新 ID；先取 NextAsync 得到完整首包，再替换 Unity 当前显示的旧视图。
        /// </summary>
        public async Task<MapSubscription> WatchMapAsync(string subscriptionId, MapViewport viewport, CancellationToken cancellation)
        {
            ValidateMapId(subscriptionId);
            if (viewport == null) throw new ArgumentNullException(nameof(viewport));
            await mapControl.WaitAsync(cancellation).ConfigureAwait(false);
            MapSubscription next = null;
            try
            {
                cancellation.ThrowIfCancellationRequested();
                next = new MapSubscription(this, subscriptionId);
                lock (state)
                {
                    if (closeReason != null) throw closeReason;
                    if (mapSubscription?.SubscriptionId == subscriptionId)
                        throw new ArgumentException("A replacement viewport requires a new subscription ID.", nameof(subscriptionId));
                    mapSubscription?.Fail(new MapViewException("MAP_REPLACED", "A new viewport replaced this subscription."));
                    mapSubscription = next;
                }
                var reply = await MapWatchAsync(Guid.NewGuid().ToString("D"),
                    new MapWatchRequest { SubscriptionId = subscriptionId, Viewport = viewport }, cancellation).ConfigureAwait(false);
                if (reply.SubscriptionId != subscriptionId)
                {
                    var failure = new InvalidDataException("Map watch acknowledgment has a different subscription ID.");
                    Close(failure); throw failure;
                }
                return next;
            }
            catch (Exception failure)
            {
                Exception reason = failure is GameplayException rejected
                    ? new MapViewException(rejected.Code, rejected.Message) : failure;
                next?.Fail(reason);
                lock (state) if (mapSubscription == next) mapSubscription = null;
                throw reason;
            }
            finally { mapControl.Release(); }
        }

        internal void CancelMap(MapSubscription subscription) { _ = CancelMapAsync(subscription); }
        private async Task CancelMapAsync(MapSubscription subscription)
        {
            await mapControl.WaitAsync().ConfigureAwait(false);
            try
            {
                lock (state)
                {
                    if (mapSubscription != subscription || closeReason != null) return;
                    mapSubscription = null;
                }
                // 取消也是有明确响应的普通协议，不再存在无需关联、无法知道是否被处理的特殊帧。
                // Dispose 本身不阻塞 Unity 主线程；超时后关闭连接，服务端随连接退出清理订阅。
                using (var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    var reply = await MapCancelAsync(Guid.NewGuid().ToString("D"),
                        new MapWatchCancel { SubscriptionId = subscription.SubscriptionId }, deadline.Token).ConfigureAwait(false);
                    if (reply.SubscriptionId != subscription.SubscriptionId)
                        throw new InvalidDataException("Map cancel acknowledgment has a different subscription ID.");
                }
            }
            catch (Exception failure) { Close(failure); }
            finally { mapControl.Release(); }
        }

        void WireRoutes.IPushHandlers.OnMapViewUpdate(MapViewUpdate update)
        {
            MapSubscription current;
            lock (state) current = mapSubscription;
            // 视野切换/取消与网络传输并发，旧 ID 的迟到包只丢弃，不得污染新视野。
            // Publish 只操作有界队列，不在接收线程执行 Unity 渲染或其他用户回调。
            // Dispose 的一次性标志也约束自动取消：控制 ACK 尚未完成时可能连续收到坏增量，
            // 只允许排入一个取消任务，不能把有界更新队列变成无界的后台取消任务队列。
            if (current != null && update.SubscriptionId == current.SubscriptionId && !current.Publish(update))
                current.Dispose();
        }

        private static void ValidateMapId(string id)
        {
            if (string.IsNullOrEmpty(id) || !Regex.IsMatch(id, @"\A[A-Za-z0-9_-]{1,64}\z"))
                throw new ArgumentException("Map subscription ID must have 1..64 URL-safe characters.", nameof(id));
        }
    }
}

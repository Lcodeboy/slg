package com.slg.game;

import com.google.protobuf.CodedOutputStream;
import com.slg.aoi.MapStateSource;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.*;
import java.io.IOException;

/**
 * 内部地图供数的线协议映射。输入只有公开 DTO，完整决定的切页与 wire 字节预算留在传输层，
 * 不依赖 SQL、旧日志格式或玩家实体；本地 AOI 与远程消费者因此采用相同的已提交后像。
 */
final class MapFeedWire {
    private MapFeedWire() { }

    static MapRoute route(MapStateSource.Route route) {
        return MapRoute.newBuilder().setMarchId(route.marchId()).setVersion(route.version())
                .setState(route.direction() == MapStateSource.Direction.OUTBOUND
                        ? MarchState.MARCH_STATE_OUTBOUND : MarchState.MARCH_STATE_RETURNING)
                .setFromX(route.fromX()).setFromY(route.fromY()).setTargetX(route.targetX()).setTargetY(route.targetY())
                .setDepartAt(route.departAt()).setArriveAt(route.arriveAt()).build();
    }

    static MapChangesPage page(MapStateSource.Page page, MapStateSource.Check check) throws Exception {
        validate(page);
        var output = MapChangesPage.newBuilder().setRealmId(page.identity().realmId())
                .setDatasetId(page.identity().datasetId()).setPrunedThrough(page.prunedThrough())
                .setHighSeq(page.highSeq()).setThroughSeq(page.afterSeq());
        // 先按最长 through 编码预留元数据；身份长度变化也不能越过 RPC 总字节上限。
        int remaining = RpcTransport.MAX_RESPONSE_BYTES
                - output.clone().setThroughSeq(Long.MAX_VALUE).build().getSerializedSize();
        if (remaining < 0) throw new IOException("Map page identity exceeds wire budget");
        int routeAt = 0, objectAt = 0;
        while (routeAt < page.changes().size() || objectAt < page.objectChanges().size()) {
            check.run();
            long routeSequence = routeAt < page.changes().size() ? page.changes().get(routeAt).sequence() : Long.MAX_VALUE;
            long objectSequence = objectAt < page.objectChanges().size() ? page.objectChanges().get(objectAt).sequence() : Long.MAX_VALUE;
            long sequence = Math.min(routeSequence, objectSequence);
            var routes = new java.util.ArrayList<MapRouteChange>();
            var objects = new java.util.ArrayList<MapObjectChange>();
            long bytes = 0;
            while (routeAt < page.changes().size() && page.changes().get(routeAt).sequence() == sequence) {
                var change = page.changes().get(routeAt++);
                var value = MapRouteChange.newBuilder().setSequence(sequence).setMarchId(change.marchId()).setVersion(change.version());
                if (change.route() == null) value.setRemoved(true); else value.setUpsert(route(change.route()));
                var encoded = value.build(); routes.add(encoded);
                bytes += CodedOutputStream.computeMessageSize(MapChangesPage.CHANGES_FIELD_NUMBER, encoded);
            }
            while (objectAt < page.objectChanges().size() && page.objectChanges().get(objectAt).sequence() == sequence) {
                var change = page.objectChanges().get(objectAt++);
                var value = MapObjectChange.newBuilder().setSequence(sequence).setObjectId(change.objectId()).setVersion(change.version());
                if (change.object() == null) value.setRemoved(true); else value.setUpsert(MapObjectWire.encode(change.object()));
                var encoded = value.build(); objects.add(encoded);
                bytes += CodedOutputStream.computeMessageSize(MapChangesPage.OBJECT_CHANGES_FIELD_NUMBER, encoded);
            }
            if (bytes > remaining) {
                // 两类变更按同一 sequence 合并计费。此前没有地图变化的决定也已完整读取，
                // 可以推进到 sequence-1，但绝不交付当前决定的一半占矿/路线变更。
                long through = sequence - 1;
                if (through == page.afterSeq()) throw new IOException("Single committed map decision exceeds wire budget");
                return output.setThroughSeq(through).build();
            }
            remaining -= (int) bytes;
            output.addAllChanges(routes).addAllObjectChanges(objects);
        }
        check.run();
        // 末尾可能只有非地图事件；不能用最后一个地图 DTO 的 sequence 代替完整读取边界。
        return output.setThroughSeq(page.throughSeq()).build();
    }

    private static void validate(MapStateSource.Page page) throws MapStateSource.BadData {
        if (page.afterSeq() < 0 || page.prunedThrough() < 0 || page.afterSeq() < page.prunedThrough()
                || page.throughSeq() < page.afterSeq() || page.highSeq() < page.throughSeq()
                || page.throughSeq() == page.afterSeq() && page.afterSeq() < page.highSeq())
            throw new MapStateSource.BadData("Invalid map page prefix");
        long previous = page.afterSeq();
        for (var change : page.changes()) {
            requireSequence(change.sequence(), previous, page); previous = change.sequence();
        }
        previous = page.afterSeq();
        for (var change : page.objectChanges()) {
            requireSequence(change.sequence(), previous, page); previous = change.sequence();
        }
    }

    private static void requireSequence(long sequence, long previous, MapStateSource.Page page) throws MapStateSource.BadData {
        if (sequence <= page.afterSeq() || sequence < previous || sequence > page.throughSeq())
            throw new MapStateSource.BadData("Map changes are outside their complete page prefix");
    }
}

package com.slg.game.aoi;

import com.slg.aoi.MapStateSource;
import com.slg.game.domain.march.MarchMap;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.util.ArrayList;
import java.util.List;

/**
 * 已提交结构化后像到公开地图 DTO 的纯投影。快照与日志使用同一转换，不查询当前玩家、
 * 不根据 factType 推测变化，也不暴露兵力/货物/账号。资料、行军和矿点各用自己的根版本。
 */
public final class StructuredMapProjection {
    private StructuredMapProjection() { }

    public static MapStateSource.Route route(VersionedState value) throws MapStateSource.BadData {
        if (!(value.state() instanceof MarchState march)) throw bad("Map march scan returned another object kind", null);
        try {
            if (march.status() != MarchState.Status.OUTBOUND && march.status() != MarchState.Status.RETURNING) return null;
            PlayerIdentity.accountOf(march.playerId());
            // 与当前 1200×1200 玩法地图一致；不能把存储允许的任意非负坐标直接当作可展示路线。
            if (march.fromX() > 1199 || march.fromY() > 1199 || march.targetX() > 1199 || march.targetY() > 1199)
                throw new IllegalArgumentException("Map route is outside the configured world");
            return new MapStateSource.Route(march.marchId(), value.version(),
                    march.status() == MarchState.Status.OUTBOUND ? MapStateSource.Direction.OUTBOUND : MapStateSource.Direction.RETURNING,
                    march.fromX(), march.fromY(), march.targetX(), march.targetY(), march.departAt(), march.arriveAt());
        } catch (IllegalArgumentException invalid) { throw bad("Invalid committed map route", invalid); }
    }

    public static MapStateSource.City city(VersionedState value, int realmId) throws MapStateSource.BadData {
        if (!(value.state() instanceof PlayerState player)) throw bad("Map player scan returned another object kind", null);
        try {
            if (player.currentRealmId() != realmId || player.name().length() > 24 || !player.name().equals(player.name().strip()))
                throw new IllegalArgumentException("City differs from realm or public name rules");
            // 资源/兵力变化不会改变 PlayerState 根版本；城市不能随任意玩家资产事件伪增版本。
            return new MapStateSource.City(player.playerId(), value.version(), MarchMap.homeX(), MarchMap.homeY(), player.name());
        } catch (IllegalArgumentException invalid) { throw bad("Invalid committed public city", invalid); }
    }

    public static MapStateSource.ResourceSite site(VersionedState value) throws MapStateSource.BadData {
        if (!(value.state() instanceof GatherSiteState site)) throw bad("Map site scan returned another object kind", null);
        try {
            if (!MarchMap.isGatherSite(site.x(), site.y())) throw new IllegalArgumentException("Unknown configured resource site");
            if (site.occupantPlayerId() != null) {
                PlayerIdentity.accountOf(site.occupantPlayerId());
                if (!site.occupantMarchId().matches("[A-Za-z0-9_-]{1,64}"))
                    throw new IllegalArgumentException("Invalid occupying march identity");
            }
            return new MapStateSource.ResourceSite(site.x(), site.y(), value.version(), "WOOD",
                    site.occupantPlayerId() == null ? "" : site.occupantPlayerId());
        } catch (IllegalArgumentException invalid) { throw bad("Invalid committed public resource site", invalid); }
    }

    /** 从未占用的配置矿点也必须出现；首次占用前版本为 0，不与持久首版本 1 混淆。 */
    public static List<MapStateSource.ResourceSite> staticSites() {
        return MarchMap.gatherSites().stream()
                .map(tile -> new MapStateSource.ResourceSite(tile.x(), tile.y(), 0, "WOOD", "")).toList();
    }

    /** 一条决定的路线/点对象一起返回，适配层只能在整个决定之间切页。 */
    public static Changes changes(CommittedEvent event, int realmId) throws MapStateSource.BadData {
        var routes = new ArrayList<MapStateSource.Change>();
        var objects = new ArrayList<MapStateSource.ObjectChange>();
        try {
            for (ObjectChange change : event.decision().changes()) {
                long version = Math.incrementExact(change.expectedVersion());
                var value = new VersionedState(version, change.afterState());
                switch (change.afterState()) {
                    case MarchState march -> routes.add(new MapStateSource.Change(event.sequence(), march.marchId(), version, route(value)));
                    case PlayerState ignored -> {
                        var city = city(value, realmId);
                        objects.add(new MapStateSource.ObjectChange(event.sequence(), city.objectId(), version, city));
                    }
                    case GatherSiteState ignored -> {
                        var site = site(value);
                        // 释放矿点仍是这个对象的新空闲后像，不能把地块从公开地图删除。
                        objects.add(new MapStateSource.ObjectChange(event.sequence(), site.objectId(), version, site));
                    }
                    default -> { }
                }
            }
            return new Changes(routes, objects);
        } catch (IllegalArgumentException | ArithmeticException invalid) { throw bad("Invalid committed map change", invalid); }
    }

    public record Changes(List<MapStateSource.Change> routes, List<MapStateSource.ObjectChange> objects) {
        public Changes { routes = List.copyOf(routes); objects = List.copyOf(objects); }
        public int size() { return Math.addExact(routes.size(), objects.size()); }
    }

    private static MapStateSource.BadData bad(String message, Throwable cause) {
        return cause == null ? new MapStateSource.BadData(message) : new MapStateSource.BadData(message, cause);
    }
}

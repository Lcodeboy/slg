package com.slg.game;

import com.slg.aoi.MapStateSource;
import com.slg.rpc.v1.MapCityView;
import com.slg.rpc.v1.MapObjectView;
import com.slg.rpc.v1.MapResourceSiteView;

/**
 * 地图点对象的唯一线协议映射。查询、订阅和远程供数共用它，防止三条读取路径暴露不同字段。
 * 输入仅为公开不可变 DTO，不接收玩家实体，因此无法把账号、部队或资源库存误序列化到公共地图。
 */
final class MapObjectWire {
    private MapObjectWire() {}

    static MapObjectView encode(MapStateSource.WorldObject object) {
        var value = MapObjectView.newBuilder().setObjectId(object.objectId()).setVersion(object.version())
                .setX(object.x()).setY(object.y());
        switch (object) {
            case MapStateSource.City city -> value.setCity(MapCityView.newBuilder()
                    .setPlayerId(city.playerId()).setName(city.name()));
            case MapStateSource.ResourceSite site -> value.setResourceSite(MapResourceSiteView.newBuilder()
                    .setResourceType(site.resourceType()).setOccupantPlayerId(site.occupantPlayerId()));
        }
        return value.build();
    }
}

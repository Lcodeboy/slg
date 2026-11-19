package com.slg.core.world;

import java.util.Objects;

/**
 * 一段行军的不可变几何与时间快照，用于路线可见性查询和客户端位置插值。
 * 兵力预留、城市状态与到达结算由游戏领域负责，路线对象本身不修改库存。
 *
 * @param marchId 行军唯一标识，改道时沿用该标识
 * @param from 本段路线起点，单位为地图格
 * @param to 本段路线终点，单位为地图格
 * @param departAt 本段出发时间，UTC 纪元毫秒，不能为负
 * @param arriveAt 本段预计到达时间，UTC 纪元毫秒，必须晚于出发时间
 * @param version 路线的非负版本号，更新已有路线时必须递增
 */
public record MarchRoute(String marchId, Point from, Point to,
                         long departAt, long arriveAt, long version) {
    /** 校验标识、端点与时间先后关系；地图边界由路线索引校验。 */
    public MarchRoute {
        if (marchId == null || marchId.isBlank()) {
            throw new IllegalArgumentException("March identifier is required");
        }
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        if (departAt < 0 || arriveAt <= departAt || version < 0) {
            throw new IllegalArgumentException("Arrival must follow departure and version must be nonnegative");
        }
    }

    /**
     * 按匀速直线运动计算指定时刻的显示位置，时间落在行军区间外时返回对应端点。
     * 该位置无需逐帧持久化，也不决定到达任务是否已经完成。
     *
     * @param timestamp 查询时间，UTC 纪元毫秒
     * @return 插值后的世界坐标
     */
    public Point positionAt(long timestamp) {
        if (timestamp <= departAt) return from;
        if (timestamp >= arriveAt) return to;
        double progress = (double) (timestamp - departAt) / (arriveAt - departAt);
        return new Point(from.x() + (to.x() - from.x()) * progress,
                from.y() + (to.y() - from.y()) * progress);
    }
}

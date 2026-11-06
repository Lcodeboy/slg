package com.slg.core.world;

/**
 * 客户端观察区域的闭合矩形，边界上的点也属于视野。
 * 允许超出地图边界，也允许退化成线或点；路线索引负责裁剪查询范围。
 *
 * @param minX 横向最小坐标，单位为地图格
 * @param minY 纵向最小坐标，单位为地图格
 * @param maxX 横向最大坐标，单位为地图格，不能小于 minX
 * @param maxY 纵向最大坐标，单位为地图格，不能小于 minY
 */
public record Viewport(double minX, double minY, double maxX, double maxY) {
    /** 校验所有边界有限且顺序正确，不强制矩形具有正面积。 */
    public Viewport {
        if (!Double.isFinite(minX) || !Double.isFinite(minY)
                || !Double.isFinite(maxX) || !Double.isFinite(maxY)
                || minX > maxX || minY > maxY) {
            throw new IllegalArgumentException("Viewport must have finite, ordered bounds");
        }
    }

    /**
     * 按闭区间判断点是否位于视野中，此方法不额外引入浮点容差。
     *
     * @param point 待判断的世界坐标
     * @return 点位于矩形内部或边界时为 true
     */
    public boolean contains(Point point) {
        return point.x() >= minX && point.x() <= maxX
                && point.y() >= minY && point.y() <= maxY;
    }
}

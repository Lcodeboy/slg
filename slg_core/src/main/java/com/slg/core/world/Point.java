package com.slg.core.world;

/**
 * 世界地图上的连续坐标，以地图格为长度单位，允许小数表示行军中的位置。
 * 本类型只约束数值有限，具体地图的边界由 {@link RouteIndex} 校验。
 *
 * @param x 横向坐标，单位为地图格
 * @param y 纵向坐标，单位为地图格
 */
public record Point(double x, double y) {
    /** 拒绝非数值和无穷大，避免几何运算产生不可比较的结果。 */
    public Point {
        if (!Double.isFinite(x) || !Double.isFinite(y)) {
            throw new IllegalArgumentException("Point coordinates must be finite");
        }
    }
}

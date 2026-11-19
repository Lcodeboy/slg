package com.slg.game.domain.march;

import java.util.List;

/** 当前部署版本的纯空间配置；权威规则与地图适配共用，不允许绕过事务热改行军中的目标。 */
public final class MarchMap {
    public record Tile(int x, int y) { }
    private static final List<Tile> GATHER = List.of(new Tile(150, 150), new Tile(650, 650), new Tile(1150, 1150));
    private MarchMap() { }
    public static int homeX() { return 0; }
    public static int homeY() { return 0; }
    public static List<Tile> gatherSites() { return GATHER; }
    public static boolean isGatherSite(int x, int y) { return GATHER.stream().anyMatch(tile -> tile.x() == x && tile.y() == y); }
    public static boolean isNpc(int x, int y) { return x == y && (x == 120 || x == 600 || x == 1100); }
}

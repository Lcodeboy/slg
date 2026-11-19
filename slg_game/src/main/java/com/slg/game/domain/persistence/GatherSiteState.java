package com.slg.game.domain.persistence;

/**
 * 矿点的结构化占用行。空闲时保留行和版本，避免旧释放命令与后来占用混淆；两个属主
 * 同时为空且 generation=0 才表示空闲。占用/释放必须与对应行军和任务一起提交。
 * 坐标是否属于实际开放矿点仍由配置及领域规则验证。
 */
public record GatherSiteState(int x, int y, String occupantPlayerId, String occupantMarchId,
                              long generation) implements ObjectState {
    public GatherSiteState {
        new ObjectKey.GatherSite(x, y);
        PersistenceChecks.optionalIdentifier(occupantPlayerId, "occupantPlayerId", 256);
        PersistenceChecks.optionalIdentifier(occupantMarchId, "occupantMarchId", 256);
        boolean empty = occupantPlayerId == null && occupantMarchId == null && generation == 0;
        boolean occupied = occupantPlayerId != null && occupantMarchId != null && generation > 0;
        if (!empty && !occupied) throw new IllegalArgumentException("Incomplete or invalid gather-site reservation");
    }

    @Override public ObjectKey.GatherSite key() { return new ObjectKey.GatherSite(x, y); }
}

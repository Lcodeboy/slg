package com.slg.game.domain.persistence;

/**
 * 当前已有的英雄占用行，assignedMarchId 为 null 表示空闲。分配/释放须与对应行军、
 * 玩家兵力在同一事务完成；这里不引入英雄养成或 PVP 规则。
 */
public record HeroState(String playerId, String heroId, String assignedMarchId) implements ObjectState {
    public HeroState {
        new ObjectKey.Hero(playerId, heroId);
        PersistenceChecks.optionalIdentifier(assignedMarchId, "assignedMarchId", 256);
    }
    @Override public ObjectKey.Hero key() { return new ObjectKey.Hero(playerId, heroId); }
}

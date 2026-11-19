package com.slg.game.domain.persistence;

import com.slg.core.battle.TroopType;

/**
 * 一个玩家、一个兵种的可用与伤亡数量，作为同一行一起更新。出征中兵力由对应行军
 * 明细保存，不能再计入 available。单行非负检查不替代跨玩家/行军的兵力守恒验收。
 */
public record TroopState(String playerId, TroopType troopType, long available, long wounded, long dead)
        implements ObjectState {
    public TroopState {
        new ObjectKey.Troop(playerId, troopType);
        PersistenceChecks.nonNegative(available, "available");
        PersistenceChecks.nonNegative(wounded, "wounded");
        PersistenceChecks.nonNegative(dead, "dead");
    }
    @Override public ObjectKey.Troop key() { return new ObjectKey.Troop(playerId, troopType); }
}

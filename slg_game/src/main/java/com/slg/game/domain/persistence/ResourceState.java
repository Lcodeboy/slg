package com.slg.game.domain.persistence;

import com.slg.core.economy.ResourceType;

/**
 * 一个玩家的一类资源余额，独立结构化行及版本。amount 是本次裁决后的绝对余额，
 * 不是可以脱离原请求无限重放的增量；扣除/奖励及相关回执须在完整业务事务中提交。
 */
public record ResourceState(String playerId, ResourceType resourceType, long amount) implements ObjectState {
    public ResourceState {
        new ObjectKey.Resource(playerId, resourceType);
        PersistenceChecks.nonNegative(amount, "amount");
    }
    @Override public ObjectKey.Resource key() { return new ObjectKey.Resource(playerId, resourceType); }
}

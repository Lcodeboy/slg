package com.slg.game.domain.persistence;

import java.util.Objects;

/**
 * 角色身份与资料行，不再携带资源、兵力、英雄或活动行军集合。playerId 沿用建角区号和
 * UC 正数账号组成的永久身份；currentRealmId 可以不同。每区账号唯一由仓储事务及唯一键
 * 保证，单个值的构造无法证明数据库中没有另一个角色。版本只描述本资料行。
 */
public record PlayerState(String playerId, long accountId, int foundingRealmId, int currentRealmId,
                          String name) implements ObjectState {
    public PlayerState {
        PersistenceChecks.positive(accountId, "accountId");
        PersistenceChecks.positive(foundingRealmId, "foundingRealmId");
        PersistenceChecks.positive(currentRealmId, "currentRealmId");
        Objects.requireNonNull(playerId, "playerId");
        if (!(foundingRealmId + "_" + accountId).equals(playerId))
            throw new IllegalArgumentException("Player ID must match its founding realm and account");
        Objects.requireNonNull(name, "name");
        if (name.isBlank() || name.length() > 128 || name.codePoints().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("Name must be nonblank, bounded and free of control characters");
    }

    @Override public ObjectKey.Player key() { return new ObjectKey.Player(playerId); }
}

package com.slg.game.domain.persistence;

import java.util.Objects;
import java.util.UUID;

/**
 * 一次写者任期的不可变声明。适配器仍须在事务内核验区服、进程与正数代次，
 * 不能因令牌构造合法就信任旧进程的写入；接管后旧令牌不得继续提交。
 */
public record WriterToken(RealmIdentity realm, UUID instanceId, long epoch) {
    public WriterToken {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(instanceId, "instanceId");
        PersistenceChecks.positive(epoch, "epoch");
    }
}

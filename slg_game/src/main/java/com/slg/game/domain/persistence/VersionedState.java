package com.slg.game.domain.persistence;

import java.util.Objects;

/**
 * 一个已存在对象的版本与完整状态。版本必须为正数；对象不存在应由读取端口单独表达，
 * 不能伪造版本 0 的状态。允许读取最大版本，但该对象不能再通过版本加一更新。
 * 状态来自结构化行，行军和战斗还包含同版本的全部明细；读接口不能拼接不同快照的行。
 * 角色资料行的版本不代表其所有资源、兵力和英雄行版本，聚合缓存必须保留各行版本。
 */
public record VersionedState(long version, ObjectState state) {
    public VersionedState {
        PersistenceChecks.positive(version, "version");
        Objects.requireNonNull(state, "state");
    }
}

package com.slg.game.domain.persistence;

import java.util.Objects;

/**
 * 对单个结构化对象的完整类型化后像及提交前版本约束，必须与同一决定的其他变化一起
 * 提交或失败。key 必须与后像的类型和全部主键分量相同，防止用矿点状态覆盖资源行。
 * 预期版本 0 仅允许首次创建，正数要求原版本匹配，成功后的版本固定加一；最大 long
 * 不能作为预期版本，以免溢出。首版不表达硬删除，不得用空后像冒充删除。
 */
public record ObjectChange(ObjectKey key, long expectedVersion, ObjectState afterState) {
    public ObjectChange {
        Objects.requireNonNull(key, "key");
        if (expectedVersion < 0 || expectedVersion == Long.MAX_VALUE)
            throw new IllegalArgumentException("expectedVersion must permit incrementing without overflow");
        Objects.requireNonNull(afterState, "afterState");
        if (!key.equals(afterState.key()))
            throw new IllegalArgumentException("Change key must match the typed after-state key");
    }
}

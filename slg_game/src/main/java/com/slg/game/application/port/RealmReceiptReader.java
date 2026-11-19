package com.slg.game.application.port;

import com.slg.game.domain.persistence.CommitReceipt;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.RequestKey;
import java.util.Objects;

/** 回执的权威读取；查询结果是一次观察，不代替未决事务的隔离和核对。 */
public interface RealmReceiptReader {
    /**
     * 从指定数据集读取原回执。不可读、错身份或回执损坏应抛错，不能伪装成不存在。
     * NotObserved 不能证明原尝试未提交：旧事务可能尚未结束，之后仍可能提交。
     */
    Observation observe(RealmIdentity identity, RequestKey key, ReadBudget budget) throws StorageAccessException;

    sealed interface Observation permits Observation.Found, Observation.NotObserved {
        record Found(CommitReceipt receipt) implements Observation {
            public Found { Objects.requireNonNull(receipt, "receipt"); }
        }

        /** 仅表示此刻没有观察到；不允许据此重复发奖或释放结果未知的对象。 */
        record NotObserved() implements Observation {}
    }
}

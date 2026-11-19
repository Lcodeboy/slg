package com.slg.game.domain.persistence;

import java.util.Objects;
import java.util.UUID;

/**
 * 区号与持久化数据集的稳定身份；同一区号的新空数据集必须另有身份，防止误接旧历史。
 * 数据集标识不等于进程标识或写者代次；正常重启应继续核验同一份身份。
 */
public record RealmIdentity(int realmId, UUID datasetId) {
    public RealmIdentity {
        PersistenceChecks.positive(realmId, "realmId");
        Objects.requireNonNull(datasetId, "datasetId");
    }
}

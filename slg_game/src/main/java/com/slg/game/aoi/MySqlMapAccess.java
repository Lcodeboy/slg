package com.slg.game.aoi;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmStateReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.PlayerState;
import com.slg.game.domain.persistence.RealmIdentity;
import java.util.Objects;

/**
 * 地图入口的 MySQL 角色归属检查，可直接以 allows 方法引用接入 AoiRpcService.RealmAccess。
 * 阻塞读取由现有地图 admission 有界线程执行，不另开池、不加载玩家目录、不取得写资格。
 * 会话在读取前后的复核仍归 RPC 入口；这里的一份短 RR 快照仅证明该视图存在本区角色。
 */
public final class MySqlMapAccess {
    private final RealmStateReader states;
    private final RealmIdentity realm;
    private final ReadBudget budget;

    public MySqlMapAccess(RealmStateReader states, RealmIdentity realm, ReadBudget budget) {
        this.states = Objects.requireNonNull(states);
        this.realm = Objects.requireNonNull(realm);
        this.budget = Objects.requireNonNull(budget);
    }

    /** 缺角色才返回 false；失联、数据集不符或坏归属必须传播失败，不能伪装成权限拒绝。 */
    public boolean allows(String account) throws StorageAccessException {
        if (account == null) throw new IllegalArgumentException("Canonical account ID required");
        long accountId = Long.parseLong(account);
        if (accountId <= 0 || !Long.toString(accountId).equals(account))
            throw new IllegalArgumentException("Canonical positive account ID required");
        try (var snapshot = states.openSnapshot(realm, budget)) {
            if (!realm.equals(snapshot.identity()))
                throw new StorageAccessException(StorageAccessException.Code.IDENTITY_MISMATCH,
                        "Map access snapshot differs from configured dataset");
            var value = snapshot.findPlayerByAccount(accountId);
            if (value.isEmpty()) return false;
            if (!(value.orElseThrow().state() instanceof PlayerState player)
                    || player.accountId() != accountId || player.currentRealmId() != realm.realmId())
                throw new StorageAccessException(StorageAccessException.Code.INVALID_DATA,
                        "Map role differs from requested account or realm");
            return true;
        }
    }
}

package com.slg.game.realm;

import com.slg.core.auth.AccountIds;

/** 角色 ID 由建角区服和 UC 账号共同确定。区服号不可重用，两个十进制分量用下划线分隔。 */
public final class PlayerIdentity {
    private PlayerIdentity() {}

    public static String atCreation(int foundingRealmId, String accountId) {
        if (foundingRealmId <= 0 || !AccountIds.isValid(accountId))
            throw new IllegalArgumentException("Invalid founding realm or account ID");
        return foundingRealmId + "_" + accountId;
    }

    /** 定时任务、战斗回调只有持久的角色 ID，取出其账号才能与该玩家的客户端请求进入同一个邮箱。 */
    public static String accountOf(String playerId) {
        if (playerId == null || playerId.length() > 30)
            throw new IllegalArgumentException("Invalid player ID");
        int separator = playerId.indexOf('_');
        if (separator <= 0 || separator > 10)
            throw new IllegalArgumentException("Invalid player ID");
        String account = playerId.substring(separator + 1);
        int foundingRealm = Integer.parseInt(playerId, 0, separator, 10);
        if (!atCreation(foundingRealm, account).equals(playerId))
            throw new IllegalArgumentException("Noncanonical player ID");
        return account;
    }
}

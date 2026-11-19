package com.slg.game.domain.player;

import java.util.Objects;

/** 纯领域角色身份；建角区和账号确定永久 ID，当前所在区不能用来重算已有角色 ID。 */
public final class PlayerIdentity {
    private PlayerIdentity() {}

    public static String atCreation(int foundingRealmId, long accountId) {
        if (foundingRealmId <= 0 || accountId <= 0) throw new IllegalArgumentException("Invalid player identity components");
        return foundingRealmId + "_" + accountId;
    }

    /**
     * 只接受唯一的 ASCII 十进制表示。正负号、前导零、Unicode 数字和溢出均拒绝，避免同一
     * 账号获得不同执行资格或角色键；不通过宽松转换把坏持久化 ID 修正成另一个合法身份。
     */
    public static Components parse(String playerId) {
        Objects.requireNonNull(playerId, "playerId");
        if (playerId.isEmpty() || playerId.length() > 30) throw new IllegalArgumentException("Invalid player ID length");
        int separator = playerId.indexOf('_');
        if (separator <= 0 || separator > 10 || separator == playerId.length() - 1
                || playerId.indexOf('_', separator + 1) >= 0)
            throw new IllegalArgumentException("Invalid player ID components");
        int foundingRealmId = Integer.parseInt(playerId, 0, separator, 10);
        long accountId = Long.parseLong(playerId, separator + 1, playerId.length(), 10);
        if (!atCreation(foundingRealmId, accountId).equals(playerId))
            throw new IllegalArgumentException("Player ID must be canonical");
        return new Components(foundingRealmId, accountId);
    }

    public static long accountOf(String playerId) { return parse(playerId).accountId(); }

    public record Components(int foundingRealmId, long accountId) {
        public Components {
            if (foundingRealmId <= 0 || accountId <= 0) throw new IllegalArgumentException("Invalid player identity components");
        }
    }
}

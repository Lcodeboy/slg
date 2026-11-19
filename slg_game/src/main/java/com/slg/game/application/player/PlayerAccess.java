package com.slg.game.application.player;

import java.util.Objects;
import java.util.UUID;

/**
 * 入口验签后复制的会话身份，只用于内存排队和执行前复核。它不是客户端可自报的角色ID，
 * 也不进入业务事实、请求摘要或持久回执；重连更换绑定不改变原请求的幂等身份。
 */
public record PlayerAccess(long accountId, long issuedAt, long expiresAt, String gameInstance,
                           long generation, String bindingId) {
    public PlayerAccess {
        if (accountId <= 0 || issuedAt < 0 || expiresAt <= issuedAt || generation <= 0)
            throw new IllegalArgumentException("Invalid authenticated player access");
        canonicalUuid(gameInstance); canonicalUuid(bindingId);
    }
    private static void canonicalUuid(String value) {
        if (!UUID.fromString(Objects.requireNonNull(value)).toString().equals(value))
            throw new IllegalArgumentException("Noncanonical session identity");
    }
}

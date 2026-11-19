package com.slg.game.application.player;

import java.util.Objects;

/** 缓存准入或生命周期失败，不能解释成账号没有角色，也不能触发重新发放初始资产。 */
public final class PlayerCacheException extends IllegalStateException {
    private static final long serialVersionUID = 1L;
    public enum Reason { CAPACITY, NOT_PINNED, CLOSED, SUPERSEDED }
    private final Reason reason;

    public PlayerCacheException(Reason reason, String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason");
    }
    public Reason reason() { return reason; }
}

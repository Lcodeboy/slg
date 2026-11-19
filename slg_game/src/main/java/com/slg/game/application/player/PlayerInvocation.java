package com.slg.game.application.player;

import java.util.Objects;

/** 单次玩家调用的不可变输入；value 是原始昵称、查询目标ID或空串，由注册的操作类型解释。 */
public record PlayerInvocation(PlayerAccess access, String value) {
    public PlayerInvocation { Objects.requireNonNull(access); Objects.requireNonNull(value); }
}

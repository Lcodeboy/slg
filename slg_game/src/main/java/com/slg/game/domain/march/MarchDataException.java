package com.slg.game.domain.march;

/** 权威行军、任务或占用不一致；应用必须停止本次修改，不能持久化成普通业务拒绝或补发资产。 */
public final class MarchDataException extends RuntimeException {
    public MarchDataException(String message) { super(message); }
}

package com.slg.game.realm;

import java.util.concurrent.TimeoutException;

/** 玩家命令在邮箱执行前超过截止时间，调用方可使用同一请求 ID 重试。 */
public final class QueueDeadlineException extends TimeoutException {
    public QueueDeadlineException() {
        super("Command deadline elapsed before execution; no business operation was started");
    }
}

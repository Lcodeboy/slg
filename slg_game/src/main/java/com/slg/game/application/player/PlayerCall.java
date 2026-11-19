package com.slg.game.application.player;

import java.util.concurrent.CompletionStage;

/** 外部取消只竞争尚未执行的操作；返回隔离stage，不能取消内部提交或提前释放缓存引用。 */
public interface PlayerCall<T> {
    CompletionStage<T> result();
    boolean cancelBeforeStart();
}

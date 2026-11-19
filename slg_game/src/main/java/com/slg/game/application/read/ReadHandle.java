package com.slg.game.application.read;

import java.util.concurrent.CompletionStage;

/** 冷读的隔离观测句柄；取消派生 Future 不改变真实读取和内部资源计量。 */
public interface ReadHandle<T> {
    CompletionStage<T> result();

    /** 仅与尚未开始的任务原子竞争；成功后仍保留准入预算至取消结果在通知线程交付。 */
    boolean cancelBeforeStart();
}

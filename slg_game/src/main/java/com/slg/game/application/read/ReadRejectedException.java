package com.slg.game.application.read;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

/** 受理前同步拒绝，保证不会打开快照；与已经开始的读取失败区分。 */
public final class ReadRejectedException extends RejectedExecutionException {
    private static final long serialVersionUID = 1L;
    public enum Reason { CAPACITY, STOPPED }
    private final Reason reason;

    public ReadRejectedException(Reason reason) {
        super(reason == Reason.CAPACITY ? "Cold-read admission budget exhausted" : "Cold-read executor is draining");
        this.reason = Objects.requireNonNull(reason, "reason");
    }
    public Reason reason() { return reason; }
}

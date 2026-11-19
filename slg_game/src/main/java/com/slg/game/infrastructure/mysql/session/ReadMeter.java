package com.slg.game.infrastructure.mysql.session;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.StorageAccessException;
import java.util.Objects;

import static com.slg.game.application.port.StorageAccessException.Code.READ_BUDGET_EXCEEDED;

/** 一份读取/快照共用一个计量器；主行与明细累计计量，不能在每次 Mapper 调用时重置。 */
public final class ReadMeter {
    private final ReadBudget budget;
    private final long started = System.nanoTime();
    private final long timeoutNanos;
    private int rows;
    private long bytes;

    public ReadMeter(ReadBudget budget) {
        this.budget = Objects.requireNonNull(budget, "budget");
        long nanos;
        try { nanos = budget.timeout().toNanos(); }
        catch (ArithmeticException overflow) { nanos = Long.MAX_VALUE; }
        timeoutNanos = nanos;
    }

    public void checkTime() throws StorageAccessException {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - started >= timeoutNanos)
            throw exceeded();
    }

    /** 完整扫描不截断；页读取可先 canFit，第一项也放不下时仍须明确抛错。 */
    public void add(int additionalRows, long additionalBytes) throws StorageAccessException {
        if (!canFit(additionalRows, additionalBytes)) throw exceeded();
        rows += additionalRows;
        bytes += additionalBytes;
    }

    public boolean canFit(int additionalRows, long additionalBytes) throws StorageAccessException {
        checkTime();
        if (additionalRows < 0 || additionalBytes < 0) throw new IllegalArgumentException("Negative read cost");
        return additionalRows <= remainingRows() && additionalBytes <= remainingBytes();
    }

    public int remainingRows() { return budget.maxRows() - rows; }
    public long remainingBytes() { return budget.maxBytes() - bytes; }

    private static StorageAccessException exceeded() {
        return new StorageAccessException(READ_BUDGET_EXCEEDED, "Authority read budget exceeded");
    }
}

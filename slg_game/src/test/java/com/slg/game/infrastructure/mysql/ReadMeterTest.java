package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.infrastructure.mysql.session.ReadMeter;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ReadMeterTest {
    @Test void costsAccumulateAndCannotOverflowIntoAnAcceptedRead() throws Exception {
        var meter = new ReadMeter(new ReadBudget(3, Long.MAX_VALUE, Duration.ofDays(1)));
        meter.add(2, Long.MAX_VALUE - 1);
        assertTrue(meter.canFit(1, 1)); assertFalse(meter.canFit(2, 1)); assertFalse(meter.canFit(1, 2));
        meter.add(1, 1);
        assertEquals(0, meter.remainingRows()); assertEquals(0, meter.remainingBytes());
        assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED,
                assertThrows(StorageAccessException.class, () -> meter.add(1, Long.MAX_VALUE)).code());
    }
    @Test void interruptedReadsFailWithoutReportingAnEmptySuccess() {
        var meter = new ReadMeter(new ReadBudget(1, 1, Duration.ofSeconds(1)));
        Thread.currentThread().interrupt();
        try { assertThrows(StorageAccessException.class, meter::checkTime); }
        finally { Thread.interrupted(); }
    }
}

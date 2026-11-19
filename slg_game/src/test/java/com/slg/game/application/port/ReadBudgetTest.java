package com.slg.game.application.port;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReadBudgetTest {
    @Test void everyBudgetDimensionMustPermitPositiveWork() {
        assertThrows(IllegalArgumentException.class, () -> new ReadBudget(0, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new ReadBudget(-1, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new ReadBudget(1, 0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new ReadBudget(1, -1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new ReadBudget(1, 1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new ReadBudget(1, 1, Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> new ReadBudget(1, 1, null));
    }

    @Test void keepsPositiveSubMillisecondBudgetsWithoutTruncatingToZero() {
        var budget = new ReadBudget(1, 1, Duration.ofNanos(1));
        assertEquals(Duration.ofNanos(1), budget.timeout());
        assertEquals(1, budget.maxRows());
        assertEquals(1, budget.maxBytes());
    }
}

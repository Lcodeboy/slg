package com.slg.rpc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WireStatusTest {
    @Test void confirmedVersionAndRequestConflictsDoNotBecomeUnknownThroughTcp() {
        assertEquals(WireStatus.NOT_ACCEPTED, WireStatus.fromError(409, "STATE_CHANGED"));
        assertEquals(WireStatus.REJECTED, WireStatus.fromError(409, "REQUEST_CONFLICT"));
        assertEquals(WireStatus.OUTCOME_UNKNOWN, WireStatus.fromError(503, "RESULT_UNKNOWN"));
    }
}

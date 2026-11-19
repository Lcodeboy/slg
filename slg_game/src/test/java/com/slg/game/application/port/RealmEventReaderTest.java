package com.slg.game.application.port;

import com.slg.game.domain.persistence.CommitDecision;
import com.slg.game.domain.persistence.CommittedEvent;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.RequestKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RealmEventReaderTest {
    private static final RealmIdentity REALM = new RealmIdentity(1, UUID.fromString("00000000-0000-0000-0000-000000000001"));

    @Test void partialPagesRetainCompleteDecisionsAndCannotBeChangedByTheCaller() {
        var supplied = new ArrayList<>(List.of(event(3), event(4)));
        var page = new RealmEventReader.EventPage(REALM, 2, 2, 7, 4, supplied);
        supplied.clear();
        assertEquals(List.of(event(3), event(4)), page.events());
        assertEquals(4, page.through());
        assertEquals(7, page.high());
        assertThrows(UnsupportedOperationException.class, () -> page.events().add(event(5)));
    }

    @Test void rejectsGapsDuplicatesReverseOrderAndUnrepresentedProgress() {
        assertThrows(IllegalArgumentException.class,
                () -> page(0, 0, 3, 3, event(1), event(3)));
        assertThrows(IllegalArgumentException.class,
                () -> page(0, 0, 2, 2, event(1), event(1)));
        assertThrows(IllegalArgumentException.class,
                () -> page(0, 0, 2, 2, event(2), event(1)));
        assertThrows(IllegalArgumentException.class,
                () -> page(0, 0, 3, 2, event(1)));
        assertThrows(IllegalArgumentException.class,
                () -> page(0, 0, 3, 1, event(1), event(2)));
    }

    @Test void emptyPageIsValidOnlyAtTheObservedHighWatermark() {
        assertEquals(0, page(0, 0, 0, 0).through());
        assertEquals(7, page(7, 3, 7, 7).through());
        assertThrows(IllegalArgumentException.class, () -> page(4, 3, 7, 4));
    }

    @Test void rejectsExpiredAndImpossibleWatermarkRanges() {
        assertThrows(IllegalArgumentException.class, () -> page(1, 2, 3, 2, event(2)));
        assertThrows(IllegalArgumentException.class, () -> page(0, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> page(-1, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> page(2, 0, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> page(2, 0, 3, 1));
        assertThrows(IllegalArgumentException.class, () -> page(2, 0, 3, 4, event(3), event(4)));
    }

    @Test void lastRepresentableSequenceDoesNotWrapIntoAFalseNextEvent() {
        var last = page(Long.MAX_VALUE - 1, 0, Long.MAX_VALUE, Long.MAX_VALUE, event(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, last.through());
        assertTrue(page(Long.MAX_VALUE, 0, Long.MAX_VALUE, Long.MAX_VALUE).events().isEmpty());
        assertThrows(IllegalArgumentException.class,
                () -> page(Long.MAX_VALUE, 0, Long.MAX_VALUE, Long.MAX_VALUE, event(1)));
    }

    private static RealmEventReader.EventPage page(long after, long pruned, long high, long through,
                                                   CommittedEvent... events) {
        return new RealmEventReader.EventPage(REALM, after, pruned, high, through, List.of(events));
    }

    private static CommittedEvent event(long sequence) {
        return new CommittedEvent(sequence, new CommitDecision(new RequestKey("player:1", "request-" + sequence),
                "digest-" + sequence, "COMMAND_REJECTED", "{}", "{}", List.of(), Map.of()));
    }
}

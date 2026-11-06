package com.slg.game.storage;

import com.slg.game.storage.JournalRetentionPreview.Consumer;
import com.slg.game.storage.JournalRetentionPreview.RecoveryPoint;
import com.slg.game.storage.JournalRetentionPreview.Result;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JournalRetentionPreviewTest {
    private static final UUID VOLUME = UUID.fromString("5c9f47b1-68c5-4c7e-9bfb-23e5370ac22a");
    private static final UUID OTHER = UUID.fromString("b197e5cd-a011-4b56-99b8-24755cf2a43f");

    @Test void laggingConsumerNeedsItsOwnBoundaryEntryAfterReconnect() {
        var result = plan(history(25), Set.of("mysql", "map"), Map.of("mysql", mysql(25),
                "map", new Consumer(1, VOLUME, 9L, "CATCHING_UP")), verified(20), 3);
        assertTrue(result.dryRun());
        assertEquals("CANDIDATE", result.state());
        assertEquals(1L, result.firstEligibleSeq());
        assertEquals(8L, result.lastEligibleSeq());
        assertEquals(List.of("consumer:map"), result.limitingFactors());
        assertEquals(24, result.bounds().get("consumer:mysql"));
        assertEquals(22, result.bounds().get("retentionWindow"));
        assertEquals(20, result.bounds().get("recoverySnapshot"));
    }

    @Test void allTiedSafetyBoundsRemainVisibleAndSnapshotIsInclusive() {
        var result = plan(history(25), Set.of("mysql"), Map.of("mysql", mysql(20)), verified(19), 6);
        assertEquals(19L, result.lastEligibleSeq());
        assertEquals(Set.of("consumer:mysql", "recoverySnapshot", "retentionWindow"),
                Set.copyOf(result.limitingFactors()));
        var snapshotOnly = plan(history(25), Set.of("mysql"), Map.of("mysql", mysql(20)), verified(7), 2);
        assertEquals(7L, snapshotOnly.lastEligibleSeq());
        assertEquals(List.of("recoverySnapshot"), snapshotOnly.limitingFactors());
    }

    @Test void zeroWindowStillPreservesTailAndZeroOrFirstCheckpointHasNoCandidate() {
        var tail = plan(history(5), Set.of("mysql"), Map.of("mysql", mysql(5)), verified(5), 0);
        assertEquals(1, tail.keepNewestEntries());
        assertEquals(4L, tail.lastEligibleSeq());
        assertEquals(Set.of("journalTail", "retentionWindow", "consumer:mysql"), Set.copyOf(tail.limitingFactors()));
        for (long cursor : List.of(0L, 1L)) {
            var result = plan(history(5), Set.of("mysql"), Map.of("mysql", mysql(cursor)), verified(5), 0);
            assertEquals("NO_CANDIDATES", result.state());
            assertEquals(List.of("consumer:mysql"), result.limitingFactors());
            assertNull(result.firstEligibleSeq());
            assertNull(result.lastEligibleSeq());
        }
        var empty = plan(history(0), Set.of("mysql"), Map.of("mysql", mysql(0)), verified(0), 12);
        assertEquals("NO_CANDIDATES", empty.state());
        assertNull(empty.lastEligibleSeq());
    }

    @Test void missingRecoveryProofStopsEvenWithCaughtUpMysqlAndReportsOtherIndependentFailures() {
        var result = plan(history(25), Set.of("mysql", "other"), Map.of("mysql", mysql(25)), null, 2);
        blocked(result, "VERIFIED_SNAPSHOT_MISSING", "CONSUMER_MISSING:other");
        assertEquals(24, result.bounds().get("consumer:mysql"));
        blocked(plan(history(25), Set.of(), Map.of(), verified(10), 2), "REQUIRED_CONSUMERS_UNKNOWN");
        blocked(plan(history(25), Set.of("map"), Map.of("map", mysql(25)), verified(10), 2),
                "MYSQL_CONSUMER_NOT_REGISTERED");
        blocked(plan(history(25), Set.of("mysql"), Map.of("mysql", mysql(25)),
                new RecoveryPoint(1, OTHER, 20, true), 2), "SNAPSHOT_IDENTITY_MISMATCH");
        blocked(plan(history(25), Set.of("mysql"), Map.of("mysql", mysql(25)),
                new RecoveryPoint(1, VOLUME, 20, false), 2), "SNAPSHOT_RESTORE_UNVERIFIED");
        for (long bad : List.of(-1L, 26L)) {
            blocked(plan(history(25), Set.of("mysql"), Map.of("mysql", mysql(25)), verified(bad), 2),
                    "SNAPSHOT_SEQUENCE_INVALID");
        }
    }

    @Test void staleForeignOrImpossibleConsumerCannotUseItsLastRememberedCursor() {
        for (String status : List.of("STARTING", "DISABLED", "BLOCKED", "RETRYING", "GAP", "unknown")) {
            var cursor = new Consumer(1, VOLUME, 15L, status);
            blocked(plan(history(25), Set.of("mysql"), Map.of("mysql", cursor), verified(22), 3),
                    "CONSUMER_UNCONFIRMED:mysql");
        }
        for (Long bad : new Long[]{null, -1L, 26L}) {
            blocked(plan(history(25), Set.of("mysql"), Map.of("mysql", new Consumer(1, VOLUME, bad, "CAUGHT_UP")),
                    verified(22), 3), "CONSUMER_CURSOR_INVALID:mysql");
        }
        for (Consumer foreign : List.of(new Consumer(2, VOLUME, 20L, "CAUGHT_UP"),
                new Consumer(1, OTHER, 20L, "CAUGHT_UP"))) {
            blocked(plan(history(25), Set.of("mysql"), Map.of("mysql", foreign), verified(22), 3),
                    "CONSUMER_IDENTITY_MISMATCH:mysql");
        }
    }

    @Test void holesAndUnknownLocalHistoryBlockEvenIfTheyArePastTheWouldBeCandidate() {
        var gap = new RealmJournal.Inspection(25, 1L, 25L, 21, "Missing journal entry 22");
        blocked(plan(gap, Set.of("mysql"), Map.of("mysql", mysql(10)), verified(10), 2),
                "JOURNAL_INSPECTION_FAILED", "JOURNAL_PREFIX_UNVERIFIED");
        blocked(plan(null, Set.of("mysql"), Map.of("mysql", mysql(25)), verified(20), 2), "JOURNAL_UNKNOWN");
        blocked(plan(new RealmJournal.Inspection(25, 2L, 25L, 25, null), Set.of("mysql"),
                Map.of("mysql", mysql(25)), verified(20), 2), "JOURNAL_RANGE_MISMATCH");
        blocked(plan(new RealmJournal.Inspection(25, 1L, 25L, 20, null), Set.of("mysql"),
                Map.of("mysql", mysql(25)), verified(20), 2), "JOURNAL_PREFIX_UNVERIFIED");
    }

    @Test void aKnownPrunedFloorLimitsOnlyTheRemainingContiguousSuffix() {
        var retained = new RealmJournal.Inspection(10, 4L, 10L, 10, null, 3);
        var caughtUp = plan(retained, Set.of("mysql"), Map.of("mysql", mysql(8)), verified(6), 2);
        assertEquals("CANDIDATE", caughtUp.state());
        assertEquals(4L, caughtUp.firstEligibleSeq());
        assertEquals(6L, caughtUp.lastEligibleSeq());

        // J4 正是此消费者要验证的 SQL 锚点，前面的 J1..J3 已清，不能再次列为候选。
        var onAnchor = plan(retained, Set.of("mysql"), Map.of("mysql", mysql(4)), verified(6), 2);
        assertEquals("NO_CANDIDATES", onAnchor.state());
        assertNull(onAnchor.firstEligibleSeq());
        assertNull(onAnchor.lastEligibleSeq());
        blocked(plan(retained, Set.of("mysql"), Map.of("mysql", mysql(3)), verified(6), 2),
                "CONSUMER_CURSOR_PRUNED:mysql");
        blocked(plan(retained, Set.of("mysql"), Map.of("mysql", mysql(8)), verified(2), 2),
                "SNAPSHOT_BEFORE_PRUNED_PREFIX");
    }

    @Test void aPrunedMarkerCannotHideHolesOrPermitDeletingTheTail() {
        for (var bad : List.of(
                new RealmJournal.Inspection(10, 5L, 10L, 10, null, 3),
                new RealmJournal.Inspection(10, 1L, 10L, 10, null, -1),
                new RealmJournal.Inspection(10, null, null, 10, null, 10),
                new RealmJournal.Inspection(0, null, null, 0, null, 3))) {
            blocked(plan(bad, Set.of("mysql"), Map.of("mysql", mysql(8)), verified(6), 2),
                    "JOURNAL_RANGE_MISMATCH");
        }
        var gap = new RealmJournal.Inspection(10, 4L, 10L, 6, "Missing J7", 3);
        blocked(plan(gap, Set.of("mysql"), Map.of("mysql", mysql(8)), verified(6), 2),
                "JOURNAL_INSPECTION_FAILED", "JOURNAL_PREFIX_UNVERIFIED");
    }

    private static RealmJournal.Inspection history(long high) {
        return new RealmJournal.Inspection(high, high == 0 ? null : 1L, high == 0 ? null : high, high, null);
    }

    private static Consumer mysql(long seq) { return new Consumer(1, VOLUME, seq, "CAUGHT_UP"); }

    private static RecoveryPoint verified(long seq) { return new RecoveryPoint(1, VOLUME, seq, true); }

    private static Result plan(RealmJournal.Inspection history, Set<String> required, Map<String, Consumer> cursors,
                               RecoveryPoint snapshot, long keep) {
        return JournalRetentionPreview.calculate(1, VOLUME, history, required, cursors, snapshot, keep);
    }

    private static void blocked(Result result, String... reasons) {
        assertTrue(result.dryRun());
        assertEquals("BLOCKED", result.state());
        assertNull(result.firstEligibleSeq());
        assertNull(result.lastEligibleSeq());
        assertTrue(result.blockers().containsAll(List.of(reasons)), result.blockers().toString());
    }
}

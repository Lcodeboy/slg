package com.slg.game;

import com.slg.core.auth.SessionClaims;
import com.slg.rpc.v1.SessionBinding;
import com.slg.rpc.v1.SessionOpenRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SessionDirectoryTest {
    private final Clock clock = Clock.fixed(Instant.ofEpochSecond(100), ZoneOffset.UTC);
    private final SessionClaims account = new SessionClaims("101", 99, 1000);

    @Test void competingPreparationAndLateRetryCannotReplaceTheNewOwner() {
        try (var sessions = new SessionDirectory(clock, 8)) {
            var first = request(sessions, account);
            var competitor = request(sessions, account);
            var bound = sessions.open(account, first);
            assertEquals(bound.getKey(), sessions.open(account, first).getKey());
            assertEquals(Status.Code.FAILED_PRECONDITION, status(() -> sessions.open(account, competitor)));
            assertEquals(Status.Code.FAILED_PRECONDITION,
                    status(() -> sessions.open(account, first.toBuilder().setExpectedGeneration(1).build())));
            var replacement = sessions.open(account, request(sessions, account));
            assertTrue(replacement.getKey().getGeneration() > bound.getKey().getGeneration());
            assertEquals(Status.Code.FAILED_PRECONDITION, status(() -> sessions.open(account, first)));
            assertTrue(sessions.isCurrent(account, replacement.getKey()));
            assertFalse(sessions.isCurrent(account, bound.getKey()));
            assertEquals(2L, sessions.metrics().get("opened"));
            sessions.release(replacement.getKey());
            assertEquals(Status.Code.FAILED_PRECONDITION, status(() -> sessions.open(account, first)),
                    "Consumed preparation cannot revive after the replacement closes");
        }
    }

    @Test void oldCloseAndRenewalCannotAffectReplacement() {
        try (var sessions = new SessionDirectory(clock, 8)) {
            var old = sessions.open(account, request(sessions, account));
            var current = sessions.open(account, request(sessions, account));
            sessions.release(old.getKey());
            assertEquals(List.of(current.getKey()), sessions.check(List.of(old.getKey(), current.getKey())));
            assertTrue(sessions.isCurrent(account, current.getKey()));
            sessions.release(current.getKey());
            assertFalse(sessions.isCurrent(account, current.getKey()));
            assertEquals(0, sessions.prepare(account).getObservedGeneration());
        }
    }

    @Test void unconsumedPreparationCannotReviveAfterAnotherBindingOpensAndCloses() {
        try (var sessions = new SessionDirectory(clock, 8)) {
            var delayed = request(sessions, account);
            var newer = request(sessions, account);
            var current = sessions.open(account, newer);
            assertEquals(0, sessions.metrics().get("preparations"), "Successful Open invalidates the other pending ticket");
            sessions.release(current.getKey());
            assertEquals(Status.Code.FAILED_PRECONDITION, status(() -> sessions.open(account, delayed)));
            assertEquals(0, sessions.metrics().get("bindings"));
            assertEquals(1L, sessions.metrics().get("opened"));
        }
    }

    @Test void expiredLeaseCannotBeRevivedAndNewBindingUsesANewGeneration() {
        var nanos = new AtomicLong(1);
        try (var sessions = new SessionDirectory(clock, 1, nanos::get)) {
            var original = sessions.open(account, request(sessions, account));
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(20));
            assertEquals(List.of(original.getKey()), sessions.check(List.of(original.getKey())));
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(29));
            assertTrue(sessions.isCurrent(account, original.getKey()));
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
            assertFalse(sessions.isCurrent(account, original.getKey()));
            assertTrue(sessions.check(List.of(original.getKey())).isEmpty());
            var next = sessions.open(account, request(sessions, account));
            assertTrue(next.getKey().getGeneration() > original.getKey().getGeneration());
            assertNotEquals(next.getKey().getBindingId(), original.getKey().getBindingId());
        }
    }

    @Test void sameNumericGenerationOnANewGameInstanceDoesNotReviveAProof() {
        try (var old = new SessionDirectory(clock, 1); var next = new SessionDirectory(clock, 1)) {
            var before = old.open(account, request(old, account));
            var after = next.open(account, request(next, account));
            assertEquals(before.getKey().getGeneration(), after.getKey().getGeneration());
            assertNotEquals(before.getKey().getGameInstance(), after.getKey().getGameInstance());
            assertFalse(next.isCurrent(account, before.getKey()));
            assertTrue(next.check(List.of(before.getKey())).isEmpty());
            assertEquals(Status.Code.FAILED_PRECONDITION, status(() -> next.open(account,
                    SessionOpenRequest.newBuilder().setGameInstance(before.getKey().getGameInstance())
                            .setTicketId(UUID.randomUUID().toString()).setAttemptId(UUID.randomUUID().toString()).build())));
        }
    }

    @Test void boundedDirectoryAndBatchRejectExcessWithoutChangingOwner() {
        try (var sessions = new SessionDirectory(clock, 1)) {
            SessionBinding first = sessions.open(account, request(sessions, account));
            var second = new SessionClaims("102", 99, 1000);
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, status(() -> sessions.open(second, request(sessions, second))));
            assertEquals(Status.Code.INVALID_ARGUMENT,
                    status(() -> sessions.check(Collections.nCopies(257, first.getKey()))));
            assertEquals(Status.Code.INVALID_ARGUMENT, status(() -> sessions.check(List.of())));
            assertTrue(sessions.isCurrent(account, first.getKey()));
            assertFalse(sessions.isCurrent(second, first.getKey()));
            assertFalse(sessions.isCurrent(new SessionClaims("101", 99, 2000), first.getKey()));
        }
    }

    @Test void preparationExpiresAndIdempotentOpenDoesNotPromiseAFreshLease() {
        var nanos = new AtomicLong(1);
        try (var sessions = new SessionDirectory(clock, 8, nanos::get)) {
            var request = request(sessions, account);
            var original = sessions.open(account, request);
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
            assertEquals(29_000, sessions.open(account, request).getLeaseMillis());
            var late = request(sessions, account);
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(5));
            assertEquals(Status.Code.FAILED_PRECONDITION, status(() -> sessions.open(account, late)));
            assertTrue(sessions.isCurrent(account, original.getKey()));
        }
    }

    @Test void renewalCannotExtendTheUcCredentialLifetime() {
        var seconds = new AtomicLong(100);
        Clock moving = new Clock() {
            public ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(ZoneId zone) { return this; }
            public Instant instant() { return Instant.ofEpochSecond(seconds.get()); }
        };
        var shortCredential = new SessionClaims("101", 99, 101);
        try (var sessions = new SessionDirectory(moving, 8)) {
            var initial = sessions.open(shortCredential, request(sessions, shortCredential));
            seconds.set(101);
            assertTrue(sessions.check(List.of(initial.getKey())).isEmpty());
            assertFalse(sessions.isCurrent(shortCredential, initial.getKey()));
            assertEquals(Status.Code.UNAUTHENTICATED, status(() -> sessions.prepare(shortCredential)));
        }
    }

    static SessionOpenRequest request(SessionDirectory directory, SessionClaims claims) {
        var prepared = directory.prepare(claims);
        return SessionOpenRequest.newBuilder().setGameInstance(prepared.getGameInstance())
                .setExpectedGeneration(prepared.getObservedGeneration()).setTicketId(prepared.getTicketId())
                .setAttemptId(UUID.randomUUID().toString()).build();
    }

    private static Status.Code status(Runnable call) {
        return assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode();
    }
}

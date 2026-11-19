package com.slg.game;

import com.slg.core.auth.SessionClaims;
import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.execution.OperationScope;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.rpc.v1.SessionOpenRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PlayerSessionAdapterTest {
    @Test void immutablePayloadExecutesOnlyWithinItsAuthenticatedAccountAndAttemptScope() throws Exception {
        var clock = new TestClock();
        var claims = new SessionClaims("101", 90, 200);
        try (var sessions = new SessionDirectory(clock, 8)) {
            var request = SessionDirectoryTest.request(sessions, claims);
            byte[] bytes = PlayerSessionAdapter.payload(claims, request);
            var command = command(101, request, bytes);
            java.util.Arrays.fill(bytes, (byte) 0);
            var handler = PlayerSessionAdapter.controlHandlers(sessions, () -> true).get(PlayerSessionAdapter.OPEN);
            var value = handler.execute(command).toCompletableFuture().get();
            assertTrue(value.size() <= handler.maxResultBytes());
            var binding = PlayerSessionAdapter.binding(value);
            assertTrue(sessions.isCurrent(claims, binding.getKey()));
            assertTrue(PlayerSessionAdapter.access(sessions, () -> true).test(PlayerSessionAdapter.access(claims, binding.getKey())));

            var another = SessionDirectoryTest.request(sessions, claims);
            var invalid = command(102, another, PlayerSessionAdapter.payload(claims, another));
            assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class, () -> handler.execute(invalid)).getStatus().getCode());
            assertTrue(sessions.isCurrent(claims, binding.getKey()), "Payload cannot acquire a different account's qualification");
        }
    }

    @Test void queuedControlRechecksReadinessAndTokenExpiryBeforeChangingSession() throws Exception {
        var clock = new TestClock();
        var claims = new SessionClaims("101", 90, 105);
        var ready = new AtomicBoolean(true);
        try (var sessions = new SessionDirectory(clock, 8)) {
            var request = SessionDirectoryTest.request(sessions, claims);
            var command = command(101, request, PlayerSessionAdapter.payload(claims, request));
            var handler = PlayerSessionAdapter.controlHandlers(sessions, ready::get).get(PlayerSessionAdapter.OPEN);
            ready.set(false);
            assertEquals(Status.Code.UNAVAILABLE, assertThrows(StatusRuntimeException.class, () -> handler.execute(command)).getStatus().getCode());
            ready.set(true); clock.seconds = 105;
            assertEquals(Status.Code.UNAUTHENTICATED, assertThrows(StatusRuntimeException.class, () -> handler.execute(command)).getStatus().getCode());
            assertEquals(0L, sessions.metrics().get("opened"));
        }
    }

    @Test void controlInputAndOutputAreBoundedAndMalformedPayloadNeverChangesDirectory() {
        var claims = new SessionClaims("101", 90, 200);
        try (var sessions = new SessionDirectory(new TestClock(), 8)) {
            var request = SessionDirectoryTest.request(sessions, claims);
            assertThrows(StatusRuntimeException.class, () -> PlayerSessionAdapter.payload(claims,
                    request.toBuilder().setTicketId("x".repeat(1000)).build()));
            var handler = PlayerSessionAdapter.controlHandlers(sessions, () -> true).get(PlayerSessionAdapter.OPEN);
            for (byte[] payload : new byte[][] { new byte[0], new byte[1024], new byte[29] })
                assertThrows(StatusRuntimeException.class, () -> handler.execute(command(101, request, payload)));
            assertEquals(0L, sessions.metrics().get("opened"));
        }
    }

    private static OperationCommand command(long account, SessionOpenRequest request, byte[] bytes) {
        return new OperationCommand(new OperationScope(new RequestKey("session:" + account, request.getAttemptId()),
                Set.of(), CommitRequest.Origin.CLIENT, Duration.ofSeconds(1), Duration.ofSeconds(1), Set.of(account)),
                PlayerSessionAdapter.OPEN, bytes);
    }

    private static final class TestClock extends Clock {
        volatile long seconds = 100;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochSecond(seconds); }
    }
}

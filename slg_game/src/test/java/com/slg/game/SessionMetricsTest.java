package com.slg.game;

import com.slg.core.auth.SessionClaims;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

import static com.slg.game.SessionDirectoryTest.request;
import static org.junit.jupiter.api.Assertions.*;

class SessionMetricsTest {
    private final Clock clock = Clock.fixed(Instant.ofEpochSecond(100), ZoneOffset.UTC);

    @Test void jmxTracksBindingReplacementAndReleaseAndIsReadOnly() throws Exception {
        var server = MBeanServerFactory.newMBeanServer();
        var name = new ObjectName("com.slg.game:type=Sessions,realm=9");
        try (var sessions = new SessionDirectory(clock, 8, () -> 1L)) {
            try (var metrics = new SessionMetrics(server, 9, sessions)) {
                assertEquals(0, server.getAttribute(name, "SessionBindings"));
                var first = new SessionClaims("101", 99, 1000);
                var second = new SessionClaims("102", 99, 1000);
                var old = sessions.open(first, request(sessions, first));
                assertEquals(1, server.getAttribute(name, "SessionBindings"));
                var current = sessions.open(first, request(sessions, first));
                assertEquals(1, server.getAttribute(name, "SessionBindings"));
                sessions.release(old.getKey());
                assertEquals(1, server.getAttribute(name, "SessionBindings"));
                var other = sessions.open(second, request(sessions, second));
                assertEquals(2, server.getAttribute(name, "SessionBindings"));
                sessions.release(current.getKey());
                assertEquals(1, server.getAttribute(name, "SessionBindings"));
                sessions.release(other.getKey());
                assertEquals(0, server.getAttribute(name, "SessionBindings"));
                var info = server.getMBeanInfo(name);
                assertEquals(1, info.getAttributes().length);
                assertTrue(info.getAttributes()[0].isReadable());
                assertFalse(info.getAttributes()[0].isWritable());
                assertEquals(0, info.getOperations().length);
                assertThrows(AttributeNotFoundException.class,
                        () -> server.setAttribute(name, new Attribute("SessionBindings", 99)));
            }
            assertFalse(server.isRegistered(name));
        }
    }

    @Test void duplicateRegistrationDoesNotReplaceExistingBeanAndRealmsAreIsolated() throws Exception {
        var server = MBeanServerFactory.newMBeanServer();
        var name = new ObjectName("com.slg.game:type=Sessions,realm=9");
        var otherName = new ObjectName("com.slg.game:type=Sessions,realm=10");
        try (var sessions = new SessionDirectory(clock, 8, () -> 1L);
             var otherSessions = new SessionDirectory(clock, 8, () -> 1L);
             var metrics = new SessionMetrics(server, 9, sessions);
             var otherMetrics = new SessionMetrics(server, 10, otherSessions)) {
            var account = new SessionClaims("101", 99, 1000);
            sessions.open(account, request(sessions, account));
            var failure = assertThrows(IOException.class, () -> new SessionMetrics(server, 9, otherSessions));
            assertInstanceOf(InstanceAlreadyExistsException.class, failure.getCause());
            assertEquals(1, server.getAttribute(name, "SessionBindings"));
            assertEquals(0, server.getAttribute(otherName, "SessionBindings"));
            metrics.close();
            metrics.close();
            assertFalse(server.isRegistered(name));
            assertTrue(server.isRegistered(otherName));
            try (var replacement = new SessionMetrics(server, 9, otherSessions)) {
                assertEquals(0, server.getAttribute(name, "SessionBindings"));
            }
        }
    }
}

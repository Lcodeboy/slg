package com.slg.uc.persistence;

import com.slg.test.MySqlTestDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class JdbcPoolTest {
    @BeforeAll static void prepareMysql() { MySqlTestDatabase.user(); }
    @Test void returningConnectionRollsBackUncommittedWorkAndResetsSessionState() throws Exception {
        try (JdbcPool pool = pool(databaseUrl())) {
            long session;
            try (Connection connection = pool.getConnection()) {
                session = scalar(connection, "SELECT CONNECTION_ID()");
                JdbcSchema.createTable(connection, "CREATE TABLE IF NOT EXISTS pool_probe (id INT PRIMARY KEY)");
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                try (var statement = connection.createStatement()) {
                    statement.executeUpdate("INSERT INTO pool_probe VALUES(1)");
                }
                // A forgotten commit must never be inherited or committed by the next borrower.
            }
            try (Connection reused = pool.getConnection()) {
                assertEquals(session, scalar(reused, "SELECT CONNECTION_ID()"));
                assertTrue(reused.getAutoCommit());
                assertEquals(Connection.TRANSACTION_READ_COMMITTED, reused.getTransactionIsolation());
                assertEquals(0, scalar(reused, "SELECT COUNT(*) FROM pool_probe"));
                try (var statement = reused.createStatement()) { statement.executeUpdate("INSERT INTO pool_probe VALUES(2)"); }
            }
            try (Connection reused = pool.getConnection()) {
                assertEquals(1, scalar(reused, "SELECT COUNT(*) FROM pool_probe"));
            }
        }
    }

    @Test void exhaustedPoolTimesOutAndWorksAgainWhenTheConnectionIsReturned() throws Exception {
        try (JdbcPool pool = pool(databaseUrl())) {
            long session;
            try (Connection held = pool.getConnection()) {
                session = scalar(held, "SELECT CONNECTION_ID()");
                long started = System.nanoTime();
                assertThrows(SQLTransientConnectionException.class, pool::getConnection);
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertTrue(elapsed >= 200, "Acquisition must wait for the configured budget");
                assertTrue(elapsed < 3_000, "Exhaustion must not wait indefinitely");
            }
            try (Connection next = pool.getConnection()) {
                assertEquals(session, scalar(next, "SELECT CONNECTION_ID()"));
            }
        }
    }

    @Test void closeReleasesPhysicalConnectionsAndRejectsFurtherBorrowing() throws Exception {
        String url = databaseUrl();
        try (Connection observer = DriverManager.getConnection(url, MySqlTestDatabase.user(), MySqlTestDatabase.password())) {
            JdbcPool pool = pool(url);
            try {
                assertEquals(2, sessions(observer));
                pool.close();
                pool.close();
                assertTrue(pool.isClosed());
                awaitSessions(observer, 1);
                assertThrows(SQLException.class, pool::getConnection);
            } finally { pool.close(); }
        }
    }

    @Test void badStartupDoesNotExposeConnectionProperties() {
        String marker = "credential-must-not-appear";
        SQLException failure = assertThrows(SQLException.class, () -> JdbcPool.open("invalid", databaseUrl(),
                MySqlTestDatabase.user(), marker, new PoolSettings(1, 250)));
        assertFalse(failure.toString().contains(marker));
        assertNull(failure.getCause());
        assertNull(failure.getNextException());
    }

    @Test void environmentBoundsAndCredentialFallbackAreExplicit() {
        assertEquals(PoolSettings.defaults(), PoolSettings.fromEnvironment(Map.of()));
        assertEquals(new PoolSettings(1, 250), PoolSettings.fromEnvironment(Map.of(
                "SLG_DB_POOL_SIZE", "1", "SLG_DB_CONNECTION_TIMEOUT_MS", "250")));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(0, 500));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(1, 249));
        assertThrows(IllegalArgumentException.class, () -> PoolSettings.fromEnvironment(Map.of("SLG_DB_POOL_SIZE", "4294967297")));
        PoolSettings production = new PoolSettings(12, 5_000, 3, 1_000, 600_000, 60_000);
        assertEquals(production, PoolSettings.fromEnvironment(Map.of("SLG_DB_POOL_SIZE", "12",
                "SLG_DB_CONNECTION_TIMEOUT_MS", "5000", "SLG_DB_MINIMUM_IDLE", "3",
                "SLG_DB_VALIDATION_TIMEOUT_MS", "1000", "SLG_DB_MAX_LIFETIME_MS", "600000", "SLG_DB_KEEPALIVE_MS", "60000")));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(2, 5_000, 3, 1_000, 600_000, 60_000));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(2, 500, 1, 1_000, 600_000, 60_000));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(2, 5_000, 1, 1_000, 29_999, 0));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(2, 5_000, 1, 1_000, 60_000, 60_000));
        assertThrows(IllegalArgumentException.class, () -> new PoolSettings(2, 5_000, 1, 1_000, 60_000, 20_000));
        assertEquals(0, new PoolSettings(2, 5_000, 0, 1_000, 60_000, 0).keepaliveMillis());
    }

    private static JdbcPool pool(String url) throws SQLException {
        return JdbcPool.open("test", url, MySqlTestDatabase.user(), MySqlTestDatabase.password(), new PoolSettings(1, 250));
    }
    private static String databaseUrl() {
        return MySqlTestDatabase.createUrl();
    }
    private static long sessions(Connection connection) throws SQLException {
        return scalar(connection, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=DATABASE()");
    }
    private static void awaitSessions(Connection connection, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (sessions(connection) != expected && System.nanoTime() < deadline) Thread.sleep(10);
        assertEquals(expected, sessions(connection));
    }
    private static long scalar(Connection connection, String sql) throws SQLException {
        try (var query = connection.createStatement(); var result = query.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }
}

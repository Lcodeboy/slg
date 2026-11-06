package com.slg.uc;

import com.slg.core.auth.TokenService;
import com.slg.uc.persistence.PoolSettings;
import com.slg.test.MySqlTestDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class AccountRepositoryPoolTest {
    @BeforeAll static void prepareMysql() { MySqlTestDatabase.user(); }
    @Test void constraintFailureReturnsConnectionAndCloseReleasesIt() throws Exception {
        String url = url();
        try (Connection observer = DriverManager.getConnection(url, MySqlTestDatabase.user(), MySqlTestDatabase.password())) {
            long accountId;
            try (AccountRepository accounts = repository(url)) {
                accountId = accounts.create("pool_user", "test-password");
                assertThrows(SQLException.class, () -> accounts.create("pool_user", "test-password"));
                assertEquals(accountId, accounts.find("pool_user").orElseThrow().accountId());
                assertEquals(2, sessions(observer));
                accounts.close();
                accounts.close();
                awaitSessions(observer, 1);
                assertThrows(SQLException.class, () -> accounts.find("pool_user"));
            }
            try (AccountRepository reopened = repository(url)) {
                assertEquals(accountId, reopened.find("pool_user").orElseThrow().accountId());
            }
            awaitSessions(observer, 1);
        }
    }

    @Test void failedServerBindingClosesTheAccountPool() throws Exception {
        String url = url();
        try (Connection observer = DriverManager.getConnection(url, MySqlTestDatabase.user(), MySqlTestDatabase.password());
             ServerSocket occupied = new ServerSocket(); AccountRepository accounts = repository(url)) {
            occupied.bind(new InetSocketAddress("127.0.0.1", 0));
            var tokenService = new TokenService("uc-pool-test-secret-at-least-32-bytes", Clock.systemUTC());
            assertThrows(java.io.IOException.class, () -> new UcServer(
                    new InetSocketAddress("127.0.0.1", occupied.getLocalPort()), new InetSocketAddress("127.0.0.1", 0),
                    accounts, tokenService, UcServer.Settings.defaults()));
            awaitSessions(observer, 1);
            assertThrows(SQLException.class, () -> accounts.find("pool_user"));
        }
    }

    private static AccountRepository repository(String url) throws SQLException {
        return new AccountRepository(url, MySqlTestDatabase.user(), MySqlTestDatabase.password(), Clock.systemUTC(), new PoolSettings(1, 500));
    }
    private static String url() {
        return MySqlTestDatabase.createUrl();
    }
    private static long sessions(Connection connection) throws SQLException {
        try (var query = connection.createStatement(); var result = query.executeQuery("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB=DATABASE()")) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }
    private static void awaitSessions(Connection connection, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (sessions(connection) != expected && System.nanoTime() < deadline) Thread.sleep(10);
        assertEquals(expected, sessions(connection));
    }
}

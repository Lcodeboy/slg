package com.slg.uc;

import com.slg.core.auth.TokenService;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.AuthRequest;
import com.slg.rpc.v1.AccountRpcGrpc;
import com.slg.test.MySqlTestDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 使用独立 MySQL schema 验证账号重启持久化和表存储配置。 */
@Timeout(45)
class UcMySqlIT {
    @BeforeAll static void prepareMysql() { MySqlTestDatabase.user(); }
    @Test
    void mysqlAccountsAuthenticateAfterServerRestartAndUseTransactionalUtf8Tables() throws Exception {
        String url = MySqlTestDatabase.createUrl();
        String user = MySqlTestDatabase.user(), password = MySqlTestDatabase.password();
        Clock clock = Clock.systemUTC();
        TokenService tokens = new TokenService("mysql-integration-only-secret-at-least-32-bytes", clock);
        String username = "mysql_" + UUID.randomUUID().toString().substring(0, 12).replace("-", "");
        String accountId;
        try (UcServer server = server(url, user, password, clock, tokens)) {
            // 选用 JavaScript 首个无法精确表示的正整数，覆盖数据库、Protobuf 会话、签名与登录的完整往返。
            try (var connection = DriverManager.getConnection(url, user, password); var ddl = connection.createStatement()) {
                ddl.execute("ALTER TABLE uc_account AUTO_INCREMENT=9007199254740993");
            }
            server.start();
            var channel = RpcTransport.channel("127.0.0.1:" + server.rpcPort());
            try {
                var stub = AccountRpcGrpc.newBlockingStub(channel);
                var created = stub.withDeadlineAfter(5, TimeUnit.SECONDS).register(auth(username.toUpperCase(), "test-password-123"));
                assertEquals(201, created.getStatus());
                assertTrue(created.hasSession());
                var session = created.getSession();
                accountId = session.getAccountId();
                assertEquals("9007199254740993", accountId);
                assertEquals(accountId, tokens.verify(session.getToken()).accountId());
                assertEquals(409, stub.withDeadlineAfter(5, TimeUnit.SECONDS).register(auth(username, "another-password")).getStatus());
                assertEquals(401, stub.withDeadlineAfter(5, TimeUnit.SECONDS).login(auth(username, "incorrect-password")).getStatus());
            } finally {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
        try (var connection = DriverManager.getConnection(url, user, password)) {
            assertEquals("MySQL", connection.getMetaData().getDatabaseProductName());
            try (var statement = connection.prepareStatement("SELECT password_salt,password_hash,iterations FROM uc_account WHERE account_id=?")) {
                statement.setLong(1, Long.parseLong(accountId));
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals(24, rows.getBytes(1).length);
                    assertArrayEquals(PasswordHasher.hash("test-password-123", rows.getBytes(1), rows.getInt(3)), rows.getBytes(2));
                }
            }
            try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                    "SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='uc_account'")) {
                assertTrue(rows.next());
                assertEquals("InnoDB", rows.getString(1));
                assertEquals("utf8mb4_0900_bin", rows.getString(2));
            }
        }
        try (UcServer restarted = server(url, user, password, clock, tokens)) {
            restarted.start();
            var channel = RpcTransport.channel("127.0.0.1:" + restarted.rpcPort());
            try {
                var login = AccountRpcGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS).login(auth(username, "test-password-123"));
                assertEquals(200, login.getStatus());
                assertTrue(login.hasSession());
                assertEquals(accountId, login.getSession().getAccountId());
            } finally {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void concurrentRegistrationsAcrossTwoUcPoolsReturnEachUsersOwnGeneratedKey() throws Exception {
        String url = MySqlTestDatabase.createUrl();
        String user = MySqlTestDatabase.user(), password = MySqlTestDatabase.password();
        try (var first = new AccountRepository(url, user, password, Clock.systemUTC());
             var second = new AccountRepository(url, user, password, Clock.systemUTC());
             var workers = Executors.newFixedThreadPool(6)) {
            var start = new CountDownLatch(1);
            var registrations = new ArrayList<Future<Long>>();
            for (int i = 0; i < 24; i++) {
                var repo = i % 2 == 0 ? first : second;
                String username = "parallel_" + i;
                registrations.add(workers.submit(() -> {
                    start.await();
                    return repo.create(username, "test-password");
                }));
            }
            start.countDown();
            var ids = new HashSet<Long>();
            for (int i = 0; i < registrations.size(); i++) {
                long assigned = registrations.get(i).get(10, TimeUnit.SECONDS);
                assertTrue(assigned > 0 && ids.add(assigned));
                assertEquals(assigned, second.find("parallel_" + i).orElseThrow().accountId());
            }
        }
    }

    @Test
    void legacyUuidTableStopsUcStartupAndPreservesExistingAccounts() throws Exception {
        String url = MySqlTestDatabase.createUrl();
        String user = MySqlTestDatabase.user(), password = MySqlTestDatabase.password();
        String legacyId = "28691492-4d56-47dd-8cdb-e77c3b906573";
        try (var connection = DriverManager.getConnection(url, user, password); var sql = connection.createStatement()) {
            sql.execute("""
                    CREATE TABLE uc_account (
                        account_id VARCHAR(64) PRIMARY KEY, username VARCHAR(32) NOT NULL UNIQUE,
                        password_salt VARBINARY(32) NOT NULL, password_hash VARBINARY(64) NOT NULL,
                        iterations INTEGER NOT NULL, created_at BIGINT NOT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin
                    """);
            try (var insert = connection.prepareStatement("INSERT INTO uc_account VALUES(?,'legacy',X'01',X'02',1,123)")) {
                insert.setString(1, legacyId);
                insert.executeUpdate();
            }
            var failure = assertThrows(SQLException.class, () -> new AccountRepository(url, user, password, Clock.systemUTC()));
            assertEquals("0A000", failure.getSQLState());
            try (var rows = sql.executeQuery("SELECT account_id FROM uc_account WHERE username='legacy'")) {
                assertTrue(rows.next());
                assertEquals(legacyId, rows.getString(1));
            }
        }
    }

    private static UcServer server(String url, String user, String password, Clock clock, TokenService tokens) throws Exception {
        return new UcServer(new InetSocketAddress("127.0.0.1", 0), new InetSocketAddress("127.0.0.1", 0),
                new AccountRepository(url, user, password, clock), tokens, UcServer.Settings.defaults());
    }

    private static AuthRequest auth(String username, String password) {
        return AuthRequest.newBuilder().setUsername(username).setPassword(password).build();
    }

}

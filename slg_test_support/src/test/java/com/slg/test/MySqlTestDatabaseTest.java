package com.slg.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.sql.DriverManager;
import java.sql.SQLException;
import static org.junit.jupiter.api.Assertions.*;

/** 验证真实 MySQL schema 隔离及清理范围，不允许无数据库时跳过。 */
@Timeout(30)
class MySqlTestDatabaseTest {
    @BeforeAll static void prepareMysql() { MySqlTestDatabase.user(); }

    @Test void schemasAreIndependentAndOnlyTrackedSchemasCanBeRemoved() throws Exception {
        String first = MySqlTestDatabase.createUrl();
        String second = MySqlTestDatabase.createUrl();
        assertNotEquals(first, second);
        try (var a = DriverManager.getConnection(first, MySqlTestDatabase.user(), MySqlTestDatabase.password());
             var b = DriverManager.getConnection(second, MySqlTestDatabase.user(), MySqlTestDatabase.password())) {
            assertEquals("MySQL", a.getMetaData().getDatabaseProductName());
            try (var statement = a.createStatement()) {
                statement.executeUpdate("CREATE TABLE isolation_probe(id INT PRIMARY KEY) ENGINE=InnoDB");
                statement.executeUpdate("INSERT INTO isolation_probe VALUES(1)");
            }
            try (var statement = b.createStatement()) {
                assertThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM isolation_probe"));
            }
            assertThrows(IllegalArgumentException.class, () -> MySqlTestDatabase.dropUrl("jdbc:mysql://localhost/slg_game"));
        }
        MySqlTestDatabase.dropUrl(first);
        assertThrows(SQLException.class, () -> DriverManager.getConnection(first, MySqlTestDatabase.user(), MySqlTestDatabase.password()));
        try (var remaining = DriverManager.getConnection(second, MySqlTestDatabase.user(), MySqlTestDatabase.password())) {
            assertTrue(remaining.isValid(1));
        }
        MySqlTestDatabase.dropUrl(second);
        assertThrows(IllegalArgumentException.class, () -> MySqlTestDatabase.dropUrl(second));
    }
}

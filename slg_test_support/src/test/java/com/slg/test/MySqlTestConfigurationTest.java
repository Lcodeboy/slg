package com.slg.test;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/** 配置失败必须显式报错，不能悄悄使用开发库或其他后端。 */
class MySqlTestConfigurationTest {
    @Test void ignoresApplicationDatabaseCredentialsAndRequiresCompleteTestConfiguration() {
        assertNull(MySqlTestDatabase.configuration(Map.of("SLG_UC_JDBC_URL", "jdbc:mysql://production/uc",
                "SLG_DB_USER", "production-user", "SLG_DB_PASSWORD", "must-not-be-exposed")));
        for (Map<String, String> partial : java.util.List.of(
                Map.of("SLG_MYSQL_IT_UC_URL", "jdbc:mysql://isolated/uc"),
                Map.of("SLG_MYSQL_IT_PASSWORD", "must-not-be-exposed"),
                Map.of("SLG_MYSQL_IT_UC_URL", "jdbc:mysql://isolated/uc", "SLG_MYSQL_IT_USER", "root", "SLG_MYSQL_IT_PASSWORD", ""))) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> MySqlTestDatabase.configuration(partial));
            assertFalse(failure.toString().contains("must-not-be-exposed"));
            assertNull(failure.getCause());
        }
    }

    @Test void changesOnlySchemaAndNeverPrintsCredentials() {
        String original = "jdbc:mysql://127.0.0.1:13399/slg_uc_it?sslMode=DISABLED&socketTimeout=10000";
        var configured = MySqlTestDatabase.configuration(Map.of("SLG_MYSQL_IT_UC_URL", original,
                "SLG_MYSQL_IT_USER", "root", "SLG_MYSQL_IT_PASSWORD", "must-not-be-exposed"));
        assertEquals(original, configured.url());
        assertEquals("root", configured.user());
        assertFalse(configured.toString().contains("must-not-be-exposed"));
        assertEquals("jdbc:mysql://127.0.0.1:13399/slg_test_123?sslMode=DISABLED&socketTimeout=10000",
                MySqlTestDatabase.replaceSchema(original, "slg_test_123"));
        for (String invalid : java.util.List.of("jdbc:h2:mem:test", "jdbc:mysql://user:must-not-be-exposed@localhost/test", "jdbc:mysql://localhost/")) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> MySqlTestDatabase.replaceSchema(invalid, "slg_test_123"));
            assertFalse(failure.toString().contains("must-not-be-exposed"));
        }
        assertThrows(IllegalStateException.class, () -> MySqlTestDatabase.replaceSchema(original, "slg_game"));
    }
}

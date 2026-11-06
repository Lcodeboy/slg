package com.slg.uc.persistence;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

/** 无需打开数据库，验证 UC 凭证优先级与错误信息。 */
class DatabaseEnvironmentTest {
    @Test void defaultsTargetTheLocalAccountDatabase() {
        assertTrue(DatabaseEnvironment.jdbcUrl(Map.of()).startsWith("jdbc:mysql://127.0.0.1:13306/slg_uc?"));
        assertEquals("slg_uc", DatabaseEnvironment.user(Map.of()));
    }

    @Test void accountCredentialsOverrideSharedCredentialsWithoutTrimmingPasswords() {
        var shared = Map.of("SLG_DB_USER", "shared", "SLG_DB_PASSWORD", "shared-secret");
        assertEquals("shared", DatabaseEnvironment.user(shared));
        assertEquals("shared-secret", DatabaseEnvironment.password(shared));
        var specific = Map.of("SLG_DB_PASSWORD", "fallback", "SLG_UC_DB_PASSWORD", " uc-secret ");
        assertEquals(" uc-secret ", DatabaseEnvironment.password(specific));
        assertThrows(IllegalArgumentException.class, () -> DatabaseEnvironment.password(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> DatabaseEnvironment.user(Map.of("SLG_DB_USER", " ")));
        assertThrows(IllegalArgumentException.class, () -> DatabaseEnvironment.password(Map.of(
                "SLG_UC_DB_PASSWORD", "", "SLG_DB_PASSWORD", "fallback")));
    }

    @Test void explicitUrlIsPreservedAndOtherEnginesAreRejectedWithoutEchoingCredentials() {
        String url = "jdbc:mysql://db.internal:3306/accounts?sslMode=VERIFY_IDENTITY";
        assertEquals(url, DatabaseEnvironment.jdbcUrl(Map.of("SLG_UC_JDBC_URL", url)));
        for (String invalid : new String[]{"jdbc:h2:mem:hidden", "jdbc:postgresql://hidden/test", ""}) {
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> DatabaseEnvironment.jdbcUrl(Map.of("SLG_UC_JDBC_URL", invalid)));
            assertFalse(failure.getMessage().contains("hidden"));
        }
    }
}

package com.slg.game.infrastructure.mysql;

import com.slg.core.economy.ResourceType;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.type.IntCodeTypeHandler;
import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MySqlSessionsTest {
    @Test void sessionPolicyIsExplicitAndConstructionDoesNotInitializeDatabase() {
        DataSource noConnection = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{DataSource.class},
                (proxy, method, args) -> { throw new AssertionError("Construction must not access the database"); });
        try (var session = new MySqlSessions(noConnection, 7).openRead()) {
            var configuration = session.getConfiguration();
            assertFalse(configuration.isCacheEnabled());
            assertFalse(configuration.isLazyLoadingEnabled());
            assertEquals(LocalCacheScope.STATEMENT, configuration.getLocalCacheScope());
            assertEquals(ExecutorType.SIMPLE, configuration.getDefaultExecutorType());
            assertEquals(7, configuration.getDefaultStatementTimeout());
            assertInstanceOf(IntCodeTypeHandler.class, configuration.getTypeHandlerRegistry().getTypeHandler(ResourceType.class));
        }
    }
}

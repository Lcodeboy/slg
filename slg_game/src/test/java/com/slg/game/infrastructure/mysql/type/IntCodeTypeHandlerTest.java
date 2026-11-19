package com.slg.game.infrastructure.mysql.type;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.core.type.IntCodeEnum;
import com.slg.game.domain.persistence.BattleState;
import com.slg.game.domain.persistence.MarchState;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.TaskState;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntCodeTypeHandlerTest {
    @Test void allEightEnumsRegisterExplicitIntegerHandlersAndRoundTripEveryDefinedCode() throws Exception {
        var configuration = new Configuration(); IntCodeTypeHandler.register(configuration);
        verify(configuration, ResourceType.class); verify(configuration, TroopType.class); verify(configuration, ObjectKind.class);
        verify(configuration, MarchState.Status.class); verify(configuration, MarchState.Mission.class);
        verify(configuration, TaskState.Type.class); verify(configuration, TaskState.Status.class); verify(configuration, BattleState.Status.class);
    }

    @Test void nullWritesAndSqlNullReadsFailBeforeBecomingAnAsset() {
        var handler = new IntCodeTypeHandler<>(ResourceType::fromCode);
        assertThrows(SQLDataException.class, () -> handler.setParameter(statement(new AtomicInteger()), 1, null, JdbcType.INTEGER));
        assertThrows(SQLDataException.class, () -> handler.getNullableResult(results(0, true), "type"));
        assertThrows(SQLDataException.class, () -> handler.getNullableResult(results(0, true), 1));
        assertThrows(SQLDataException.class, () -> handler.getNullableResult(callable(0, true), 1));
    }

    @Test void zeroUnknownAndNegativeDatabaseCodesNeverSelectOrdinalOrDefault() {
        var handler = new IntCodeTypeHandler<>(ResourceType::fromCode);
        for (int invalid : new int[]{0, -1, 99, Integer.MAX_VALUE})
            assertThrows(SQLDataException.class, () -> handler.getNullableResult(results(invalid, false), "type"));
    }

    private static <E extends Enum<E> & IntCodeEnum> void verify(Configuration configuration, Class<E> type) throws Exception {
        TypeHandler<E> handler = configuration.getTypeHandlerRegistry().getTypeHandler(type);
        assertInstanceOf(IntCodeTypeHandler.class, handler);
        for (E value : type.getEnumConstants()) {
            var written = new AtomicInteger(); handler.setParameter(statement(written), 1, value, JdbcType.INTEGER);
            assertEquals(value.code(), written.get());
            assertSame(value, handler.getResult(results(written.get(), false), "type"));
            assertSame(value, handler.getResult(results(written.get(), false), 1));
            assertSame(value, handler.getResult(callable(written.get(), false), 1));
        }
    }
    private static PreparedStatement statement(AtomicInteger code) {
        return (PreparedStatement) Proxy.newProxyInstance(IntCodeTypeHandlerTest.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("setInt")) { code.set((Integer) args[1]); return null; }
                    throw new AssertionError("Unexpected JDBC call: " + method.getName());
                });
    }
    private static ResultSet results(int code, boolean wasNull) { return jdbc(ResultSet.class, code, wasNull); }
    private static CallableStatement callable(int code, boolean wasNull) { return jdbc(CallableStatement.class, code, wasNull); }
    private static <T> T jdbc(Class<T> type, int code, boolean wasNull) {
        return type.cast(Proxy.newProxyInstance(IntCodeTypeHandlerTest.class.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> switch (method.getName()) {
            case "getInt" -> code;
            case "wasNull" -> wasNull;
            default -> throw new AssertionError("Unexpected JDBC call: " + method.getName());
        }));
    }
}

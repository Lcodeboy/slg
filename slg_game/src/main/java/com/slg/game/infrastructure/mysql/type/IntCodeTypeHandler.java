package com.slg.game.infrastructure.mysql.type;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.core.type.IntCodeEnum;
import com.slg.game.domain.persistence.BattleState;
import com.slg.game.domain.persistence.MarchState;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.TaskState;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLDataException;
import java.util.Objects;
import java.util.function.IntFunction;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

/**
 * 必填枚举的 JDBC 整数边界。NULL 与未知 code 都使整行失败，不让 getInt 的缺省零、
 * MyBatis 默认名称处理器或 ordinal 变成一份看似有效的资产。这里只转换，不开启事务。
 */
public final class IntCodeTypeHandler<E extends Enum<E> & IntCodeEnum> extends BaseTypeHandler<E> {
    private final IntFunction<E> decode;

    public IntCodeTypeHandler(IntFunction<E> decode) { this.decode = Objects.requireNonNull(decode, "decode"); }

    @Override public void setParameter(PreparedStatement statement, int index, E value, JdbcType jdbcType) throws SQLException {
        if (value == null) throw new SQLDataException("Required enum parameter is null", "22004");
        super.setParameter(statement, index, value, jdbcType);
    }
    @Override public void setNonNullParameter(PreparedStatement statement, int index, E value, JdbcType jdbcType) throws SQLException {
        if (value == null) throw new SQLDataException("Required enum parameter is null", "22004");
        statement.setInt(index, value.code());
    }
    @Override public E getNullableResult(ResultSet results, String column) throws SQLException {
        int code = results.getInt(column); return value(code, results.wasNull());
    }
    @Override public E getNullableResult(ResultSet results, int column) throws SQLException {
        int code = results.getInt(column); return value(code, results.wasNull());
    }
    @Override public E getNullableResult(CallableStatement statement, int column) throws SQLException {
        int code = statement.getInt(column); return value(code, statement.wasNull());
    }
    private E value(int code, boolean wasNull) throws SQLException {
        if (wasNull) throw new SQLDataException("Required enum column is SQL NULL", "22004");
        try { return Objects.requireNonNull(decode.apply(code), "decoded enum"); }
        catch (RuntimeException invalid) { throw new SQLDataException("Unknown persisted enum code", "22000", invalid); }
    }

    /** 所有已定义持久化枚举显式登记；新增类型须同时补登记和真实数据库往返测试。 */
    public static void register(Configuration configuration) {
        register(configuration, ResourceType.class, ResourceType::fromCode);
        register(configuration, TroopType.class, TroopType::fromCode);
        register(configuration, ObjectKind.class, ObjectKind::fromCode);
        register(configuration, MarchState.Status.class, MarchState.Status::fromCode);
        register(configuration, MarchState.Mission.class, MarchState.Mission::fromCode);
        register(configuration, TaskState.Type.class, TaskState.Type::fromCode);
        register(configuration, TaskState.Status.class, TaskState.Status::fromCode);
        register(configuration, BattleState.Status.class, BattleState.Status::fromCode);
    }
    private static <E extends Enum<E> & IntCodeEnum> void register(Configuration config, Class<E> type, IntFunction<E> decode) {
        config.getTypeHandlerRegistry().register(type, new IntCodeTypeHandler<>(decode));
    }
}

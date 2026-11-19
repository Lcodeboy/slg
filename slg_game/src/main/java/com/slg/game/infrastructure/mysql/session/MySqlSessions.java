package com.slg.game.infrastructure.mysql.session;

import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.infrastructure.mysql.mapper.JournalMapper;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.mapper.StateMapper;
import com.slg.game.infrastructure.mysql.type.IntCodeTypeHandler;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

/**
 * 手动组合的 MyBatis Session 工厂。DataSource 的生命周期属于外部装配；本类既不建表、
 * 不获得写权，也不启动线程。一个端口调用/快照独占一个 Session，禁止跨异步回调共享。
 */
public final class MySqlSessions {
    public static final int SCHEMA_VERSION = 1;
    private final SqlSessionFactory factory;

    public MySqlSessions(DataSource dataSource, int statementTimeoutSeconds) {
        Objects.requireNonNull(dataSource, "dataSource");
        if (statementTimeoutSeconds <= 0) throw new IllegalArgumentException("Statement timeout must be positive");
        var configuration = new Configuration(new Environment("mysql-authority", new JdbcTransactionFactory(), dataSource));
        configuration.setCacheEnabled(false);
        configuration.setLocalCacheScope(LocalCacheScope.STATEMENT);
        configuration.setLazyLoadingEnabled(false);
        configuration.setDefaultExecutorType(ExecutorType.SIMPLE);
        configuration.setDefaultStatementTimeout(statementTimeoutSeconds);
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.setArgNameBasedConstructorAutoMapping(true);
        IntCodeTypeHandler.register(configuration);
        configuration.addMapper(RealmMetadataMapper.class);
        configuration.addMapper(StateMapper.class);
        configuration.addMapper(JournalMapper.class);
        factory = new SqlSessionFactoryBuilder().build(configuration);
    }

    /** RR 普通一致读在首条读取建立视图；H、根及明细必须继续使用这一 Session。 */
    public SqlSession openRead() { return open(); }
    public SqlSession openWrite() { return open(); }

    private SqlSession open() {
        // SIMPLE 保证 CAS、创建占位和锁定均立即执行；不允许延迟到 cursor 之后才 flush。
        return factory.openSession(ExecutorType.SIMPLE, TransactionIsolationLevel.REPEATABLE_READ);
    }

    /** 正常访问只核验部署已初始化的身份与格式；缺库/错库不得退回新建空库。 */
    public void validateIdentity(SqlSession session, RealmIdentity identity) throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        try {
            var row = session.getMapper(RealmMetadataMapper.class).identity(identity.realmId());
            if (row == null)
                throw new StorageAccessException(StorageAccessException.Code.IDENTITY_MISMATCH, "Authority realm is not initialized");
            if (row.schemaVersion() != SCHEMA_VERSION)
                throw new StorageAccessException(StorageAccessException.Code.UNSUPPORTED_SCHEMA, "Unsupported authority schema version");
            if (row.realmId() != identity.realmId() || !identity.datasetId().equals(UUID.fromString(row.datasetId())))
                throw new StorageAccessException(StorageAccessException.Code.IDENTITY_MISMATCH, "Authority dataset identity mismatch");
        } catch (IllegalArgumentException invalid) {
            throw new StorageAccessException(StorageAccessException.Code.INVALID_DATA, "Invalid authority identity");
        } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }
}

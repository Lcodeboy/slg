package com.slg.game.infrastructure.mysql;

import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.test.MySqlTestDatabase;
import com.zaxxer.hikari.HikariDataSource;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** 只在独立随机测试库部署发布 DDL；测试结束只删除 MySqlTestDatabase 登记的本次库。 */
public abstract class AuthoritySchemaFixture {
    protected HikariDataSource pool;
    protected MySqlSessions sessions;
    protected RealmIdentity identity;
    protected String url;

    @BeforeEach protected void setupAuthority() throws Exception {
        url = MySqlTestDatabase.createUrl();
        pool = new HikariDataSource();
        pool.setJdbcUrl(url);
        pool.setUsername(MySqlTestDatabase.user());
        pool.setPassword(MySqlTestDatabase.password());
        pool.setMaximumPoolSize(8);
        pool.setMinimumIdle(0);
        pool.setConnectionTimeout(5_000);
        identity = new RealmIdentity(1, UUID.randomUUID());
        try (Connection connection = connection();
             var reader = new InputStreamReader(Objects.requireNonNull(
                     getClass().getResourceAsStream("/mysql/authority/001-schema.sql"), "Authority schema resource missing"),
                     StandardCharsets.UTF_8)) {
            var runner = new ScriptRunner(connection);
            runner.setLogWriter(null);
            runner.setErrorLogWriter(null);
            runner.setStopOnError(true);
            runner.runScript(reader);
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement(
                    "INSERT INTO realm_identity(realm_id,dataset_id,schema_version) VALUES (?,UUID_TO_BIN(?),1)")) {
                statement.setInt(1, identity.realmId());
                statement.setString(2, identity.datasetId().toString());
                statement.executeUpdate();
            }
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO realm_owner VALUES (1,0,NULL,0)");
                statement.executeUpdate("INSERT INTO realm_owner_lease VALUES (1,0,NULL,0)");
                statement.executeUpdate("INSERT INTO realm_commit_cursor VALUES (1,0,0)");
            }
            connection.commit();
        }
        sessions = new MySqlSessions(pool, 5);
    }

    @AfterEach protected void closeAuthority() {
        if (pool != null) pool.close();
        if (url != null) MySqlTestDatabase.dropUrl(url);
    }

    protected Connection connection() throws SQLException { return pool.getConnection(); }

    /** 仅用于测试造数/故障注入，生产写入只能经事务端口。 */
    protected int execute(String sql, Object... values) throws SQLException {
        try (Connection connection = connection(); var statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) statement.setObject(i + 1, values[i]);
            return statement.executeUpdate();
        }
    }

    protected long scalar(String sql) throws SQLException {
        try (Connection connection = connection(); var statement = connection.createStatement();
             var rows = statement.executeQuery(sql)) {
            if (!rows.next()) throw new SQLException("Expected a scalar test result");
            return rows.getLong(1);
        }
    }
}

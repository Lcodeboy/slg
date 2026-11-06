package com.slg.game.projection;

import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Entry;
import com.slg.test.MySqlTestDatabase;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

/** 每个用例独占一个随机 schema；先建 001，调用者决定是在写入历史之前还是之后安装 003。 */
abstract class ProjectionSchemaFixture {
    static final String TYPED_MIGRATION = "003-role-identity-projections.sql";
    final UUID volume = UUID.randomUUID();
    String url;
    HikariDataSource pool;
    MySqlProjectionStore store;

    @BeforeEach void createFreshSchema() throws Exception {
        url = MySqlTestDatabase.createUrl();
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(MySqlTestDatabase.user());
        config.setPassword(MySqlTestDatabase.password());
        config.setMaximumPoolSize(3);
        pool = new HikariDataSource(config);
        migrate("001-realm-projection.sql");
        store = new MySqlProjectionStore(pool, 1, volume);
    }

    @AfterEach void closeSchema() {
        if (pool != null) pool.close();
        if (url != null) MySqlTestDatabase.dropUrl(url);
    }

    /** 使用部署文件原文；DELIMITER 是 mysql 客户端指令，JDBC 必须把存储过程或触发器整体发送。 */
    void migrate(String file) throws Exception {
        migrate(file, sql -> {});
    }

    /** 允许测试在两条隐式提交的 DDL 之间插入一次真实业务事务。 */
    @FunctionalInterface interface AfterStatement { void run(String sql) throws SQLException; }

    void migrate(String file, AfterStatement after) throws Exception {
        try (var source = getClass().getResourceAsStream("/mysql/schema/" + file);
             var conn = pool.getConnection(); var statement = conn.createStatement()) {
            assertNotNull(source, "Missing deployment migration " + file);
            var pending = new StringBuilder();
            String delimiter = ";";
            for (String line : new String(source.readAllBytes(), StandardCharsets.UTF_8).lines().toList()) {
                String trimmed = line.strip();
                if (trimmed.isEmpty() || trimmed.startsWith("--")) continue;
                if (trimmed.startsWith("DELIMITER ")) {
                    assertTrue(pending.isEmpty(), "Incomplete SQL before DELIMITER in " + file);
                    delimiter = trimmed.substring("DELIMITER ".length()).strip();
                } else if (trimmed.endsWith(delimiter)) {
                    pending.append(trimmed, 0, trimmed.length() - delimiter.length());
                    String sql = pending.toString();
                    statement.execute(sql);
                    after.run(sql);
                    pending.setLength(0);
                } else {
                    pending.append(line).append('\n');
                }
            }
            assertTrue(pending.isEmpty(), "Incomplete SQL at the end of " + file);
        }
    }

    static Entry entry(long seq, String scope, String id, String type, Change... changes) {
        return new Entry(1, seq, scope, id, "digest-" + id, type, "{\"seq\":" + seq + "}",
                "{\"id\":\"" + id + "\"}", List.of(changes));
    }

    static Change change(String key, long expected, int amount) {
        if (key.startsWith("player:")) {
            String id = key.substring("player:".length());
            String[] identity = id.split("_", 2);
            int founding = Integer.parseInt(identity[0]);
            var state = new java.util.LinkedHashMap<String,Object>();
            state.put("playerId", id);
            state.put("accountId", identity[1]);
            state.put("foundingRealmId", founding);
            state.put("currentRealmId", founding);
            state.put("name", "Commander " + id);
            state.put("troops", Map.of("INFANTRY", 3000));
            state.put("heroes", List.of());
            state.put("activeMarchIds", List.of());
            state.put("resources", Map.of("WOOD", amount));
            state.put("wounded", Map.of());
            state.put("dead", Map.of());
            return new Change(key, expected, Json.write(state));
        }
        return new Change(key, expected, "{\"amount\":" + amount + "}");
    }

    long number(String sql) throws SQLException {
        try (var conn = pool.getConnection(); var stmt = conn.createStatement(); var rows = stmt.executeQuery(sql)) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }
}

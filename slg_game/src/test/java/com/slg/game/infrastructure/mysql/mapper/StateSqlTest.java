package com.slg.game.infrastructure.mysql.mapper;

import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.infrastructure.mysql.row.StateRows;
import com.slg.game.infrastructure.mysql.type.IntCodeTypeHandler;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StateSqlTest {
    private final Configuration configuration = configuration();

    @Test void allRootQueriesUseTheirExplicitAuthorityTableAndRealmPrefix() {
        Map.of("players", "game_player", "resources", "game_player_resource", "troops", "game_player_troop",
                "heroes", "game_player_hero", "marches", "game_march", "gatherSites", "game_gather_site",
                "tasks", "game_due_task", "battles", "game_battle").forEach((method, table) -> {
            var sql = sql(method, Map.of("q", StateRows.Query.scan(7, null, null, false, 25)));
            assertTrue(sql.contains(" FROM " + table + " WHERE realm_id=?"));
            assertTrue(sql.endsWith(" LIMIT ?"));
            assertFalse(sql.contains("SELECT *"));
            assertFalse(sql.contains("OFFSET"));
        });
    }

    @Test void compositeCursorUsesSeparateParametersAndIntegerTypeColumn() {
        var after = new ObjectKey.Resource("player:'", ResourceType.WOOD);
        var query = StateRows.Query.scan(7, after, "player:'", false, 12);
        var bound = configuration.getMappedStatement(StateMapper.class.getName() + ".resources").getBoundSql(Map.of("q", query));
        assertTrue(bound.getSql().contains("(player_id,resource_type) > (?,?)"));
        assertTrue(bound.getSql().contains("AND player_id=? ORDER BY player_id,resource_type"));
        assertFalse(bound.getSql().contains("player:'"));
        assertTrue(bound.getParameterMappings().stream().anyMatch(mapping -> mapping.getProperty().equals("q.after.resourceType")));
    }

    @Test void activeMarchQueryFiltersOwnerAndActiveStatusesInSql() {
        var sql = sql("marches", Map.of("q", StateRows.Query.scan(7, new ObjectKey.March("m"), "p", true, 20)));
        assertTrue(sql.contains("FORCE INDEX (ix_march_active)"));
        assertTrue(sql.contains("AND player_id=? AND status IN (1,2,3,4)"));
        assertTrue(sql.contains("ORDER BY march_id"));
        assertFalse(sql.contains("COMPLETED"));
    }

    @Test void dueQueryFixesPendingStatusUpperBoundAndExclusiveTupleCursor() {
        var query = new StateRows.Query(7, null, null, null, false, null, null, 200L, 100L,
                "task", true, 1, 4096);
        var sql = sql("tasks", Map.of("q", query));
        assertTrue(sql.contains("status=1 AND due_at<=? AND (due_at,task_id)>(?,?)"));
        assertTrue(sql.endsWith("ORDER BY due_at,task_id LIMIT ?"));
    }

    @Test void battlePayloadIsBoundedBeforeJdbcMaterializesBlob() {
        var sql = sql("battles", Map.of("q", StateRows.Query.scan(7, null, null, false, 1).withPayloadBudget(1024)));
        assertTrue(sql.contains("CASE WHEN OCTET_LENGTH(input_json)>LEAST(?,1048576) THEN X'' ELSE input_json END AS input_json"));
        assertTrue(sql.contains("OCTET_LENGTH(input_json) AS input_bytes"));
        assertTrue(sql.contains("OCTET_LENGTH(result_json) AS result_bytes"));
    }

    @Test void callerCannotRemoveStoredPayloadLimitWithAnUnlimitedBudget() {
        var sql = sql("battles", Map.of("q", StateRows.Query.scan(7, null, null, false, 1).withPayloadBudget(Long.MAX_VALUE)));
        assertTrue(sql.contains("OCTET_LENGTH(input_json)>LEAST(?,1048576)"));
        assertTrue(sql.contains("OCTET_LENGTH(result_json)>LEAST(?,1048576)"));
    }

    @Test void rootUpdateUsesVersionCasAndLeavesSourceSequenceForTransactionTail() {
        var row = new StateRows.Resource("p", ResourceType.WOOD, 50L, 2L, 0L);
        var sql = sql("update", Map.of("realmId", 7, "row", row, "expectedVersion", 1L));
        assertEquals("UPDATE game_player_resource SET amount=?,version=? WHERE realm_id=? AND player_id=? AND resource_type=? AND version=?", sql);
        assertFalse(sql.contains("last_seq"));
    }

    @Test void rootCreationReservesUniqueKeyWithExplicitColumnsBeforeCursor() {
        var row = new StateRows.Resource("p", ResourceType.WOOD, 50L, 1L, 0L);
        var sql = sql("insert", Map.of("realmId", 7, "row", row));
        assertEquals("INSERT INTO game_player_resource (realm_id,player_id,resource_type,amount,version,last_seq) VALUES (?,?,?,?,?,?)", sql);
        assertFalse(sql.contains("ON DUPLICATE"));
    }

    @Test void versionReadIsLockingAndFinalizationOnlyTouchesTheAlreadyLockedRoot() {
        var key = new ObjectKey.GatherSite(10, 2);
        assertEquals("SELECT version,last_seq FROM game_gather_site WHERE realm_id=? AND x=? AND y=? FOR UPDATE",
                sql("lockVersion", Map.of("realmId", 7, "key", key)));
        assertEquals("UPDATE game_gather_site SET last_seq=? WHERE realm_id=? AND x=? AND y=?",
                sql("finalizeSequence", Map.of("realmId", 7, "key", key, "sequence", 9L)));
    }

    private String sql(String method, Map<String, ?> arguments) {
        return configuration.getMappedStatement(StateMapper.class.getName() + "." + method).getBoundSql(arguments).getSql();
    }
    private static Configuration configuration() {
        var configuration = new Configuration();
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.setArgNameBasedConstructorAutoMapping(true);
        IntCodeTypeHandler.register(configuration);
        configuration.addMapper(StateMapper.class);
        return configuration;
    }
}

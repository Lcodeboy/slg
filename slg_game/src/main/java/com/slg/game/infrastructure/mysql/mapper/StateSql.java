package com.slg.game.infrastructure.mysql.mapper;

import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.row.StateRows;
import org.apache.ibatis.builder.annotation.ProviderContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 仅按封闭类型选择固定表/列，所有业务值均绑定参数。没有调用方自带表名或任意字段更新入口。
 * 主键元组用于 keyset 分页，资源/兵种绑定整数 TypeHandler，不依赖名称排序或 OFFSET 扫历史。
 */
public final class StateSql {
    private StateSql() {}

    record Shape(String table, List<String> keys, List<String> fields) {}

    private static Shape shape(ObjectKind kind) {
        return switch (kind) {
            case PLAYER -> shape("game_player", "playerId", "playerId accountId foundingRealmId currentRealmId name");
            case RESOURCE -> shape("game_player_resource", "playerId resourceType", "playerId resourceType amount");
            case TROOP -> shape("game_player_troop", "playerId troopType", "playerId troopType available wounded dead");
            case HERO -> shape("game_player_hero", "playerId heroId", "playerId heroId assignedMarchId");
            case MARCH -> shape("game_march", "marchId", "marchId playerId status mission fromX fromY targetX targetY departAt arriveAt tier taskId rulesVersion");
            case GATHER_SITE -> shape("game_gather_site", "x y", "x y occupantPlayerId occupantMarchId generation");
            case TASK -> shape("game_due_task", "taskId", "taskId playerId marchId type status generation dueAt reason");
            case BATTLE -> shape("game_battle", "battleId", "battleId marchId playerId status inputHash rulesVersion marchVersion inputSchema inputJson resultSchema resultJson");
        };
    }

    private static Shape shape(String table, String keys, String fields) {
        return new Shape(table, List.of(keys.split(" ")), List.of((fields + " version lastSeq").split(" ")));
    }

    private static String column(String field) { return field.replaceAll("([A-Z])", "_$1").toLowerCase(java.util.Locale.ROOT); }
    private static String bind(String prefix, String field) { return "#{" + prefix + "." + field + "}"; }
    private static String equality(Shape shape, String prefix) {
        return shape.keys().stream().map(key -> column(key) + "=" + bind(prefix, key)).collect(Collectors.joining(" AND "));
    }
    private static String after(Shape shape, String prefix) {
        return "(" + shape.keys().stream().map(StateSql::column).collect(Collectors.joining(",")) + ") > ("
                + shape.keys().stream().map(key -> bind(prefix, key)).collect(Collectors.joining(",")) + ")";
    }

    public static String select(Map<String, Object> parameters, ProviderContext context) {
        var kind = switch (context.getMapperMethod().getName()) {
            case "players" -> ObjectKind.PLAYER;
            case "resources" -> ObjectKind.RESOURCE;
            case "troops" -> ObjectKind.TROOP;
            case "heroes" -> ObjectKind.HERO;
            case "marches" -> ObjectKind.MARCH;
            case "gatherSites" -> ObjectKind.GATHER_SITE;
            case "tasks" -> ObjectKind.TASK;
            case "battles" -> ObjectKind.BATTLE;
            default -> throw new IllegalArgumentException("Unsupported state query");
        };
        var q = (StateRows.Query) parameters.get("q");
        var shape = shape(kind);
        String sql = "SELECT " + shape.fields().stream().map(field -> {
            String column = column(field);
            // 在 JDBC 复制 LONGBLOB 之前限制长度；空字节哨兵由 Repository 识别为预算失败。
            if (field.equals("inputJson") || field.equals("resultJson"))
                return "CASE WHEN OCTET_LENGTH(" + column + ")>LEAST(#{q.maxPayloadBytes},"
                        + PersistenceCodec.DEFAULT_MAX_JSON_BYTES + ") THEN X'' ELSE " + column + " END AS " + column;
            return column;
        }).collect(Collectors.joining(","))
                + (kind == ObjectKind.BATTLE ? ",OCTET_LENGTH(input_json) AS input_bytes,OCTET_LENGTH(result_json) AS result_bytes" : "")
                + " FROM " + shape.table() + indexHint(kind, q) + " WHERE realm_id=#{q.realmId}";
        if (q.exact() != null) {
            if (q.exact().kind() != kind) throw new IllegalArgumentException("Query key kind mismatch");
            sql += " AND " + equality(shape, "q.exact");
        }
        if (q.after() != null) {
            if (q.after().kind() != kind) throw new IllegalArgumentException("Cursor key kind mismatch");
            sql += " AND " + after(shape, "q.after");
        }
        if (q.owner() != null) sql += " AND " + (kind == ObjectKind.GATHER_SITE ? "occupant_player_id" : "player_id") + "=#{q.owner}";
        if (q.accountId() != null) {
            if (kind != ObjectKind.PLAYER) throw new IllegalArgumentException("Account lookup requires player");
            sql += " AND account_id=#{q.accountId}";
        }
        if (q.activeOnly()) {
            if (kind != ObjectKind.MARCH || q.owner() == null) throw new IllegalArgumentException("Active query requires owned marches");
            sql += " AND status IN (1,2,3,4)";
        }
        if (q.movingStatus() != null) {
            if (kind != ObjectKind.MARCH || q.owner() != null || q.activeOnly())
                throw new IllegalArgumentException("Moving query requires realm-wide marches");
            // 固定状态后按 march_id 沿索引分页，避免 IN 两段后每页重新 filesort 全部移动项。
            sql += " AND status=#{q.movingStatus}";
        }
        if (q.pendingOnly()) {
            if (kind != ObjectKind.TASK && kind != ObjectKind.BATTLE) throw new IllegalArgumentException("Invalid pending query");
            sql += " AND status=1";
        }
        if (q.dueThrough() != null) {
            if (kind != ObjectKind.TASK || !q.pendingOnly()) throw new IllegalArgumentException("Due query requires pending tasks");
            sql += " AND due_at<=#{q.dueThrough}";
            if (q.afterDueAt() != null)
                sql += " AND (due_at,task_id)>(#{q.afterDueAt},#{q.afterTaskId})";
            return sql + " ORDER BY due_at,task_id LIMIT #{q.limit}";
        }
        return sql + " ORDER BY " + shape.keys().stream().map(StateSql::column).collect(Collectors.joining(",")) + " LIMIT #{q.limit}";
    }

    private static String indexHint(ObjectKind kind, StateRows.Query q) {
        // 查询契约禁止扫描 COMPLETED 历史再过滤；不能为了省排序任由优化器改走全历史主键。
        if (q.activeOnly()) return " FORCE INDEX (ix_march_active)";
        if (q.movingStatus() != null) return " FORCE INDEX (ix_march_moving)";
        if (q.accountId() != null) return " FORCE INDEX (uq_player_account)";
        if (q.pendingOnly()) return " FORCE INDEX (" + (kind == ObjectKind.TASK ? "ix_task_due" : "ix_battle_pending") + ")";
        if (q.owner() == null) return "";
        return " FORCE INDEX (" + switch (kind) {
            case MARCH -> "ix_march_owned";
            case GATHER_SITE -> "ix_gather_owned";
            case TASK -> "ix_task_owned";
            case BATTLE -> "ix_battle_owned";
            default -> "PRIMARY";
        } + ")";
    }

    public static String lockVersion(Map<String, Object> parameters) {
        var shape = shape(((ObjectKey) parameters.get("key")).kind());
        // RR 的完整唯一键锁定读也覆盖不存在条件。调用者须在 cursor 前完成创建 INSERT。
        return "SELECT version,last_seq FROM " + shape.table() + " WHERE realm_id=#{realmId} AND "
                + equality(shape, "key") + " FOR UPDATE";
    }

    public static String insert(Map<String, Object> parameters) {
        var shape = shape(((StateRows.Root) parameters.get("row")).key().kind());
        return "INSERT INTO " + shape.table() + " (realm_id,"
                + shape.fields().stream().map(StateSql::column).collect(Collectors.joining(",")) + ") VALUES (#{realmId},"
                + shape.fields().stream().map(field -> bind("row", field)).collect(Collectors.joining(",")) + ")";
    }

    public static String update(Map<String, Object> parameters) {
        var shape = shape(((StateRows.Root) parameters.get("row")).key().kind());
        // 更新不把已提交来源序号清零；根锁与 version CAS 保护明细，尾部统一写新 last_seq。
        return "UPDATE " + shape.table() + " SET " + shape.fields().stream()
                .filter(field -> !shape.keys().contains(field) && !field.equals("lastSeq"))
                .map(field -> column(field) + "=" + bind("row", field)).collect(Collectors.joining(","))
                + " WHERE realm_id=#{realmId} AND " + equality(shape, "row") + " AND version=#{expectedVersion}";
    }

    public static String finalizeSequence(Map<String, Object> parameters) {
        var shape = shape(((ObjectKey) parameters.get("key")).kind());
        return "UPDATE " + shape.table() + " SET last_seq=#{sequence} WHERE realm_id=#{realmId} AND " + equality(shape, "key");
    }
}

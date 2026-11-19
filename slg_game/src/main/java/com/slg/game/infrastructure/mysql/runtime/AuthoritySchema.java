package com.slg.game.infrastructure.mysql.runtime;

import java.util.*;

/**
 * schema v1 的运行依赖，不生成或执行 DDL。列宽/二进制排序参与对象身份与分页语义，
 * 索引名称参与 FORCE INDEX，故不能只检查表存在就允许进程接管写权。
 */
final class AuthoritySchema {
    private AuthoritySchema() { }
    record Column(String type, boolean nullable, Long length, String collation) { }
    record Index(String name, boolean unique, List<String> columns) { }
    record Table(String name, Map<String, Column> columns, List<Index> indexes) { }

    static final List<Table> TABLES = List.of(
            table("realm_identity", "realm_id:i dataset_id:b16 schema_version:i", "realm_id", ix("dataset_id", true, "dataset_id")),
            table("realm_owner", "realm_id:i writer_epoch:l owner_instance:b16? active:t", "realm_id"),
            table("realm_owner_lease", "realm_id:i writer_epoch:l owner_instance:b16? expires_at_millis:l", "realm_id"),
            table("realm_commit_cursor", "realm_id:i high_seq:l pruned_through:l", "realm_id"),
            table("game_player", "realm_id:i player_id:v256 account_id:l founding_realm_id:i current_realm_id:i name:u128 version:l last_seq:l",
                    "realm_id,player_id", ix("uq_player_account", true, "realm_id,account_id")),
            table("game_player_resource", "realm_id:i player_id:v256 resource_type:i amount:l version:l last_seq:l", "realm_id,player_id,resource_type"),
            table("game_player_troop", "realm_id:i player_id:v256 troop_type:i available:l wounded:l dead:l version:l last_seq:l", "realm_id,player_id,troop_type"),
            table("game_player_hero", "realm_id:i player_id:v256 hero_id:v256 assigned_march_id:v256? version:l last_seq:l", "realm_id,player_id,hero_id"),
            table("game_march", "realm_id:i march_id:v256 player_id:v256 status:i mission:i from_x:d from_y:d target_x:i target_y:i depart_at:l arrive_at:l tier:i task_id:v256? rules_version:v128 version:l last_seq:l",
                    "realm_id,march_id", ix("ix_march_owned", false, "realm_id,player_id,march_id"),
                    ix("ix_march_active", false, "realm_id,player_id,status,march_id"), ix("ix_march_moving", false, "realm_id,status,march_id")),
            table("game_march_troop", "realm_id:i march_id:v256 troop_type:i count:l", "realm_id,march_id,troop_type"),
            table("game_march_cargo", "realm_id:i march_id:v256 resource_type:i count:l", "realm_id,march_id,resource_type"),
            table("game_march_hero", "realm_id:i march_id:v256 hero_id:v256 slot_index:i", "realm_id,march_id,hero_id",
                    ix("uq_march_hero_slot", true, "realm_id,march_id,slot_index")),
            table("game_gather_site", "realm_id:i x:i y:i occupant_player_id:v256? occupant_march_id:v256? generation:l version:l last_seq:l",
                    "realm_id,x,y", ix("ix_gather_owned", false, "realm_id,occupant_player_id,x,y")),
            table("game_due_task", "realm_id:i task_id:v256 player_id:v256 march_id:v256 type:i status:i generation:l due_at:l reason:u2048? version:l last_seq:l",
                    "realm_id,task_id", ix("ix_task_due", false, "realm_id,status,due_at,task_id"), ix("ix_task_owned", false, "realm_id,player_id,task_id")),
            table("game_battle", "realm_id:i battle_id:v256 player_id:v256 march_id:v256 status:i input_hash:v128 rules_version:v128 march_version:l input_schema:i input_json:blob result_schema:i? result_json:blob? version:l last_seq:l",
                    "realm_id,battle_id", ix("ix_battle_pending", false, "realm_id,status,battle_id"), ix("ix_battle_owned", false, "realm_id,player_id,battle_id")),
            table("game_battle_hero", "realm_id:i battle_id:v256 hero_id:v256 slot_index:i", "realm_id,battle_id,hero_id",
                    ix("uq_battle_hero_slot", true, "realm_id,battle_id,slot_index")),
            table("game_receipt", "realm_id:i scope_id:v256 request_id:v128 digest:v128 journal_seq:l result_json:blob", "realm_id,scope_id,request_id"),
            table("game_journal", "realm_id:i journal_seq:l entry_schema:i entry_payload:blob entry_hash:b32", "realm_id,journal_seq")
    );
    static final Map<String, Table> BY_NAME;
    static final String SQL_TABLE_LIST;
    static {
        var tables = new LinkedHashMap<String, Table>();
        TABLES.forEach(table -> tables.put(table.name(), table)); BY_NAME = Map.copyOf(tables);
        // 仅拼接本文件固定标识符，任何环境值/数据库文本都不进入 SQL 语法。
        SQL_TABLE_LIST = TABLES.stream().map(table -> "'" + table.name() + "'").collect(java.util.stream.Collectors.joining(","));
    }
    private static Index ix(String name, boolean unique, String columns) {
        return new Index(name, unique, List.of(columns.split(",")));
    }
    private static Table table(String name, String descriptors, String primary, Index... indexes) {
        var columns = new LinkedHashMap<String, Column>();
        for (String descriptor : descriptors.split(" ")) {
            String[] pair = descriptor.split(":"); String type = pair[1]; boolean nullable = type.endsWith("?");
            if (nullable) type = type.substring(0, type.length() - 1);
            var value = switch (type) {
                case "i" -> new Column("int", nullable, null, null);
                case "l" -> new Column("bigint", nullable, null, null);
                case "d" -> new Column("double", nullable, null, null);
                case "t" -> new Column("tinyint", nullable, null, null);
                case "b16" -> new Column("binary", nullable, 16L, null);
                case "b32" -> new Column("binary", nullable, 32L, null);
                case "v128" -> new Column("varchar", nullable, 128L, "ascii_bin");
                case "v256" -> new Column("varchar", nullable, 256L, "ascii_bin");
                case "u128" -> new Column("varchar", nullable, 128L, "utf8mb4_bin");
                case "u2048" -> new Column("varchar", nullable, 2048L, "utf8mb4_bin");
                case "blob" -> new Column("longblob", nullable, null, null);
                default -> throw new ExceptionInInitializerError("Unknown authority column contract");
            };
            columns.put(pair[0], value);
        }
        var required = new ArrayList<Index>(); required.add(ix("PRIMARY", true, primary)); required.addAll(List.of(indexes));
        return new Table(name, Map.copyOf(columns), List.copyOf(required));
    }
}

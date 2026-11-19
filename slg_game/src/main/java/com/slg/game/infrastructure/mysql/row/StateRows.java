package com.slg.game.infrastructure.mysql.row;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;

/**
 * SQL 行 DTO，仅在基础设施内使用。核心列逐项声明，不以字段 Map 或 JSON 替代权威状态。
 * 数值使用包装类型：意外 SQL NULL 必须在组装领域值时失败，不能被 JDBC 默认成合法的零。
 * 行军/战斗明细没有独立版本，必须与根行在同一 Session 中读取或替换。
 */
public final class StateRows {
    private StateRows() {}

    public sealed interface Root permits Player, Resource, Troop, Hero, March, GatherSite, Task, Battle {
        Long version();
        Long lastSeq();
        ObjectKey key();
    }

    public record Player(String playerId, Long accountId, Integer foundingRealmId, Integer currentRealmId,
                         String name, Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.Player key() { return new ObjectKey.Player(playerId); }
    }
    public record Resource(String playerId, ResourceType resourceType, Long amount,
                           Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.Resource key() { return new ObjectKey.Resource(playerId, resourceType); }
    }
    public record Troop(String playerId, TroopType troopType, Long available, Long wounded, Long dead,
                        Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.Troop key() { return new ObjectKey.Troop(playerId, troopType); }
    }
    public record Hero(String playerId, String heroId, String assignedMarchId,
                       Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.Hero key() { return new ObjectKey.Hero(playerId, heroId); }
    }
    public record March(String marchId, String playerId, MarchState.Status status, MarchState.Mission mission,
                        Double fromX, Double fromY, Integer targetX, Integer targetY, Long departAt, Long arriveAt,
                        Integer tier, String taskId, String rulesVersion, Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.March key() { return new ObjectKey.March(marchId); }
    }
    public record GatherSite(Integer x, Integer y, String occupantPlayerId, String occupantMarchId, Long generation,
                             Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.GatherSite key() { return new ObjectKey.GatherSite(x, y); }
    }
    public record Task(String taskId, String playerId, String marchId, TaskState.Type type, TaskState.Status status,
                       Long generation, Long dueAt, String reason, Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.Task key() { return new ObjectKey.Task(taskId); }
    }
    public record Battle(String battleId, String marchId, String playerId, BattleState.Status status,
                         String inputHash, String rulesVersion, Long marchVersion, Integer inputSchema,
                         byte[] inputJson, Integer resultSchema, byte[] resultJson,
                         Long inputBytes, Long resultBytes,
                         Long version, Long lastSeq) implements Root {
        @Override public ObjectKey.Battle key() { return new ObjectKey.Battle(battleId); }
    }
    public record MarchTroop(TroopType troopType, Long count) {}
    public record MarchCargo(ResourceType resourceType, Long count) {}
    public record HeroSlot(String heroId, Integer slotIndex) {}
    public record Version(Long version, Long lastSeq) {}

    /** 仅控制已审核查询形状；字符串值始终用绑定参数，不能当作 SQL 片段。 */
    public record Query(int realmId, ObjectKey exact, ObjectKey after, String owner, boolean activeOnly, Integer movingStatus,
                        Long accountId, Long dueThrough, Long afterDueAt, String afterTaskId,
                        boolean pendingOnly, int limit, long maxPayloadBytes) {
        public Query {
            if (realmId <= 0 || limit <= 0 || maxPayloadBytes < 0) throw new IllegalArgumentException("Invalid state query bounds");
            if (movingStatus != null && movingStatus != MarchState.Status.OUTBOUND.code()
                    && movingStatus != MarchState.Status.RETURNING.code())
                throw new IllegalArgumentException("Map scan requires a moving march status");
        }
        public static Query exact(int realmId, ObjectKey key) {
            return new Query(realmId, key, null, null, false, null, null, null, null, null, false, 2, 8L * 1024 * 1024);
        }
        public static Query scan(int realmId, ObjectKey after, String owner, boolean active, int limit) {
            return new Query(realmId, null, after, owner, active, null, null, null, null, null, false, limit, 8L * 1024 * 1024);
        }
        public static Query moving(int realmId, ObjectKey after, int status, int limit) {
            return new Query(realmId, null, after, null, false, status, null, null, null, null, false, limit, 8L * 1024 * 1024);
        }
        public Query withPayloadBudget(long bytes) {
            return new Query(realmId, exact, after, owner, activeOnly, movingStatus, accountId, dueThrough, afterDueAt,
                    afterTaskId, pendingOnly, limit, bytes);
        }
    }
}

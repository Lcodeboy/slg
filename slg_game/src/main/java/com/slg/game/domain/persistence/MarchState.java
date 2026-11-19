package com.slg.game.domain.persistence;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.core.type.EnumCodes;
import com.slg.core.type.IntCodeEnum;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 一条行军的完整聚合后像。固定字段映射行军主表；troops、heroIds 和 cargo 分别映射
 * 结构化明细表，均受主行同一版本保护，不把集合序列化为另一份可写 JSON 状态。
 *
 * <p>集合防御复制，保留英雄次序。适配器须在同一事务读写主行及全部明细；一条行军
 * 变更的日志也包含完整后像，消费者不能只应用一部分。时间为 UTC Unix 毫秒，途中召回
 * 的小数起点保留为有限 double。地图边界、速度、兵种配置与合法状态转换由领域用例校验。</p>
 */
public record MarchState(String marchId, String playerId, Status status, Mission mission,
                         double fromX, double fromY, int targetX, int targetY,
                         long departAt, long arriveAt, int tier, Map<TroopType, Long> troops,
                         List<String> heroIds, String taskId, String rulesVersion, Map<ResourceType, Long> cargo)
        implements ObjectState {
    public MarchState {
        new ObjectKey.March(marchId);
        PersistenceChecks.identifier(playerId, "playerId", 256);
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(mission, "mission");
        PersistenceChecks.coordinate(fromX, "fromX");
        PersistenceChecks.coordinate(fromY, "fromY");
        PersistenceChecks.nonNegative(targetX, "targetX");
        PersistenceChecks.nonNegative(targetY, "targetY");
        PersistenceChecks.nonNegative(departAt, "departAt");
        if (arriveAt < departAt) throw new IllegalArgumentException("Arrival cannot precede departure");
        PersistenceChecks.positive(tier, "tier");
        troops = PersistenceChecks.amounts(troops, "troops");
        heroIds = PersistenceChecks.identifiers(heroIds, "heroIds");
        PersistenceChecks.optionalIdentifier(taskId, "taskId", 256);
        PersistenceChecks.identifier(rulesVersion, "rulesVersion", 128);
        cargo = PersistenceChecks.amounts(cargo, "cargo");
        boolean timed = status == Status.OUTBOUND || status == Status.GATHERING || status == Status.RETURNING;
        if (timed != (taskId != null))
            throw new IllegalArgumentException("Only a timed march stage must refer to a task");
        if (status == Status.GATHERING && mission != Mission.GATHER
                || status == Status.BATTLE_PENDING && mission != Mission.ATTACK_NPC)
            throw new IllegalArgumentException("March stage does not match its mission");
    }

    @Override public ObjectKey.March key() { return new ObjectKey.March(marchId); }

    /** 状态编码是持久化契约，不取 ordinal；发布后不改值、不复用，读取未知编码立即拒绝。 */
    public enum Status implements IntCodeEnum {
        OUTBOUND(1), GATHERING(2), BATTLE_PENDING(3), RETURNING(4), COMPLETED(5);

        private static final Map<Integer, Status> BY_CODE = EnumCodes.index(values());
        private final int code;
        Status(int code) { this.code = code; }
        @Override public int code() { return code; }
        public static Status fromCode(int code) { return EnumCodes.require(BY_CODE, code, "MarchState.Status"); }
    }

    /** 任务目的编码与状态编码独立；不使用 ordinal，不改值或复用，未知编码不能降级成采集。 */
    public enum Mission implements IntCodeEnum {
        GATHER(1), ATTACK_NPC(2);

        private static final Map<Integer, Mission> BY_CODE = EnumCodes.index(values());
        private final int code;
        Mission(int code) { this.code = code; }
        @Override public int code() { return code; }
        public static Mission fromCode(int code) { return EnumCodes.require(BY_CODE, code, "MarchState.Mission"); }
    }
}

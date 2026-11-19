package com.slg.game.domain.persistence;

import com.slg.core.type.EnumCodes;
import com.slg.core.type.IntCodeEnum;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 战斗任务的权威主行与有序英雄明细。状态、归属、规则、输入指纹和行军预留版本都是
 * 结构化字段；只有固定计算输入和完整结果使用版本化 JSON。英雄明细随主版本一起读写。
 *
 * <p>PENDING 没有结果，SETTLED 必须有结果；结果内容、输入哈希及战损合法性由 codec
 * 与领域结算校验。构造成功不表示 Worker 获得结算权。结算须与伤亡、行军及回执同事务，
 * 已结算行保留用于原战报查询，不再维护另一张可独立变动的 pending-battle 权威表。</p>
 */
public record BattleState(String battleId, String marchId, String playerId, Status status,
                          List<String> heroIds, String inputHash, String rulesVersion, long marchVersion,
                          JsonPayload input, JsonPayload result) implements ObjectState {
    public BattleState {
        new ObjectKey.Battle(battleId);
        PersistenceChecks.identifier(marchId, "marchId", 256);
        PersistenceChecks.identifier(playerId, "playerId", 256);
        Objects.requireNonNull(status, "status");
        heroIds = PersistenceChecks.identifiers(heroIds, "heroIds");
        PersistenceChecks.identifier(inputHash, "inputHash", 128);
        PersistenceChecks.identifier(rulesVersion, "rulesVersion", 128);
        PersistenceChecks.positive(marchVersion, "marchVersion");
        Objects.requireNonNull(input, "input");
        if ((status == Status.SETTLED) != (result != null))
            throw new IllegalArgumentException("Only a settled battle must carry its result");
    }

    @Override public ObjectKey.Battle key() { return new ObjectKey.Battle(battleId); }

    /** 状态编码不取 ordinal，发布后不改值、不复用；未知编码拒绝读取，不能猜测战斗是否已结算。 */
    public enum Status implements IntCodeEnum {
        PENDING(1), SETTLED(2);

        private static final Map<Integer, Status> BY_CODE = EnumCodes.index(values());
        private final int code;
        Status(int code) { this.code = code; }
        @Override public int code() { return code; }
        public static Status fromCode(int code) { return EnumCodes.require(BY_CODE, code, "BattleState.Status"); }
    }
}

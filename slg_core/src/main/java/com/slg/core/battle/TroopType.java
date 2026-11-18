package com.slg.core.battle;

import com.slg.core.type.EnumCodes;
import com.slg.core.type.IntCodeEnum;
import java.util.Map;

/**
 * 远征演示规则中的三种兵种。
 * 声明顺序同时参与前排目标排序，改变顺序会影响确定性结果，需作为规则版本变更处理。
 * 新权威库使用独立的固定 code；code 不由 ordinal 推导，也不能因增加数据库映射而
 * 调整现有声明顺序。保留 name/默认 JSON 行为，旧运行链路仍按原名称编解码。
 */
public enum TroopType implements IntCodeEnum {
    /** 步兵，在当前演示规则中优先承受攻击。 */
    INFANTRY(1),
    /** 枪兵，在步兵之后参与目标排序。 */
    LANCER(2),
    /** 射手，在当前演示规则中排在最后。 */
    MARKSMAN(3);

    private static final Map<Integer, TroopType> BY_CODE = EnumCodes.index(values());
    private final int code;

    TroopType(int code) { this.code = code; }

    @Override public int code() { return code; }

    /** 未知持久化编码明确失败，不能按声明位置选一个兵种继续结算。 */
    public static TroopType fromCode(int code) { return EnumCodes.require(BY_CODE, code, "TroopType"); }
}

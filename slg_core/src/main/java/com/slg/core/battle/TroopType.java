package com.slg.core.battle;

/**
 * 远征演示规则中的三种兵种。
 * 声明顺序同时参与前排目标排序，改变顺序会影响确定性结果，需作为规则版本变更处理。
 */
public enum TroopType {
    /** 步兵，在当前演示规则中优先承受攻击。 */
    INFANTRY,
    /** 枪兵，在步兵之后参与目标排序。 */
    LANCER,
    /** 射手，在当前演示规则中排在最后。 */
    MARKSMAN
}

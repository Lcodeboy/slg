package com.slg.core.battle;

/** 两种演示战斗模式共用的胜负结果，由最后一个同步回合结算后的存活状态决定。 */
public enum Outcome {
    /** 仅攻击方仍有存活作战单位。 */
    ATTACKER_WIN,
    /** 仅防守方仍有存活作战单位。 */
    DEFENDER_WIN,
    /** 双方均被消灭，或达到回合上限时双方均仍有存活单位。 */
    DRAW
}

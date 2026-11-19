package com.slg.game.domain.persistence;

/**
 * 已支持业务对象的不可变完整状态。封闭类型集合防止调用方用 Object、任意属性 Map 或
 * JSON 文本伪装核心权威状态。状态映射到结构化表，事件仍可由独立版本化 codec 编码。
 * 构造只检查本对象的不变量；跨对象归属、余额守恒与允许的状态转换仍由用例和事务保证。
 */
public sealed interface ObjectState permits PlayerState, ResourceState, TroopState, HeroState,
        MarchState, GatherSiteState, TaskState, BattleState {
    ObjectKey key();
}

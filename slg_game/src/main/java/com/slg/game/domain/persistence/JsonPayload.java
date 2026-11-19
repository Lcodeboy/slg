package com.slg.game.domain.persistence;

/**
 * 固定战斗输入/结果的版本化辅助载荷，不能用作 ObjectState 或资源等核心行的替代。
 * 保留原文本，只检查版本与非空；后续 codec 必须校验语法、具体 schema、编码大小与
 * 输入指纹，构造本值不代表 JSON 已通过校验。业务列不得藏进此载荷成为另一份权威状态。
 */
public record JsonPayload(int schemaVersion, String json) {
    public JsonPayload {
        PersistenceChecks.positive(schemaVersion, "schemaVersion");
        PersistenceChecks.jsonText(json, "json");
    }
}

package com.slg.game.application.background;

import java.util.Set;

/** 系统回执只记录原始推进结果；客户端回执查询的 account scope 不读取这些独立去重域。 */
public record BackgroundOutcome(String operation, long accountId, String objectId, long generation, String status) {
    public BackgroundOutcome {
        if (!Set.of("DUE_TASK", "BATTLE_RESULT").contains(operation) || accountId <= 0 || generation <= 0
                || objectId == null || objectId.isBlank() || objectId.length() > 256
                || objectId.chars().anyMatch(c -> c < 33 || c > 126)
                || !Set.of("APPLIED", "BLOCKED").contains(status))
            throw new IllegalArgumentException("Invalid background outcome");
    }
}

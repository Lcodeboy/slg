package com.slg.game.application.march;

import java.util.Objects;

/**
 * 召回资格规划的只读提示，不是裁决授权。正式取得账号/行军/任务/矿点资格后必须重读并核验。
 * version=0 表示规划时没看到行军；不能把它当成最终不存在，也不能据此写拒绝回执。
 */
public record MarchPlan(String marchId, long version, Integer siteX, Integer siteY) {
    public MarchPlan {
        Objects.requireNonNull(marchId, "marchId");
        if (marchId.isBlank() || marchId.length() > 64 || version < 0)
            throw new IllegalArgumentException("Invalid march planning identity");
        if ((siteX == null) != (siteY == null) || (siteX != null &&
                (siteX < 0 || siteX > 1199 || siteY < 0 || siteY > 1199 || version == 0)))
            throw new IllegalArgumentException("Invalid planned gather site");
    }
}

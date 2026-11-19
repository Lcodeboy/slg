package com.slg.game.domain.march;

import com.slg.game.domain.persistence.MarchState;
import com.slg.game.domain.persistence.ObjectChange;
import com.slg.game.domain.persistence.ObjectKey;
import java.util.List;
import java.util.Map;

/**
 * 一项行军业务的不可变完整裁决。changes 是各根最终后像，不能分拆提交；每根只增加一次版本。
 * 召回前补做已到期阶段后仍可能拒绝召回，此时 error 与已经裁决的变化共同提交。
 * phases 保留本次推演顺序供事实审计，不表示每个内存阶段都已有独立持久版本。
 */
public record MarchTransition(List<ObjectChange> changes, Map<ObjectKey, Long> readVersions, MarchState march,
                              String errorCode, String errorMessage, List<String> phases, PendingBattle pendingBattle) {
    public MarchTransition {
        changes = List.copyOf(changes); readVersions = Map.copyOf(readVersions); phases = List.copyOf(phases);
        if ((errorCode == null) != (errorMessage == null) || (errorCode == null) != (march != null))
            throw new IllegalArgumentException("A march result must contain either success or rejection");
    }
}

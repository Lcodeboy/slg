package com.slg.game.domain.march;

import com.slg.game.domain.persistence.ObjectChange;
import com.slg.game.domain.persistence.ObjectKey;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 到期裁决的纯结果；SKIPPED 没有持久业务，BLOCKED 仅隔离坏任务，APPLIED 才推进合法阶段。 */
public record DueTransition(Status status, List<ObjectChange> changes, Map<ObjectKey, Long> readVersions,
                            List<String> phases, PendingBattle pendingBattle) {
    public DueTransition {
        Objects.requireNonNull(status); changes = List.copyOf(changes); readVersions = Map.copyOf(readVersions);
        phases = List.copyOf(phases);
        if (status == Status.SKIPPED && (!changes.isEmpty() || !phases.isEmpty() || pendingBattle != null))
            throw new IllegalArgumentException("Skipped due task cannot carry a mutation");
        if (status != Status.APPLIED && pendingBattle != null)
            throw new IllegalArgumentException("Only applied arrival can prepare a battle");
    }
    public enum Status { APPLIED, BLOCKED, SKIPPED }
}

package com.slg.game.application.background;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.march.MarchPlan;
import com.slg.game.domain.march.MarchIdentity;
import com.slg.game.domain.persistence.*;
import java.util.*;

/** 系统入口声明与 handler 读前核验共用同一范围定义，禁止从已经裁决的 changes 才补锁。 */
final class BackgroundScopes {
    private BackgroundScopes() { }

    static Set<ObjectKey> task(BackgroundInvocation input) {
        var task = (TaskState) input.candidate().state();
        var plan = Objects.requireNonNull(input.plan(), "task plan");
        var keys = new LinkedHashSet<ObjectKey>();
        keys.add(task.key()); keys.add(new ObjectKey.March(task.marchId())); keys.add(new ObjectKey.Battle(task.marchId()));
        MarchIdentity.taskIds(task.marchId()).forEach(id -> keys.add(new ObjectKey.Task(id)));
        if (plan.siteX() != null) keys.add(new ObjectKey.GatherSite(plan.siteX(), plan.siteY()));
        return Set.copyOf(keys);
    }

    static Set<ObjectKey> battle(BackgroundInvocation input) {
        var battle = (BattleState) input.candidate().state();
        return Set.of(battle.key(), new ObjectKey.March(battle.marchId()),
                new ObjectKey.Task(MarchIdentity.returning(battle.marchId())));
    }

    static void validate(OperationCommand command, BackgroundInvocation input, BackgroundCodec codec,
                         CommitRequest.Origin expected, Set<ObjectKey> objects) {
        if (command.scope().origin() != expected || !command.scope().accounts().contains(input.accountId())
                || !command.scope().request().equals(codec.request(input.candidate()))
                || !command.scope().objects().containsAll(objects))
            throw new IllegalArgumentException("System input differs from its declared qualification");
    }

    static MarchPlan plan(String id, VersionedState row) {
        if (row == null) return new MarchPlan(id, 0, null, null);
        var march = (MarchState) row.state();
        boolean mine = march.mission() == MarchState.Mission.GATHER &&
                (march.status() == MarchState.Status.OUTBOUND || march.status() == MarchState.Status.GATHERING);
        return new MarchPlan(id, row.version(), mine ? march.targetX() : null, mine ? march.targetY() : null);
    }
}

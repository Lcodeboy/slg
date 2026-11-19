package com.slg.game.application.march;

import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.GameException;
import com.slg.game.domain.march.MarchIdentity;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.util.*;
import java.util.concurrent.CompletionStage;

/**
 * 只读行军端口组合：规划只找资格，正式读取才提供裁决输入。三类任务、行军、矿点及战斗
 * 缺失证明来自同一短快照；不在回调中保留连接，不缓存世界对象，也不把读库失败解释为缺行。
 */
final class MarchSnapshotReader {
    private final RealmIdentity realm;
    private final RealmStateReader states;
    private final BoundedReadExecutor reads;
    private final ReadBudget budget;

    MarchSnapshotReader(RealmIdentity realm, RealmStateReader states, BoundedReadExecutor reads, ReadBudget budget) {
        this.realm = Objects.requireNonNull(realm); this.states = Objects.requireNonNull(states);
        this.reads = Objects.requireNonNull(reads); this.budget = Objects.requireNonNull(budget);
    }

    CompletionStage<MarchPlan> plan(long account, String id) {
        return reads.readSnapshot(states, realm, budget, view -> {
            var row = view.find(new ObjectKey.March(id)).filter(value -> owned(value, account)).orElse(null);
            return plan(id, row);
        }).result();
    }

    CompletionStage<MarchState> get(long account, String id) {
        return reads.readSnapshot(states, realm, budget, view -> {
            var row = view.find(new ObjectKey.March(id)).filter(value -> owned(value, account));
            GameException.require(row.isPresent(), "MARCH_NOT_FOUND", "March not found");
            return (MarchState) row.orElseThrow().state();
        }).result();
    }

    CompletionStage<RecallSnapshot> recall(long account, String id, MarchPlan plan) {
        return reads.readSnapshot(states, realm, budget, view -> {
            var stored = view.find(new ObjectKey.March(id));
            var row = stored.filter(value -> owned(value, account)).orElse(null);
            var tasks = new LinkedHashMap<String, VersionedState>();
            var versions = new LinkedHashMap<ObjectKey, Long>();
            versions.put(new ObjectKey.March(id), stored.map(VersionedState::version).orElse(0L));
            // 外部账号的行军与缺失一样处理，不继续读取它的子记录或暴露版本。
            if (row == null) return new RecallSnapshot(view.highSequence(), null, tasks, Optional.empty(), versions);
            for (String taskId : MarchIdentity.taskIds(id)) {
                var key = new ObjectKey.Task(taskId);
                var task = view.find(key);
                task.ifPresent(value -> tasks.put(taskId, value));
                versions.put(key, task.map(VersionedState::version).orElse(0L));
            }
            Optional<VersionedState> site = Optional.empty();
            if (plan.siteX() != null) {
                var key = new ObjectKey.GatherSite(plan.siteX(), plan.siteY());
                site = view.find(key);
                versions.put(key, site.map(VersionedState::version).orElse(0L));
            }
            var battleKey = new ObjectKey.Battle(id);
            versions.put(battleKey, view.find(battleKey).map(VersionedState::version).orElse(0L));
            return new RecallSnapshot(view.highSequence(), row, tasks, site, versions);
        }).result();
    }

    static MarchPlan plan(String id, VersionedState row) {
        if (row == null) return new MarchPlan(id, 0, null, null);
        var march = (MarchState) row.state();
        boolean mine = march.mission() == MarchState.Mission.GATHER &&
                (march.status() == MarchState.Status.OUTBOUND || march.status() == MarchState.Status.GATHERING);
        return new MarchPlan(id, row.version(), mine ? march.targetX() : null, mine ? march.targetY() : null);
    }

    private static boolean owned(VersionedState row, long account) {
        if (!(row.state() instanceof MarchState march)) throw new IllegalArgumentException("Expected march state");
        return PlayerIdentity.accountOf(march.playerId()) == account;
    }

    record RecallSnapshot(long highSequence, VersionedState march, Map<String, VersionedState> tasks,
                          Optional<VersionedState> site, Map<ObjectKey, Long> versions) {
        RecallSnapshot { tasks = Map.copyOf(tasks); versions = Map.copyOf(versions); }
    }
}

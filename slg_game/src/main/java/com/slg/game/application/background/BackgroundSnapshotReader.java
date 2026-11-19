package com.slg.game.application.background;

import com.slg.game.application.march.MarchPlan;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.GameException;
import com.slg.game.domain.march.MarchIdentity;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.util.*;
import java.util.concurrent.CompletionStage;

/**
 * 背景用例的短一致快照。扫描候选不是当前状态；正式读取重新取得任务/战斗和关联世界根，
 * 缺失使用 Optional/版本0表达，查询失败整份抛出。连接和快照不跨越异步阶段。
 */
final class BackgroundSnapshotReader {
    private final RealmIdentity realm;
    private final RealmStateReader states;
    private final BoundedReadExecutor reads;
    private final ReadBudget budget;

    BackgroundSnapshotReader(RealmIdentity realm, RealmStateReader states, BoundedReadExecutor reads, ReadBudget budget) {
        this.realm = realm; this.states = states; this.reads = reads; this.budget = budget;
    }

    CompletionStage<MarchPlan> plan(TaskState task) {
        return reads.readSnapshot(states, realm, budget, view -> BackgroundScopes.plan(task.marchId(),
                view.find(new ObjectKey.March(task.marchId())).orElse(null))).result();
    }

    CompletionStage<TaskSnapshot> task(BackgroundInvocation input) {
        var candidate = (TaskState) input.candidate().state();
        return reads.readSnapshot(states, realm, budget, view -> {
            var tasks = new LinkedHashMap<String, VersionedState>();
            var versions = new LinkedHashMap<ObjectKey, Long>();
            var ids = new LinkedHashSet<>(MarchIdentity.taskIds(candidate.marchId())); ids.add(candidate.taskId());
            for (String id : ids) {
                var key = new ObjectKey.Task(id); var row = view.find(key);
                row.ifPresent(value -> tasks.put(id, value)); versions.put(key, row.map(VersionedState::version).orElse(0L));
            }
            var marchKey = new ObjectKey.March(candidate.marchId()); var march = view.find(marchKey);
            versions.put(marchKey, march.map(VersionedState::version).orElse(0L));
            Optional<VersionedState> site = Optional.empty();
            if (input.plan().siteX() != null) {
                var key = new ObjectKey.GatherSite(input.plan().siteX(), input.plan().siteY()); site = view.find(key);
                versions.put(key, site.map(VersionedState::version).orElse(0L));
            }
            var battleKey = new ObjectKey.Battle(candidate.marchId()); var battle = view.find(battleKey);
            versions.put(battleKey, battle.map(VersionedState::version).orElse(0L));
            return new TaskSnapshot(view.highSequence(), march.orElse(null), tasks, site, battle, versions);
        }).result();
    }

    CompletionStage<BattleSnapshot> battle(BattleState candidate) {
        return reads.readSnapshot(states, realm, budget, view -> new BattleSnapshot(view.highSequence(),
                view.find(candidate.key()).orElse(null), view.find(new ObjectKey.March(candidate.marchId())).orElse(null),
                view.find(new ObjectKey.Task(MarchIdentity.returning(candidate.marchId()))))).result();
    }

    CompletionStage<BattleState> report(long account, String battleId) {
        return reads.readSnapshot(states, realm, budget, view -> {
            var row = view.find(new ObjectKey.Battle(battleId));
            GameException.require(row.isPresent() && row.orElseThrow().state() instanceof BattleState battle
                    && PlayerIdentity.accountOf(battle.playerId()) == account, "BATTLE_NOT_FOUND", "Battle not found");
            return (BattleState) row.orElseThrow().state();
        }).result();
    }

    record TaskSnapshot(long highSequence, VersionedState march, Map<String, VersionedState> tasks,
                        Optional<VersionedState> site, Optional<VersionedState> battle, Map<ObjectKey, Long> versions) {
        TaskSnapshot { tasks = Map.copyOf(tasks); versions = Map.copyOf(versions); }
    }
    record BattleSnapshot(long highSequence, VersionedState battle, VersionedState march,
                          Optional<VersionedState> returnTask) { }
}

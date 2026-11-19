package com.slg.game.domain.march;

import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.*;

import static com.slg.game.domain.march.MarchRules.*;

/**
 * 召回与后台到期推进共用的纯草案推演器。召回补齐有限阶段；后台每次只推进一个已经到期的阶段，
 * 两种入口共享同一占矿、放矿和交付规则，不能因在线/离线入口不同产生两套经济结果。
 * 推演的中间阶段不向缓存发布；每个根最终只写一次，最终 PENDING 任务与矿点占用引用最终
 * 行军版本，不能把内存推演次数当成已提交版本数。输入损坏直接失败并保留数据库现场。
 */
final class MarchEvolution {
    private final PlayerSnapshot player;
    private final VersionedState originalMarch;
    private final Map<String, VersionedState> tasks;
    private final LinkedHashMap<ObjectKey, Long> reads;
    private final LinkedHashMap<ObjectKey, ObjectChange> changes = new LinkedHashMap<>();
    private final List<String> phases = new ArrayList<>();
    private final long finalVersion;
    private final long now;
    private final long siteVersion;
    private GatherSiteState site;
    private MarchState march;
    private PendingBattle pendingBattle;

    private MarchEvolution(PlayerSnapshot player, VersionedState originalMarch, Map<String, VersionedState> tasks,
                        Optional<VersionedState> storedSite, long now) {
        this.player = player; this.originalMarch = originalMarch; this.tasks = Map.copyOf(tasks); this.now = now;
        this.march = (MarchState) originalMarch.state(); finalVersion = Math.incrementExact(originalMarch.version());
        reads = profileDependency(player); reads.put(march.key(), originalMarch.version());
        for (String id : MarchIdentity.taskIds(march.marchId())) {
            var row = tasks.get(id); reads.put(new ObjectKey.Task(id), row == null ? 0L : row.version());
            if (row != null) {
                verify(row.state() instanceof TaskState, "March task row has another type");
                var task = (TaskState) row.state();
                verify(id.equals(task.taskId()) && march.marchId().equals(task.marchId())
                        && march.playerId().equals(task.playerId()), "March task identity differs");
            }
        }
        boolean touchesSite = march.mission() == MarchState.Mission.GATHER
                && (march.status() == MarchState.Status.OUTBOUND || march.status() == MarchState.Status.GATHERING);
        siteVersion = storedSite.map(VersionedState::version).orElse(0L);
        if (touchesSite) {
            verify(MarchMap.isGatherSite(march.targetX(), march.targetY()), "Gather march has unknown target");
            if (storedSite.isPresent()) {
                verify(storedSite.orElseThrow().state() instanceof GatherSiteState, "Gather site row has another type");
                site = (GatherSiteState) storedSite.orElseThrow().state();
                verify(site.x() == march.targetX() && site.y() == march.targetY(), "Gather site identity differs");
            } else site = new GatherSiteState(march.targetX(), march.targetY(), null, null, 0);
            reads.put(site.key(), siteVersion);
        } else verify(storedSite.isEmpty(), "Unexpected site supplied for this march stage");
    }

    static MarchTransition decide(PlayerSnapshot before, VersionedState storedMarch, Map<String, VersionedState> tasks,
                                   Optional<VersionedState> site, long now) {
        Objects.requireNonNull(before); Objects.requireNonNull(tasks); Objects.requireNonNull(site);
        if (now < 0) throw new IllegalArgumentException("Negative server time");
        var reads = profileDependency(before);
        if (before.profile().isEmpty()) return rejected(reads, "PLAYER_NOT_FOUND", "Create a character first");
        if (storedMarch == null) return rejected(reads, "MARCH_NOT_FOUND", "March not found");
        verify(storedMarch.state() instanceof MarchState, "March row has another type");
        var march = (MarchState) storedMarch.state();
        if (!march.playerId().equals(before.playerId())) return rejected(reads, "MARCH_NOT_FOUND", "March not found");
        var recall = new MarchEvolution(before, storedMarch, tasks, site, now);
        recall.validateStoredMarch();
        return recall.run();
    }

    static DueTransition advanceDue(PlayerSnapshot before, VersionedState candidate, VersionedState storedMarch,
                                    Map<String, VersionedState> tasks, Optional<VersionedState> site,
                                    Optional<VersionedState> battle, long now) {
        Objects.requireNonNull(before); Objects.requireNonNull(candidate); Objects.requireNonNull(tasks); Objects.requireNonNull(site);
        Objects.requireNonNull(battle);
        if (now < 0) throw new IllegalArgumentException("Negative server time");
        if (!(candidate.state() instanceof TaskState hint)) throw new IllegalArgumentException("Expected due task candidate");
        var dependencies = profileDependency(before);
        var current = tasks.get(hint.taskId());
        dependencies.put(hint.key(), current == null ? 0L : current.version());
        // 扫描版本与行军 generation 是两个不同的版本。旧扫描、已完成或尚未到期只跳过，
        // 不把竞争/墙钟回拨标为坏任务，也不为这次空操作制造持久事件。
        if (current == null || !current.equals(candidate) || hint.status() != TaskState.Status.PENDING || hint.dueAt() > now)
            return new DueTransition(DueTransition.Status.SKIPPED, List.of(), dependencies, List.of(), null);
        dependencies.put(new ObjectKey.March(hint.marchId()), storedMarch == null ? 0L : storedMarch.version());
        for (String id : MarchIdentity.taskIds(hint.marchId())) {
            var task = tasks.get(id); dependencies.put(new ObjectKey.Task(id), task == null ? 0L : task.version());
        }
        site.ifPresent(row -> dependencies.put(row.state().key(), row.version()));
        var battleKey = new ObjectKey.Battle(hint.marchId());
        dependencies.put(battleKey, battle.map(VersionedState::version).orElse(0L));
        try {
            verify(before.profile().isPresent() && before.playerId().equals(hint.playerId()), "Task player reservation missing");
            verify(storedMarch != null && storedMarch.state() instanceof MarchState, "Task march missing");
            var march = (MarchState) storedMarch.state();
            verify(hint.marchId().equals(march.marchId()) && hint.playerId().equals(march.playerId()), "Task march identity differs");
            var evolution = new MarchEvolution(before, storedMarch, tasks, site, now);
            evolution.validateStoredMarch();
            verify(hint.taskId().equals(march.taskId()), "Pending task is no longer the current march task");
            TaskState due = evolution.validateCurrentTask(storedMarch.version());
            verify(due.equals(hint), "Current march task differs from candidate");
            if (due.type() == TaskState.Type.MARCH_ARRIVE && march.mission() == MarchState.Mission.ATTACK_NPC) {
                evolution.reads.put(battleKey, battle.map(VersionedState::version).orElse(0L));
                verify(battle.isEmpty(), "NPC arrival already has a battle record");
            }
            evolution.finish(due, TaskState.Status.DONE);
            evolution.advance(due);
            return new DueTransition(DueTransition.Status.APPLIED, List.copyOf(evolution.changes.values()),
                    evolution.reads, evolution.phases, evolution.pendingBattle);
        } catch (MarchDataException | ArithmeticException broken) {
            // 只有当前候选仍匹配且关联权威数据坏时才隔离这个任务；丢弃所有局部资产/占用草案。
            String reason = broken.getMessage() == null ? "Invalid march task" : broken.getMessage();
            if (reason.length() > 2048) reason = reason.substring(0, 2048);
            var blocked = new TaskState(hint.taskId(), hint.playerId(), hint.marchId(), hint.type(),
                    TaskState.Status.BLOCKED, hint.generation(), hint.dueAt(), reason);
            return new DueTransition(DueTransition.Status.BLOCKED,
                    List.of(new ObjectChange(blocked.key(), current.version(), blocked)), dependencies,
                    List.of("MARCH_TASK_BLOCKED"), null);
        }
    }

    private static MarchTransition rejected(Map<ObjectKey, Long> reads, String code, String message) {
        return new MarchTransition(List.of(), reads, null, code, message, List.of(), null);
    }

    private void validateStoredMarch() {
        verify((march.mission() == MarchState.Mission.GATHER ? GATHER_RULES : NPC_MARCH_RULES).equals(march.rulesVersion()),
                "Unknown march rules");
        verify(march.tier() == 1 && coordinate(march.fromX()) && coordinate(march.fromY())
                && coordinate(march.targetX()) && coordinate(march.targetY()), "Invalid march route or tier");
        boolean ended = march.status() == MarchState.Status.COMPLETED;
        var resident = player.activeMarches().get(march.marchId());
        verify(ended ? resident == null : originalMarch.equals(resident), "Player active march differs from current march");
        boolean emptyAllowed = march.mission() == MarchState.Mission.ATTACK_NPC
                && (march.status() == MarchState.Status.RETURNING || ended);
        verify((emptyAllowed || !march.troops().isEmpty()) && march.troops().values().stream().allMatch(count -> count > 0),
                "Invalid march troop reservation");
        if (march.status() == MarchState.Status.OUTBOUND || march.status() == MarchState.Status.GATHERING)
            verify(march.cargo().isEmpty(), "Uncollected march already carries cargo");
        if (march.mission() == MarchState.Mission.GATHER)
            verify(march.cargo().keySet().stream().allMatch(type -> type == ResourceType.WOOD), "Unsupported gather cargo");
        if (!ended) for (String id : march.heroIds()) {
            var row = player.heroes().get(id);
            verify(row != null && row.state() instanceof HeroState hero && march.marchId().equals(hero.assignedMarchId()),
                    "March hero reservation differs");
        }
        if (march.taskId() != null) validateCurrentTask(originalMarch.version());
        if (march.status() == MarchState.Status.GATHERING) verifyHeld(originalMarch.version());
    }

    private MarchTransition run() {
        // 当前合法链最多 OUTBOUND -> GATHERING -> RETURNING -> COMPLETED，不允许坏数据造成无界追赶。
        int advanced = 0;
        while (march.taskId() != null && march.arriveAt() <= now) {
            if (++advanced > 3) throw new MarchDataException("March catch-up exceeded legal stages");
            TaskState current = validateCurrentTask(changes.containsKey(march.key()) ? finalVersion : originalMarch.version());
            finish(current, TaskState.Status.DONE);
            advance(current);
        }
        if (march.status() != MarchState.Status.OUTBOUND && march.status() != MarchState.Status.GATHERING) {
            String message = march.status() == MarchState.Status.RETURNING ? "This march is already returning" : "This march cannot be recalled";
            return result(null, "INVALID_MARCH_STATE", message);
        }
        TaskState current = validateCurrentTask(changes.containsKey(march.key()) ? finalVersion : originalMarch.version());
        finish(current, TaskState.Status.CANCELLED);
        double progress = Math.clamp((now - (double) march.departAt()) / Math.max(1d, march.arriveAt() - (double) march.departAt()), 0d, 1d);
        double x = march.fromX() + (march.targetX() - march.fromX()) * progress;
        double y = march.fromY() + (march.targetY() - march.fromY()) * progress;
        if (march.status() == MarchState.Status.GATHERING) release(current.generation());
        update(returning(march, x, y, now, Map.of()));
        createTask(task(march, TaskState.Type.MARCH_RETURN, finalVersion));
        phases.add("MARCH_RECALLED");
        return result(march, null, null);
    }

    private void advance(TaskState current) {
        long start = current.dueAt();
        switch (current.type()) {
            case MARCH_ARRIVE -> {
                if (march.mission() == MarchState.Mission.ATTACK_NPC) {
                    update(stage(march, MarchState.Status.BATTLE_PENDING, march.targetX(), march.targetY(), march.targetX(),
                            march.targetY(), start, start, null, Map.of()));
                    pendingBattle = battle(march, finalVersion); phases.add("MARCH_BATTLE_PREPARED");
                } else if (site.occupantMarchId() != null) {
                    verify(!site.occupantMarchId().equals(march.marchId()), "Outbound march already occupies its target");
                    update(returning(march, march.targetX(), march.targetY(), start, Map.of()));
                    createTask(task(march, TaskState.Type.MARCH_RETURN, finalVersion)); phases.add("MARCH_TARGET_OCCUPIED");
                } else {
                    update(stage(march, MarchState.Status.GATHERING, march.targetX(), march.targetY(), march.targetX(), march.targetY(),
                            start, Math.addExact(start, 3000L), MarchIdentity.gather(march.marchId()), Map.of()));
                    site = new GatherSiteState(site.x(), site.y(), march.playerId(), march.marchId(), finalVersion);
                    put(site, siteVersion); createTask(task(march, TaskState.Type.GATHER_DONE, finalVersion));
                    phases.add("MARCH_GATHER_STARTED");
                }
            }
            case GATHER_DONE -> {
                release(current.generation());
                long count = march.troops().values().stream().reduce(0L, Math::addExact);
                update(returning(march, march.targetX(), march.targetY(), start,
                        Map.of(ResourceType.WOOD, Math.multiplyExact(count, 5L))));
                createTask(task(march, TaskState.Type.MARCH_RETURN, finalVersion)); phases.add("MARCH_GATHER_FINISHED");
            }
            case MARCH_RETURN -> {
                deliver();
                update(stage(march, MarchState.Status.COMPLETED, march.fromX(), march.fromY(), MarchMap.homeX(), MarchMap.homeY(),
                        march.departAt(), start, null, march.cargo()));
                phases.add("MARCH_COMPLETED");
            }
        }
    }

    private TaskState validateCurrentTask(long generation) {
        var key = new ObjectKey.Task(march.taskId());
        var draft = changes.get(key);
        var row = tasks.get(march.taskId());
        TaskState task = draft != null ? (TaskState) draft.afterState() : row == null ? null : (TaskState) row.state();
        verify(task != null, "Missing current march task");
        TaskState.Type expected = switch (march.status()) {
            case OUTBOUND -> TaskState.Type.MARCH_ARRIVE;
            case GATHERING -> TaskState.Type.GATHER_DONE;
            case RETURNING -> TaskState.Type.MARCH_RETURN;
            default -> throw new MarchDataException("Untimed march has a current task");
        };
        String expectedId = switch (expected) {
            case MARCH_ARRIVE -> MarchIdentity.arrival(march.marchId());
            case GATHER_DONE -> MarchIdentity.gather(march.marchId());
            case MARCH_RETURN -> MarchIdentity.returning(march.marchId());
        };
        verify(task.type() == expected && expectedId.equals(task.taskId()) && task.status() == TaskState.Status.PENDING
                && task.generation() == generation && task.dueAt() == march.arriveAt(), "Current task disagrees with march stage or generation");
        return task;
    }

    private void finish(TaskState task, TaskState.Status status) {
        var original = tasks.get(task.taskId());
        put(new TaskState(task.taskId(), task.playerId(), task.marchId(), task.type(), status, task.generation(), task.dueAt(), null),
                original == null ? 0 : original.version());
    }

    private void createTask(TaskState task) {
        verify(!tasks.containsKey(task.taskId()) && !changes.containsKey(task.key()), "Unexpected successor march task exists");
        put(task, 0);
    }

    private void verifyHeld(long generation) {
        verify(site != null && march.marchId().equals(site.occupantMarchId()) && march.playerId().equals(site.occupantPlayerId())
                && generation == site.generation(), "Gather reservation differs from march and generation");
    }

    private void release(long generation) {
        verifyHeld(generation);
        site = new GatherSiteState(site.x(), site.y(), null, null, 0);
        // 同一决定中先占再放也只生成一个最终后像，保留空闲行及版本供下一位竞争者校验。
        put(site, siteVersion);
    }

    private void deliver() {
        for (var entry : march.troops().entrySet()) {
            var row = player.troops().get(entry.getKey());
            verify(row != null && row.state() instanceof TroopState, "Returning troop type missing from player");
            var troop = (TroopState) row.state(); reads.put(troop.key(), row.version());
            put(new TroopState(player.playerId(), troop.troopType(), Math.addExact(troop.available(), entry.getValue()),
                    troop.wounded(), troop.dead()), row.version());
        }
        for (var entry : march.cargo().entrySet()) {
            var row = player.resources().get(entry.getKey());
            var key = new ObjectKey.Resource(player.playerId(), entry.getKey());
            long amount = row == null ? 0 : ((ResourceState) row.state()).amount();
            long version = row == null ? 0 : row.version(); reads.put(key, version);
            put(new ResourceState(player.playerId(), entry.getKey(), Math.addExact(amount, entry.getValue())), version);
        }
        for (String id : march.heroIds()) {
            var row = player.heroes().get(id);
            verify(row != null && row.state() instanceof HeroState hero && march.marchId().equals(hero.assignedMarchId()),
                    "Returning hero reservation differs");
            reads.put(row.state().key(), row.version()); put(new HeroState(player.playerId(), id, null), row.version());
        }
    }

    private void update(MarchState next) { march = next; put(next, originalMarch.version()); }
    private void put(ObjectState state, long expectedVersion) { changes.put(state.key(), new ObjectChange(state.key(), expectedVersion, state)); }
    private MarchTransition result(MarchState response, String code, String message) {
        return new MarchTransition(List.copyOf(changes.values()), reads, response, code, message, phases, pendingBattle);
    }

    private static PendingBattle battle(MarchState march, long version) {
        List<TroopGroup> attackers = march.troops().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(entry -> new TroopGroup(entry.getKey().name(), entry.getKey(), march.tier(), entry.getValue())).toList();
        var attacker = new Army(march.playerId(), attackers, 1 + march.heroIds().size() * .01, 1.0);
        var defender = new Army("npc", List.of(new TroopGroup("npc-infantry", TroopType.INFANTRY, 1, 60),
                new TroopGroup("npc-lancer", TroopType.LANCER, 1, 40), new TroopGroup("npc-marksman", TroopType.MARKSMAN, 1, 40)));
        var input = new BattleInput(march.marchId(), attacker, defender, march.marchId().hashCode(), 100);
        return new PendingBattle(march.marchId(), march.playerId(), march.heroIds(), version, ExpeditionBattleEngine.RULES_VERSION, input);
    }
}

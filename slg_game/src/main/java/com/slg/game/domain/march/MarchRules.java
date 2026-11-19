package com.slg.game.domain.march;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.*;

import static com.slg.game.domain.GameException.require;

/**
 * 纯行军用例入口。调用者先取得账号及显式世界对象资格，并提供已提交快照和一次采样时间；
 * 返回草案本身不证明提交成功。名字、时间、SQL、回执编码和缓存发布均不在领域规则中处理。
 */
public final class MarchRules {
    public static final String GATHER_RULES = "realm-gather-v2";
    public static final String NPC_MARCH_RULES = "realm-march-v1";
    private static final String STARTER_HERO = "sergey";
    private MarchRules() { }

    /** 出征仅预留城内兵力和英雄；矿点在到达时争用，因此此决定不创建或修改矿点。 */
    public static MarchTransition start(PlayerSnapshot before, String marchId, StartMarchOrder order, long now) {
        Objects.requireNonNull(before); Objects.requireNonNull(order);
        new ObjectKey.March(marchId);
        if (now < 0) throw new IllegalArgumentException("Negative server time");
        var reads = profileDependency(before);
        try {
            require(order.targetX() != null && coordinate(order.targetX()), "INVALID_ARGUMENT", "targetX must be in 0..1199");
            require(order.targetY() != null && coordinate(order.targetY()), "INVALID_ARGUMENT", "targetY must be in 0..1199");
            require(order.tier() == null || order.tier() == 1, "INVALID_ARGUMENT", "Only T1 is supported");
            var mission = mission(order.mission());
            int x = order.targetX(), y = order.targetY();
            require(mission == MarchState.Mission.GATHER ? MarchMap.isGatherSite(x, y) : MarchMap.isNpc(x, y),
                    "INVALID_TARGET", "Target does not match the mission");
            require(before.profile().isPresent(), "PLAYER_NOT_FOUND", "Create a character first");
            // 行军集合上限由共同账号资格保护；不能把资料版本误当集合版本，也不扩大到其他行军根。
            require(before.activeMarches().size() < 3, "MARCH_LIMIT", "All march queues are occupied");
            var selected = troops(order.troops());
            var changes = new ArrayList<ObjectChange>();
            for (var entry : selected.entrySet()) {
                var row = before.troops().get(entry.getKey());
                verify(row != null && row.state() instanceof TroopState, "Required troop row missing");
                var troop = (TroopState) row.state();
                reads.put(troop.key(), row.version());
                require(troop.available() >= entry.getValue(), "INSUFFICIENT_TROOPS", "Not enough available troops");
                changes.add(new ObjectChange(troop.key(), row.version(), new TroopState(before.playerId(), troop.troopType(),
                        troop.available() - entry.getValue(), troop.wounded(), troop.dead())));
            }
            List<String> heroes = order.heroIds() == null ? List.of(STARTER_HERO) : order.heroIds();
            require(heroes.size() <= 1, "INVALID_ARGUMENT", "Select at most one hero");
            require(heroes.isEmpty() || STARTER_HERO.equals(heroes.getFirst()), "HERO_NOT_FOUND", "Hero not owned");
            for (String id : heroes) {
                var row = before.heroes().get(id);
                verify(row != null && row.state() instanceof HeroState, "Required hero row missing");
                var hero = (HeroState) row.state(); reads.put(hero.key(), row.version());
                require(hero.assignedMarchId() == null, "HERO_BUSY", "Hero is already assigned to a march");
                changes.add(new ObjectChange(hero.key(), row.version(), new HeroState(before.playerId(), id, marchId)));
            }
            verify(!before.activeMarches().containsKey(marchId), "Derived march already exists without a receipt");
            String taskId = MarchIdentity.arrival(marchId);
            var march = new MarchState(marchId, before.playerId(), MarchState.Status.OUTBOUND, mission,
                    MarchMap.homeX(), MarchMap.homeY(), x, y, now, arrivalAt(MarchMap.homeX(), MarchMap.homeY(), x, y, now),
                    1, selected, heroes, taskId, mission == MarchState.Mission.GATHER ? GATHER_RULES : NPC_MARCH_RULES, Map.of());
            var task = task(march, TaskState.Type.MARCH_ARRIVE, 1);
            changes.add(new ObjectChange(march.key(), 0, march)); changes.add(new ObjectChange(task.key(), 0, task));
            reads.put(march.key(), 0L); reads.put(task.key(), 0L);
            return new MarchTransition(changes, reads, march, null, null, List.of("MARCH_STARTED"), null);
        } catch (GameException rejected) {
            // 已计算的局部扣兵草案尚未提交；拒绝不能携带这些半成品变化。
            return new MarchTransition(List.of(), reads, null, rejected.code(), rejected.getMessage(), List.of(), null);
        }
    }

    /** 已到期阶段在同一决定内推演，由共享推演器明确控制最终版本、任务和矿点的一致性。 */
    public static MarchTransition recall(PlayerSnapshot before, VersionedState march, Map<String, VersionedState> tasks,
                                         Optional<VersionedState> site, long now) {
        return MarchEvolution.decide(before, march, tasks, site, now);
    }

    /** 后台只推进一个阶段；下一待办仍持久保存，由有界扫描继续发现，不在一个 handler 内无限追赶。 */
    public static DueTransition advance(PlayerSnapshot before, VersionedState candidateTask, VersionedState march,
                                         Map<String, VersionedState> currentTasks, Optional<VersionedState> site, long now) {
        return advance(before, candidateTask, march, currentTasks, site, Optional.empty(), now);
    }

    /** NPC 到达的 battle 缺失也属于当前快照依赖；碰撞必须隔离原待办，不能不断重试或覆盖旧战报。 */
    public static DueTransition advance(PlayerSnapshot before, VersionedState candidateTask, VersionedState march,
                                         Map<String, VersionedState> currentTasks, Optional<VersionedState> site,
                                         Optional<VersionedState> battle, long now) {
        return MarchEvolution.advanceDue(before, candidateTask, march, currentTasks, site, battle, now);
    }

    static LinkedHashMap<ObjectKey, Long> profileDependency(PlayerSnapshot before) {
        var reads = new LinkedHashMap<ObjectKey, Long>();
        reads.put(new ObjectKey.Player(before.playerId()), before.profile().map(VersionedState::version).orElse(0L));
        return reads;
    }

    private static MarchState.Mission mission(Integer code) {
        if (code == null) return MarchState.Mission.GATHER;
        try { return MarchState.Mission.fromCode(code); }
        catch (IllegalArgumentException unsupported) { throw new GameException("INVALID_ARGUMENT", "Unknown march mission"); }
    }

    private static Map<TroopType, Long> troops(List<StartMarchOrder.TroopSelection> input) {
        var result = new EnumMap<TroopType, Long>(TroopType.class);
        for (var group : input) {
            TroopType type;
            try { type = TroopType.fromCode(group.type()); }
            catch (IllegalArgumentException unsupported) { throw new GameException("INVALID_ARGUMENT", "Unknown troop type"); }
            require(group.count() >= 0 && group.count() <= 100_000, "INVALID_ARGUMENT", "Troop count must be in 0..100000");
            require(result.putIfAbsent(type, group.count()) == null, "INVALID_ARGUMENT", "Duplicate troop type");
        }
        result.values().removeIf(count -> count == 0);
        require(!result.isEmpty(), "INVALID_ARGUMENT", "Select at least one troop");
        return Map.copyOf(result);
    }

    static boolean coordinate(double value) { return Double.isFinite(value) && value >= 0 && value <= 1199; }
    static long arrivalAt(double x, double y, int targetX, int targetY, long start) {
        long seconds = Math.max(3, (long) Math.ceil(Math.hypot(targetX - x, targetY - y) / 100d));
        return Math.addExact(start, Math.multiplyExact(seconds, 1000L));
    }

    static MarchState stage(MarchState source, MarchState.Status state, double x, double y, int targetX, int targetY,
                            long start, long end, String taskId, Map<ResourceType, Long> cargo) {
        return new MarchState(source.marchId(), source.playerId(), state, source.mission(), x, y, targetX, targetY,
                start, end, source.tier(), source.troops(), source.heroIds(), taskId, source.rulesVersion(), cargo);
    }

    static MarchState returning(MarchState source, double x, double y, long start, Map<ResourceType, Long> cargo) {
        return stage(source, MarchState.Status.RETURNING, x, y, MarchMap.homeX(), MarchMap.homeY(), start,
                arrivalAt(x, y, MarchMap.homeX(), MarchMap.homeY(), start), MarchIdentity.returning(source.marchId()), cargo);
    }

    static TaskState task(MarchState march, TaskState.Type type, long generation) {
        return new TaskState(march.taskId(), march.playerId(), march.marchId(), type, TaskState.Status.PENDING,
                generation, march.arriveAt(), null);
    }

    static void verify(boolean valid, String message) { if (!valid) throw new MarchDataException(message); }
}

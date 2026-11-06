package com.slg.game.realm;

import com.slg.core.battle.Army;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.ExpeditionBattleEngine;
import com.slg.core.battle.TroopGroup;
import com.slg.core.battle.TroopType;
import com.slg.core.protocol.Json;
import com.slg.game.domain.GameException;
import com.slg.game.realm.ActivePlayer.Hero;
import com.slg.game.realm.ActivePlayer.Player;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Decision;
import com.slg.game.storage.RealmJournal.DueTask;
import com.slg.game.storage.RealmJournal.Value;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 在玩家及相关矿点的执行资格内推进状态。扫描结果是提示，任务、行军和矿点以卷内当前版本为准。 */
final class MarchTasks {
    private static final System.Logger LOG = System.getLogger(MarchTasks.class.getName());
    private final RealmJournal journal;
    private final Clock clock;

    MarchTasks(RealmJournal journal, Clock clock) throws IOException {
        this.journal = journal;
        this.clock = clock;
        GatherSite.verifyActiveReservations(journal);
    }

    record March(String marchId, String playerId, String state, String mission, double fromX, double fromY,
                 int targetX, int targetY, long departAt, long arriveAt, int tier, Map<String,Long> troops,
                 List<String> heroIds, String taskId, String rulesVersion, Map<String,Long> cargo) {
        March {
            troops = Map.copyOf(troops);
            heroIds = List.copyOf(heroIds);
            cargo = Map.copyOf(cargo);
        }
        March withSurvivors(Map<String, Long> survivors) {
            return new March(marchId, playerId, state, mission, fromX, fromY, targetX, targetY,
                    departAt, arriveAt, tier, survivors, heroIds, taskId, rulesVersion, cargo);
        }
        March stage(String state, double x, double y, int destX, int destY, long start, long end,
                    String nextTask, Map<String,Long> cargo) {
            return new March(marchId, playerId, state, mission, x, y, destX, destY, start, end,
                    tier, troops, heroIds, nextTask, rulesVersion, cargo);
        }
        /** 进入采集的行军和唯一矿点占用在同一批次持久化。 */
        March gather(long start) {
            return new March(marchId, playerId, "GATHERING", mission, targetX, targetY, targetX, targetY,
                    start, Math.addExact(start, 3000L), tier, troops, heroIds, "gather-" + marchId,
                    GatherSite.RULES_VERSION, Map.of());
        }
    }

    record Task(String taskId, String playerId, String marchId, String type, String state,
                int generation, long dueAt, String reason) {
        DueTask hint() { return new DueTask(taskId, playerId, marchId, type, generation, dueAt); }
        Task finish(String state, String reason) {
            return new Task(taskId, playerId, marchId, type, state, generation, dueAt, reason);
        }
    }

    record Recall(March march, List<Change> changes) {}
    /** relatedObject 可是本次到达产生的战斗或本次采集变更的矿点，与任务和行军同批提交。 */
    private record Step(String event, March march, Task next, Player player, Change relatedObject) {}
    static final class BrokenTask extends RuntimeException {
        BrokenTask(String message) { super(message); }
    }

    boolean applyDue(ActivePlayer active, DueTask hint) throws IOException {
        if (!active.playerId().equals(hint.playerId()) || hint.dueAt() > clock.millis()) return false;
        if (!active.valid()) throw new IOException("Stale player while dispatching march task");
        try {
            Value stored = journal.read("task:" + hint.taskId());
            if (stored == null) return false;
            Task task = Json.MAPPER.readValue(stored.json(), Task.class);
            if (!"PENDING".equals(task.state()) || !task.hint().equals(hint)) return false;
            var snapshot = active.snapshot();
            Step step;
            Value marchValue = journal.read("march:" + task.marchId());
            try {
                verify(marchValue != null, "March missing");
                March march = decodeMarch(marchValue);
                verify(active.playerId().equals(march.playerId()) && task.marchId().equals(march.marchId()), "March owner/ID mismatch");
                verify(marchValue.version() == task.generation() && task.taskId().equals(march.taskId())
                        && task.dueAt() == march.arriveAt(), "March task generation or due time mismatch");
                verify(knownRules(march), "Unknown march rules");
                validateReservation(snapshot.player(), march);
                step = advance(task, march, snapshot.player());
                if (step.next() != null) verify(journal.read("task:" + step.next().taskId()) == null,
                        "Unexpected existing successor task");
            } catch (BrokenTask | ArithmeticException | IllegalArgumentException broken) {
                String reason = broken.getMessage() == null ? "Invalid march task" : broken.getMessage();
                var changes = List.of(taskChange(task.finish("BLOCKED", reason), stored.version()));
                boolean applied = commit(active, hint, "MARCH_TASK_BLOCKED", Map.of("reason", reason), changes, null);
                if (applied) LOG.log(System.Logger.Level.WARNING, "March task blocked: {0}, march {1}: {2}",
                        hint.taskId(), hint.marchId(), reason);
                return applied;
            }
            var changes = new ArrayList<Change>();
            changes.add(taskChange(task.finish("DONE", null), stored.version()));
            changes.add(new Change("march:" + task.marchId(), marchValue.version(), Json.write(step.march())));
            if (step.next() != null) changes.add(taskChange(step.next(), 0));
            if (step.relatedObject() != null) changes.add(step.relatedObject());
            if (step.player() != null) changes.add(new Change("player:" + active.playerId(), snapshot.version(), Json.write(step.player())));
            return commit(active, hint, step.event(), step.march(), changes, step.player());
        } catch (IOException | RuntimeException | Error e) {
            // 未知写、版本竞争或发布失败，下一轮必须重读原卷。
            active.invalidate();
            throw e;
        }
    }

    private boolean commit(ActivePlayer active, DueTask hint, String event, Object detail, List<Change> changes, Player player)
            throws IOException {
        String request = RealmMarchService.hash(hint.taskId() + "." + hint.generation());
        Decision decision = new Decision("timer:" + hint.playerId(), request, RealmMarchService.hash(Json.write(hint)),
                event, Json.write(Map.of("task", hint, "result", detail)), Json.write(Map.of("event", event)), changes);
        var result = journal.commitWithOutcome(decision);
        if (result.applied()) active.committed(result.receipt().sequence(), active.snapshot().version(), player);
        else active.replayed(result.receipt().sequence());
        return result.applied();
    }

    private Step advance(Task task, March march, Player player) throws IOException {
        long start = task.dueAt();
        int nextGeneration = Math.incrementExact(task.generation());
        return switch (task.type()) {
            case "MARCH_ARRIVE" -> {
                verify("OUTBOUND".equals(march.state()), "Arrival requires outbound march");
                if ("ATTACK_NPC".equals(march.mission())) {
                    verify(journal.read("battle:" + march.marchId()) == null, "Unexpected existing battle");
                    var pending = march.stage("BATTLE_PENDING", march.targetX(), march.targetY(), march.targetX(),
                            march.targetY(), start, start, null, Map.of());
                    yield new Step("MARCH_BATTLE_PREPARED", pending, null, null, battle(march, nextGeneration));
                }
                verify("GATHER".equals(march.mission()), "Unknown arrival mission");
                var site = GatherSite.read(journal, march.targetX(), march.targetY());
                if (site.occupied()) {
                    // 两位玩家都可以派兵。只有目标执行队列中先成功提交占用的一支进入采集；
                    // 后来的这支同批完成到达并建返城任务，兵力仍随队，直到到家才释放。
                    March returning = startReturn(march, march.targetX(), march.targetY(), start, Map.of());
                    yield new Step("MARCH_TARGET_OCCUPIED", returning, nextTask(returning, "MARCH_RETURN", nextGeneration),
                            null, null);
                }
                var gather = march.gather(start);
                yield new Step("MARCH_GATHER_STARTED", gather, nextTask(gather, "GATHER_DONE", nextGeneration),
                        null, site.claim(gather, nextGeneration));
            }
            case "GATHER_DONE" -> {
                verify("GATHERING".equals(march.state()) && "GATHER".equals(march.mission()), "Invalid gathering stage");
                long count = march.troops().values().stream().reduce(0L, Math::addExact);
                March returning = startReturn(march, march.targetX(), march.targetY(), start,
                        Map.of("WOOD", Math.multiplyExact(count, 5L)));
                yield new Step("MARCH_GATHER_FINISHED", returning, nextTask(returning, "MARCH_RETURN", nextGeneration),
                        null, releaseMine(march, task.generation()));
            }
            case "MARCH_RETURN" -> {
                verify("RETURNING".equals(march.state()), "Return requires returning march");
                March completed = march.stage("COMPLETED", march.fromX(), march.fromY(), 0, 0,
                        march.departAt(), start, null, march.cargo());
                yield new Step("MARCH_COMPLETED", completed, null, deliver(player, march), null);
            }
            default -> throw new BrokenTask("Unknown march task type");
        };
    }

    private static boolean knownRules(March march) {
        return switch (march.mission()) {
            case "GATHER" -> GatherSite.RULES_VERSION.equals(march.rulesVersion());
            case "ATTACK_NPC" -> RealmMarchService.RULES_VERSION.equals(march.rulesVersion());
            default -> false;
        };
    }

    private Change releaseMine(March march, int generation) throws IOException {
        // 采集完成和召回都须持有同一矿的当代占用。缺少归属时保留现场，不能放矿或交付货物。
        return GatherSite.read(journal, march.targetX(), march.targetY()).release(march, generation);
    }

    /** 成功后才发布的返城草案；部队、货物、英雄和队列槽位必须一起交还。 */
    private static Player deliver(Player player, March march) {
        Map<String, Long> troops = new HashMap<>(player.troops());
        march.troops().forEach((type, count) -> troops.merge(type, count, Math::addExact));
        Map<String, Long> resources = new HashMap<>(player.resources());
        march.cargo().forEach((type, amount) -> {
            boolean valid = "WOOD".equals(type) || ("ATTACK_NPC".equals(march.mission())
                    && ("FOOD".equals(type) || "GEMS".equals(type)));
            verify(valid && amount >= 0, "Invalid cargo");
            resources.merge(type, amount, Math::addExact);
        });
        List<Hero> heroes = player.heroes().stream().map(hero -> march.marchId().equals(hero.assignedMarch())
                ? new Hero(hero.heroId(), null) : hero).toList();
        var marches = new ArrayList<>(player.activeMarchIds());
        marches.remove(march.marchId());
        return player.withArmy(troops, heroes, marches, resources,
                player.wounded(), player.dead());
    }

    private static Change battle(March march, long marchVersion) {
        List<TroopGroup> attackers = march.troops().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(e -> new TroopGroup(e.getKey(), TroopType.valueOf(e.getKey()), march.tier(), e.getValue())).toList();
        var attacker = new Army(march.playerId(), attackers, 1 + march.heroIds().size() * .01, 1.0);
        var defender = new Army("npc", List.of(new TroopGroup("npc-infantry", TroopType.INFANTRY, 1, 60),
                new TroopGroup("npc-lancer", TroopType.LANCER, 1, 40),
                new TroopGroup("npc-marksman", TroopType.MARKSMAN, 1, 40)));
        var input = new BattleInput(march.marchId(), attacker, defender, march.marchId().hashCode(), 100);
        return new Change("battle:" + march.marchId(), 0, Json.write(new NpcBattles.Job("PENDING",
                march.marchId(), march.marchId(), march.playerId(), march.heroIds(), input,
                RealmMarchService.hash(Json.write(input)), ExpeditionBattleEngine.RULES_VERSION, marchVersion, null)));
    }

    /** 若扫描器晚到，先补做在本请求采样时间已经发生的阶段，然后裁决召回。 */
    Recall recall(ActivePlayer active, String marchId) throws IOException {
        long now = clock.millis();
        for (int n = 0; n < 4; n++) {
            Value mv = journal.read("march:" + marchId);
            GameException.require(mv != null, "MARCH_NOT_FOUND", "March not found");
            March march = Json.MAPPER.readValue(mv.json(), March.class);
            GameException.require(active.playerId().equals(march.playerId()), "MARCH_NOT_FOUND", "March not found");
            if (!knownRules(march)) throw new IOException("Unknown march rules during recall");
            boolean canRecall = "OUTBOUND".equals(march.state()) || "GATHERING".equals(march.state());
            GameException.require(canRecall || "RETURNING".equals(march.state()), "INVALID_MARCH_STATE", "This march cannot be recalled");
            Value tv = journal.read("task:" + march.taskId());
            if (tv == null) throw new IOException("Missing current march task");
            Task task = Json.MAPPER.readValue(tv.json(), Task.class);
            GameException.require("PENDING".equals(task.state()), "INVALID_MARCH_STATE", "March task is not pending");
            if (!Objects.equals(task.marchId(), marchId) || !Objects.equals(task.playerId(), active.playerId())
                    || !Objects.equals(task.taskId(), march.taskId()) || task.generation() != mv.version()
                    || task.dueAt() != march.arriveAt()) throw new IOException("Inconsistent march and task");
            if (task.dueAt() <= now) {
                if (!applyDue(active, task.hint()) || !active.valid()) throw new IOException("March changed during recall");
                continue;
            }
            GameException.require(canRecall, "INVALID_MARCH_STATE", "This march is already returning");
            double progress = Math.clamp((now - (double) march.departAt()) /
                    Math.max(1d, march.arriveAt() - (double) march.departAt()), 0d, 1d);
            double x = march.fromX() + (march.targetX() - march.fromX()) * progress;
            double y = march.fromY() + (march.targetY() - march.fromY()) * progress;
            March returning = startReturn(march, x, y, now, Map.of());
            Task next = nextTask(returning, "MARCH_RETURN", Math.toIntExact(Math.incrementExact(mv.version())));
            var changes = new ArrayList<>(List.of(taskChange(task.finish("CANCELLED", null), tv.version()),
                    new Change("march:" + marchId, mv.version(), Json.write(returning)), taskChange(next, 0)));
            if ("GATHERING".equals(march.state())) {
                try {
                    changes.add(releaseMine(march, task.generation()));
                } catch (BrokenTask inconsistent) {
                    // 发现占用不符时保留现场，不能为召回写出错误的矿点释放或业务回执。
                    throw new IOException("Cannot recall march with inconsistent gather reservation", inconsistent);
                }
            }
            return new Recall(returning, changes);
        }
        throw new IOException("March catch-up exceeded legal number of stages");
    }

    static March startReturn(March m, double x, double y, long start, Map<String,Long> cargo) {
        long seconds = Math.max(3, (long) Math.ceil(Math.hypot(x, y) / 100d));
        return m.stage("RETURNING", x, y, 0, 0, start, Math.addExact(start, Math.multiplyExact(seconds, 1000L)),
                "return-" + m.marchId(), cargo);
    }

    static Task nextTask(March m, String type, int generation) {
        return new Task(m.taskId(), m.playerId(), m.marchId(), type, "PENDING", generation, m.arriveAt(), null);
    }

    static Change taskChange(Task task, long expected) {
        return new Change("task:" + task.taskId(), expected, Json.write(task));
    }

    private static March decodeMarch(Value value) {
        try {
            March march = Json.MAPPER.readValue(value.json(), March.class);
            verify(march != null, "Malformed stored march");
            return march;
        }
        catch (IOException e) { throw new BrokenTask("Malformed stored march"); }
    }

    static void validateReservation(Player player, March m) {
        verify(player != null && Collections.frequency(player.activeMarchIds(), m.marchId()) == 1,
                "Player reservation missing");
        verify(m.tier() == 1 && ("GATHER".equals(m.mission()) || "ATTACK_NPC".equals(m.mission())),
                "Unknown march tier or mission");
        verify(m.fromX() >= 0 && m.fromX() <= 1199 && m.fromY() >= 0 && m.fromY() <= 1199
                && m.targetX() >= 0 && m.targetX() <= 1199 && m.targetY() >= 0 && m.targetY() <= 1199
                && m.departAt() <= m.arriveAt(), "Invalid march route");
        // 战败可能全军覆没，英雄和槽位仍须经过返城阶段统一释放。
        boolean emptyReturn = "ATTACK_NPC".equals(m.mission()) && "RETURNING".equals(m.state());
        verify((emptyReturn || !m.troops().isEmpty()) && m.troops().values().stream().allMatch(n -> n > 0),
                "Invalid reserved troops");
        for (String type : m.troops().keySet()) TroopType.valueOf(type);
        List<String> reservedHeroes = player.heroes().stream().filter(h -> m.marchId().equals(h.assignedMarch()))
                .map(Hero::heroId).sorted().toList();
        verify(reservedHeroes.equals(m.heroIds().stream().sorted().toList()), "Hero reservation mismatch");
    }

    private static void verify(boolean valid, String reason) {
        if (!valid) throw new BrokenTask(reason);
    }
}

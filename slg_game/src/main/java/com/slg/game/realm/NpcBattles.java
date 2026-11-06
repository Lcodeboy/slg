package com.slg.game.realm;

import com.slg.core.battle.Army;
import com.slg.core.battle.ArmyResult;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.core.battle.ExpeditionBattleEngine;
import com.slg.core.battle.GroupResult;
import com.slg.core.battle.Outcome;
import com.slg.core.battle.TroopGroup;
import com.slg.core.protocol.Json;
import com.slg.game.domain.GameException;
import com.slg.game.realm.ActivePlayer.Player;
import com.slg.game.realm.MarchTasks.March;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Decision;
import com.slg.game.storage.RealmJournal.Value;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** GAME 掌握发奖和状态推进。Worker 只提供战损建议，无权限修改玩家或自行释放行军。 */
final class NpcBattles {
    private static final Map<String, Long> WIN_CARGO = Map.of("WOOD", 500L, "FOOD", 500L, "GEMS", 10L);
    private final RealmJournal journal;
    private final Clock clock;

    NpcBattles(RealmJournal journal, Clock clock) {
        this.journal = journal;
        this.clock = clock;
    }

    /** 到达批次固定输入与行军预留版本。缺失或不匹配的版本不能派发或结算。 */
    record Job(String state, String battleId, String marchId, String playerId, List<String> heroIds,
               BattleInput input, String inputHash, String rulesVersion, long marchVersion, BattleResult result) {
        Job settled(BattleResult result) {
            return new Job("SETTLED", battleId, marchId, playerId, heroIds, input, inputHash,
                    rulesVersion, marchVersion, result);
        }
    }

    record Report(String battleId, String marchId, String state, String rulesVersion,
                  String inputHash, BattleResult result) {}

    Report report(String playerId, String battleId) throws IOException {
        var value = journal.read("battle:" + battleId);
        GameException.require(value != null, "BATTLE_NOT_FOUND", "Battle not found");
        Job job = decode(value, battleId);
        GameException.require(playerId.equals(job.playerId()), "BATTLE_NOT_FOUND", "Battle not found");
        return new Report(job.battleId(), job.marchId(), job.state(), job.rulesVersion(), job.inputHash(), job.result());
    }

    static BattleInput battleInput(Value value, String battleId) throws IOException {
        Job job = decode(value, battleId);
        check("PENDING".equals(job.state()) && job.result() == null, "Battle is not pending");
        check(ExpeditionBattleEngine.RULES_VERSION.equals(job.rulesVersion()), "Unsupported battle rules");
        return job.input();
    }

    private static Job decode(Value value, String id) throws IOException {
        check(id != null && value != null, "Battle missing");
        Job job = Json.MAPPER.readValue(value.json(), Job.class);
        check(job != null && id.equals(job.battleId()) && id.equals(job.marchId()) && job.input() != null
                && id.equals(job.input().battleId()), "Battle identity mismatch");
        check(job.playerId() != null && job.playerId().equals(job.input().attacker().ownerId())
                && "npc".equals(job.input().defender().ownerId()) && job.heroIds() != null
                && job.marchVersion() > 0, "Battle reservation identity mismatch");
        // 查阅已完成战报不依赖当前部署的引擎，只有准备重算或首次结算时才要求规则可执行。
        check(job.rulesVersion() != null && !job.rulesVersion().isBlank()
                && RealmMarchService.hash(Json.write(job.input())).equals(job.inputHash()), "Battle input hash or rules missing");
        return job;
    }

    boolean settle(ActivePlayer active, String battleId, String inputHash, String rulesVersion,
                   BattleResult result) throws IOException {
        check(active.valid(), "Player needs reload");
        Value stored = journal.read("battle:" + battleId);
        Job job = decode(stored, battleId);
        check(active.playerId().equals(job.playerId()), "Battle owner mismatch");
        check(job.inputHash().equals(inputHash) && job.rulesVersion().equals(rulesVersion), "Stale Worker response");
        String scope = "battle:" + job.playerId();
        String digest = RealmMarchService.hash(inputHash + "\n" + rulesVersion);
        if ("SETTLED".equals(job.state())) {
            var receipt = journal.lookup(scope, battleId, digest);
            check(receipt != null && job.result() != null, "Settled battle missing report or receipt");
            active.replayed(receipt.sequence());
            return false;
        }
        check("PENDING".equals(job.state()) && job.result() == null, "Unexpected battle state");
        check(ExpeditionBattleEngine.RULES_VERSION.equals(rulesVersion), "Unsupported battle rules");
        Value mv = journal.read("march:" + job.marchId());
        check(mv != null && mv.version() == job.marchVersion(), "Battle march generation mismatch");
        March march = Json.MAPPER.readValue(mv.json(), March.class);
        check(march != null && job.marchId().equals(march.marchId()) && job.playerId().equals(march.playerId())
                && "BATTLE_PENDING".equals(march.state()) && "ATTACK_NPC".equals(march.mission())
                && RealmMarchService.RULES_VERSION.equals(march.rulesVersion()) && march.taskId() == null
                && job.heroIds().equals(march.heroIds()), "Battle no longer owns this march");
        var snapshot = active.snapshot();
        Player player = snapshot.player();
        try {
            MarchTasks.validateReservation(player, march);
        } catch (MarchTasks.BrokenTask | IllegalArgumentException bad) {
            throw new IOException("Battle reservation is inconsistent", bad);
        }
        validateResult(job, march, result);

        // 城内可用兵已在出征时扣除。先登记伤亡，幸存者和胜利奖励随同一趟返城到达。
        var wounded = new HashMap<>(player.wounded());
        var dead = new HashMap<>(player.dead());
        var survivors = new HashMap<String, Long>();
        for (GroupResult group : result.attacker().groups()) {
            String type = group.type().name();
            add(survivors, type, group.survivors());
            add(wounded, type, group.wounded());
            add(dead, type, group.dead());
        }
        Player updated = player.withArmy(player.troops(), player.heroes(),
                player.activeMarchIds(), player.resources(), wounded, dead);
        var cargo = result.outcome() == Outcome.ATTACKER_WIN ? WIN_CARGO : Map.<String, Long>of();
        March returning = MarchTasks.startReturn(march.withSurvivors(survivors), march.targetX(), march.targetY(),
                Math.max(march.arriveAt(), clock.millis()), cargo);
        var task = MarchTasks.nextTask(returning, "MARCH_RETURN", Math.toIntExact(Math.incrementExact(mv.version())));
        Job settled = job.settled(result);
        var changes = List.of(new Change("battle:" + battleId, stored.version(), Json.write(settled)),
                new Change("player:" + active.playerId(), snapshot.version(), Json.write(updated)),
                new Change("march:" + march.marchId(), mv.version(), Json.write(returning)), MarchTasks.taskChange(task, 0));
        var decision = new Decision(scope, battleId, digest, "NPC_BATTLE_SETTLED", Json.write(result),
                Json.write(Map.of("battleId", battleId, "state", "SETTLED")), changes);
        try {
            var outcome = journal.commitWithOutcome(decision);
            if (outcome.applied()) active.committed(outcome.receipt().sequence(), snapshot.version(), updated);
            else active.replayed(outcome.receipt().sequence());
            return outcome.applied();
        } catch (IOException | RuntimeException | Error uncertain) {
            active.invalidate();
            throw uncertain;
        }
    }

    private static void validateResult(Job job, March march, BattleResult result) throws IOException {
        BattleInput input = job.input();
        check(result != null && job.battleId().equals(result.battleId()) && input.seed() == result.seed()
                && job.rulesVersion().equals(result.rulesVersion())
                && result.rounds() > 0 && result.rounds() <= input.maxRounds(), "Invalid Worker report metadata");

        var reserved = new HashMap<String, Long>();
        for (TroopGroup group : input.attacker().groups()) {
            check(group.tier() == march.tier() && group.count() > 0, "Invalid reserved battle group");
            add(reserved, group.type().name(), group.count());
        }
        check(reserved.equals(march.troops()), "Battle input differs from reserved troops");
        validateArmy(input.attacker(), result.attacker());
        validateArmy(input.defender(), result.defender());
        boolean attackerLives = result.attacker().survivors() > 0;
        boolean defenderLives = result.defender().survivors() > 0;
        Outcome expected = attackerLives == defenderLives ? Outcome.DRAW
                : attackerLives ? Outcome.ATTACKER_WIN : Outcome.DEFENDER_WIN;
        check(result.outcome() == expected && (!attackerLives || !defenderLives || result.rounds() == input.maxRounds()),
                "Battle outcome or round limit contradicts survivors");
    }

    /** 战果的构造器已经检查每组数量非负且守恒；这里针对出征时的原组再逐项核对，不能串组。 */
    private static void validateArmy(Army input, ArmyResult result) throws IOException {
        check(input.ownerId().equals(result.ownerId()) && input.groups().size() == result.groups().size(),
                "Battle army or group count mismatch");
        var groups = new HashMap<String, TroopGroup>();
        for (TroopGroup group : input.groups()) groups.put(group.groupId(), group);
        for (GroupResult row : result.groups()) {
            TroopGroup group = groups.remove(row.groupId());
            check(group != null && row.type() == group.type() && row.tier() == group.tier()
                    && row.initialCount() == group.count(), "Battle group differs from input");
            check(row.wounded() == ExpeditionBattleEngine.woundedFromLosses(row.losses()),
                    "Wounded count differs from battle rules");
        }
        check(groups.isEmpty(), "Battle report omitted a group");
    }

    private static void add(Map<String, Long> amounts, String type, long amount) {
        if (amount > 0) amounts.merge(type, amount, Math::addExact);
    }

    private static void check(boolean valid, String message) throws IOException {
        if (!valid) throw new IOException(message);
    }
}

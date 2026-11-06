package com.slg.game.realm;

import com.slg.rpc.v1.BattleReportState;
import com.slg.rpc.v1.GetBattleRsp;
import com.slg.rpc.v1.MarchMission;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopGroup;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.battle.ArmyResult;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.core.battle.ExpeditionBattleEngine;
import com.slg.core.battle.GroupResult;
import com.slg.core.battle.Outcome;
import com.slg.core.battle.TroopType;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class RealmNpcBattleTest {
    @TempDir Path path;
    private static final String OWNER = "101";
    private static final long START = 1_000_000L;
    private final UUID volumeId = UUID.randomUUID();
    private static final String RULES = ExpeditionBattleEngine.RULES_VERSION;
    private static final List<TroopGroup> STRONG = List.of(
            troop(com.slg.rpc.v1.TroopType.TROOP_TYPE_INFANTRY, 1200),
            troop(com.slg.rpc.v1.TroopType.TROOP_TYPE_LANCER, 500),
            troop(com.slg.rpc.v1.TroopType.TROOP_TYPE_MARKSMAN, 500));

    @Test void droppedWorkerReplyThenRestartAndDuplicateReturnDeliverOnlyOnce() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        Prepared battle;
        BattleResult lostReply;
        try (var volume = open()) {
            battle = prepare(volume.journal(), STRONG);
            lostReply = new ExpeditionBattleEngine().simulate(battle.input());
            assertEquals(Outcome.ATTACKER_WIN, lostReply.outcome());
            assertTrue(lostReply.attacker().wounded() > 0);
            assertEquals(BattleReportState.REPORT_PENDING, report(volume.journal(), battle.id()).getState());
        }

        // Worker 已经产生战果，但 GAME 未收到便退出。重启仍取原输入并可确定性重算。
        try (var volume = open()) {
            var journal = volume.journal();
            var game = game(journal, START + 60_000);
            var input = game.battleInput(journal.read("battle:" + battle.id()), battle.id());
            assertEquals(lostReply, new ExpeditionBattleEngine().simulate(input));
            long before = journal.highSequence();
            var active = game.loadPlayer(OWNER);
            assertTrue(game.settleBattle(active, battle.id(), battle.hash(), RULES, lostReply));
            assertFalse(game.settleBattle(active, battle.id(), battle.hash(), RULES, lostReply));
            assertEquals(before + 1, journal.highSequence());
            var report = report(journal, battle.id());
            assertEquals(BattleReportState.REPORT_SETTLED, report.getState());
            assertEquals(com.slg.rpc.BattleWire.reply(lostReply, battle.hash()), report.getResult());
            var pendingReturn = read(journal, "march:" + battle.id());
            assertEquals("RETURNING", pendingReturn.path("state").asText());
            assertEquals(500, pendingReturn.at("/cargo/WOOD").asLong());
            var player = active.snapshot().player();
            assertEquals(battle.id(), player.heroes().getFirst().assignedMarch());
            assertEquals(Map.of("WOOD", 0L), player.resources());
            assertEquals(List.of(battle.id()), player.activeMarchIds());
            for (var row : lostReply.attacker().groups()) {
                String type = row.type().name();
                assertEquals(row.survivors(), pendingReturn.path("troops").path(type).asLong());
                assertEquals(row.wounded(), player.wounded().getOrDefault(type, 0L));
                assertEquals(row.dead(), player.dead().getOrDefault(type, 0L));
            }
        }
        // 已结算但回执丢失，再发一遍旧战果和旧返城提示均不会重复发兵或发奖。
        try (var volume = open()) {
            var journal = volume.journal();
            var wake = singleTask(journal);
            var game = game(journal, wake.dueAt());
            var active = game.loadPlayer(OWNER);
            long before = journal.highSequence();
            assertFalse(game.settleBattle(active, battle.id(), battle.hash(), RULES, lostReply));
            assertTrue(game.applyDue(active, wake));
            assertFalse(game.applyDue(active, wake));
            assertFalse(game.settleBattle(active, battle.id(), battle.hash(), RULES, lostReply));
            assertEquals(before + 1, journal.highSequence());
            var player = active.snapshot().player();
            assertNull(player.heroes().getFirst().assignedMarch());
            assertTrue(player.activeMarchIds().isEmpty());
            assertEquals(Map.of("WOOD", 500L, "FOOD", 500L, "GEMS", 10L), player.resources());
            for (var row : lostReply.attacker().groups()) {
                assertEquals((row.type() == TroopType.INFANTRY ? 3000L : 1000L) - row.losses(),
                        player.troops().get(row.type().name()));
            }
        }
    }

    @Test void allAttackersLostStillReturnTheHeroAndQueueSlotWithoutLoot() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = open()) {
            var journal = volume.journal();
            var battle = prepare(journal, List.of(troop(com.slg.rpc.v1.TroopType.TROOP_TYPE_INFANTRY, 20)));
            var result = new ExpeditionBattleEngine().simulate(battle.input());
            assertEquals(Outcome.DEFENDER_WIN, result.outcome());
            assertEquals(0, result.attacker().survivors());
            var game = game(journal, START + 60_000);
            var active = game.loadPlayer(OWNER);
            assertTrue(game.settleBattle(active, battle.id(), battle.hash(), RULES, result));
            assertTrue(read(journal, "march:" + battle.id()).path("troops").isEmpty());
            var task = singleTask(journal);
            assertTrue(game(journal, task.dueAt()).applyDue(active, task));
            var player = active.snapshot().player();
            assertEquals(2980L, player.troops().get("INFANTRY"));
            assertEquals(14L, player.wounded().get("INFANTRY"));
            assertEquals(6L, player.dead().get("INFANTRY"));
            assertNull(player.heroes().getFirst().assignedMarch());
            assertTrue(player.activeMarchIds().isEmpty());
            assertEquals(Map.of("WOOD", 0L), player.resources());
        }
    }

    @Test void drawAtRoundLimitReturnsSurvivorsButNoVictoryCargo() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = open()) {
            var journal = volume.journal();
            var battle = prepare(journal, List.of(troop(com.slg.rpc.v1.TroopType.TROOP_TYPE_INFANTRY, 150)));
            var game = game(journal, START + 60_000);
            // 此场景用一回合上限生成真正的平局；带旧输入摘要的迟到结果不得结算新快照。
            var initial = battle.input();
            var input = new BattleInput(battle.id(), initial.attacker(), initial.defender(), initial.seed(), 1);
            String digest = RealmMarchService.hash(Json.write(input));
            String key = "battle:" + battle.id();
            var original = journal.read(key);
            var job = (ObjectNode) Json.MAPPER.readTree(original.json());
            job.set("input", Json.MAPPER.valueToTree(input));
            job.put("inputHash", digest);
            journal.commitWithOutcome(new RealmJournal.Decision("fixture", "round-limit", digest, "FIXTURE", "{}", "{}",
                    List.of(new RealmJournal.Change(key, original.version(), Json.write(job)))));
            var active = game.loadPlayer(OWNER);
            var draw = new ExpeditionBattleEngine().simulate(input);
            assertEquals(Outcome.DRAW, draw.outcome());
            assertThrows(IOException.class, () -> game.settleBattle(active, battle.id(), battle.hash(), RULES,
                    new ExpeditionBattleEngine().simulate(initial)));
            assertTrue(game.settleBattle(active, battle.id(), digest, RULES, draw));
            var task = singleTask(journal);
            assertTrue(game(journal, task.dueAt()).applyDue(active, task));
            assertEquals(3000L - draw.attacker().losses(), active.snapshot().player().troops().get("INFANTRY"));
            assertEquals(Map.of("WOOD", 0L), active.snapshot().player().resources());
            assertTrue(active.snapshot().player().activeMarchIds().isEmpty());
        }
    }

    @Test void staleOrMaliciousAttachmentsCannotConsumeTheReservation() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = open()) {
            var journal = volume.journal();
            var battle = prepare(journal, STRONG);
            var game = game(journal, START + 60_000);
            var active = game.loadPlayer(OWNER);
            var result = new ExpeditionBattleEngine().simulate(battle.input());
            long before = journal.highSequence();
            assertEquals("BATTLE_NOT_FOUND", rejection(() -> game.getBattle(game.loadPlayer("102"), "other", battleById(battle.id()))));
            assertThrows(IOException.class, () -> game.settleBattle(game.loadPlayer("102"), battle.id(), battle.hash(), RULES, result));
            assertThrows(IOException.class, () -> game.settleBattle(active, battle.id(), "wrong-hash", RULES, result));
            assertThrows(IOException.class, () -> game.settleBattle(active, battle.id(), battle.hash(), "unknown", result));
            var group = result.attacker().groups().getFirst();
            var changed = new ArrayList<>(result.attacker().groups());
            changed.set(0, new GroupResult(group.groupId(), group.type(), group.tier() + 1, group.initialCount(),
                    group.survivors(), group.wounded(), group.dead()));
            var alteredGroups = new ArmyResult("1_" + OWNER, changed);
            var damaged = result.attacker().groups().stream().filter(row -> row.dead() > 0).findFirst().orElseThrow();
            changed = new ArrayList<>(result.attacker().groups());
            changed.set(changed.indexOf(damaged), new GroupResult(damaged.groupId(), damaged.type(), damaged.tier(),
                    damaged.initialCount(), damaged.survivors(), damaged.wounded() + 1, damaged.dead() - 1));
            var wrongSplit = new ArmyResult("1_" + OWNER, changed);
            for (var bad : List.of(
                    withArmy(result, new ArmyResult("1_102", result.attacker().groups())),
                    withArmy(result, new ArmyResult("1_" + OWNER, List.of())), withArmy(result, alteredGroups),
                    withArmy(result, wrongSplit), new BattleResult("someone-else", result.outcome(), result.rounds(),
                            result.attacker(), result.defender(), RULES, result.seed()),
                    new BattleResult(result.battleId(), result.outcome(), result.rounds(), result.attacker(),
                            result.defender(), RULES, result.seed() + 1),
                    new BattleResult(result.battleId(), result.outcome(), 101, result.attacker(),
                            result.defender(), RULES, result.seed()),
                    new BattleResult(result.battleId(), Outcome.DRAW, result.rounds(), result.attacker(),
                            result.defender(), RULES, result.seed()))) {
                assertThrows(IOException.class, () -> game.settleBattle(active, battle.id(), battle.hash(), RULES, bad));
            }
            assertEquals(before, journal.highSequence());
            assertEquals(BattleReportState.REPORT_PENDING, report(journal, battle.id()).getState());
            assertEquals("BATTLE_PENDING", read(journal, "march:" + battle.id()).path("state").asText());
            assertTrue(journal.dueTasks(Long.MAX_VALUE, null, 10).isEmpty());
            assertTrue(active.snapshot().player().wounded().isEmpty());
            assertEquals(battle.id(), active.snapshot().player().heroes().getFirst().assignedMarch());
            assertTrue(game.settleBattle(active, battle.id(), battle.hash(), RULES, result));
        }
    }

    @Test void corruptedInputOrSupersededMarchVersionNeverStartsReturn() throws Exception {
        RealmVolume.initialize(path, 1, volumeId);
        try (var volume = open()) {
            var journal = volume.journal();
            var battle = prepare(journal, STRONG);
            var game = game(journal, START + 60_000);
            var job = journal.read("battle:" + battle.id());
            var unbound = (ObjectNode) Json.MAPPER.readTree(job.json());
            unbound.remove("marchVersion");
            assertThrows(IOException.class, () -> game.battleInput(
                    new RealmJournal.Value(job.version(), Json.write(unbound)), battle.id()));
            var tampered = (ObjectNode) Json.MAPPER.readTree(job.json());
            ((ObjectNode) tampered.path("input")).put("seed", battle.input().seed() + 1);
            assertThrows(IOException.class, () -> game.battleInput(new RealmJournal.Value(job.version(), Json.write(tampered)), battle.id()));
            assertThrows(IOException.class, () -> game.battleInput(job, "wrong-id"));
            String key = "march:" + battle.id();
            var march = journal.read(key);
            journal.commitWithOutcome(new RealmJournal.Decision("fixture", "version", RealmMarchService.hash("version"),
                    "FIXTURE", "{}", "{}", List.of(new RealmJournal.Change(key, march.version(), march.json()))));
            long before = journal.highSequence();
            assertThrows(IOException.class, () -> game.settleBattle(game.loadPlayer(OWNER), battle.id(), battle.hash(),
                    RULES, new ExpeditionBattleEngine().simulate(battle.input())));
            assertEquals(before, journal.highSequence());
            assertTrue(journal.dueTasks(Long.MAX_VALUE, null, 10).isEmpty());
            assertEquals(BattleReportState.REPORT_PENDING, report(journal, battle.id()).getState());
        }
    }

    private static BattleResult withArmy(BattleResult r, ArmyResult attacker) {
        return new BattleResult(r.battleId(), r.outcome(), r.rounds(), attacker, r.defender(), r.rulesVersion(), r.seed());
    }

    private RealmVolume open() throws IOException { return RealmVolume.open(path, 1, volumeId); }

    private static RealmMarchService game(RealmJournal journal, long now) throws IOException {
        return new RealmMarchService(journal, Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC));
    }

    private record Prepared(String id, String hash, BattleInput input) {}
    private static Prepared prepare(RealmJournal journal, List<TroopGroup> troops) throws Exception {
        var game = game(journal, START);
        var active = game.loadPlayer(OWNER);
        game.createPlayer(active, "create", named(OWNER));
        var request = StartMarchReq.newBuilder().setMission(MarchMission.MARCH_MISSION_ATTACK_NPC)
                .setTargetX(120).setTargetY(120).addAllTroops(troops).build();
        var march = game.startMarch(active, "attack", request).getMarch();
        var arrival = singleTask(journal);
        game = game(journal, arrival.dueAt());
        assertTrue(game.applyDue(game.loadPlayer(OWNER), arrival));
        String id = march.getMarchId();
        var job = journal.read("battle:" + id);
        return new Prepared(id, read(journal, "battle:" + id).path("inputHash").asText(), game.battleInput(job, id));
    }

    private static RealmJournal.DueTask singleTask(RealmJournal journal) throws IOException {
        var tasks = journal.dueTasks(Long.MAX_VALUE, null, 8);
        assertEquals(1, tasks.size());
        return tasks.getFirst();
    }
    private static JsonNode read(RealmJournal journal, String key) throws IOException {
        return Json.MAPPER.readTree(journal.read(key).json());
    }
    private static GetBattleRsp report(RealmJournal journal, String battle) throws IOException {
        var game = game(journal, START);
        return game.getBattle(game.loadPlayer(OWNER), "read-report", battleById(battle));
    }
}

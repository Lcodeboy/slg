package com.slg.rpc;

import com.slg.core.battle.*;
import com.slg.core.protocol.Json;
import com.slg.rpc.v1.BattleArmyLosses;
import com.slg.rpc.v1.BattleArmySnapshot;
import com.slg.rpc.v1.BattleComputeReply;
import com.slg.rpc.v1.BattleComputeRequest;
import com.slg.rpc.v1.BattleGroupLosses;
import com.slg.rpc.v1.BattleGroupSnapshot;
import com.slg.rpc.v1.BattleOutcome;
import com.slg.rpc.v1.BattleTroopKind;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

/** 将固定快照及结果映射到内部协议；GAME 的持久化输入摘要在跨进程传输后仍可核验。 */
public final class BattleWire {
    private BattleWire() {}

    public static String inputHash(BattleInput input) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(Json.write(input).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    public static BattleComputeRequest request(BattleInput input, String hash, String rules) {
        text(input.battleId()); text(rules);
        require(inputHash(input).equals(hash), "Battle input digest mismatch");
        return BattleComputeRequest.newBuilder().setBattleId(input.battleId()).setInputHash(hash)
                .setRulesVersion(rules).setSeed(input.seed()).setMaxRounds(input.maxRounds())
                .setAttacker(army(input.attacker())).setDefender(army(input.defender())).build();
    }

    public static BattleInput input(BattleComputeRequest r) {
        text(r.getBattleId()); text(r.getRulesVersion());
        require(r.hasAttacker() && r.hasDefender(), "Both armies are required");
        BattleInput input = new BattleInput(r.getBattleId(), army(r.getAttacker()), army(r.getDefender()),
                r.getSeed(), r.getMaxRounds());
        require(inputHash(input).equals(r.getInputHash()), "Battle input digest mismatch");
        return input;
    }

    public static BattleComputeReply reply(BattleResult result, String hash) {
        return BattleComputeReply.newBuilder().setBattleId(result.battleId()).setInputHash(hash)
                .setRulesVersion(result.rulesVersion()).setSeed(result.seed()).setRounds(result.rounds())
                .setOutcome(switch (result.outcome()) {
                    case DRAW -> BattleOutcome.BATTLE_DRAW;
                    case ATTACKER_WIN -> BattleOutcome.BATTLE_ATTACKER_WIN;
                    case DEFENDER_WIN -> BattleOutcome.BATTLE_DEFENDER_WIN;
                }).setAttacker(losses(result.attacker())).setDefender(losses(result.defender())).build();
    }

    public static BattleResult result(BattleComputeReply reply, BattleInput input, String hash, String rules) {
        require(reply.getBattleId().equals(input.battleId()) && reply.getSeed() == input.seed()
                && reply.getRulesVersion().equals(rules) && reply.getInputHash().equals(hash),
                "Reply does not match the submitted battle snapshot");
        require(reply.hasAttacker() && reply.hasDefender() && reply.getRounds() <= input.maxRounds(),
                "Invalid reply dimensions");
        var attacker = losses(reply.getAttacker(), input.attacker());
        var defender = losses(reply.getDefender(), input.defender());
        Outcome outcome = switch (reply.getOutcome()) {
            case BATTLE_DRAW -> Outcome.DRAW;
            case BATTLE_ATTACKER_WIN -> Outcome.ATTACKER_WIN;
            case BATTLE_DEFENDER_WIN -> Outcome.DEFENDER_WIN;
            default -> throw new IllegalArgumentException("Unknown battle outcome");
        };
        boolean attackerAlive = attacker.survivors() > 0;
        boolean defenderAlive = defender.survivors() > 0;
        Outcome expected = Outcome.DRAW;
        if (attackerAlive != defenderAlive) expected = attackerAlive ? Outcome.ATTACKER_WIN : Outcome.DEFENDER_WIN;
        require(outcome == expected, "Battle outcome contradicts losses");
        boolean canFight = attacker.initialCount() > 0 && defender.initialCount() > 0;
        require(canFight == (reply.getRounds() > 0), "Round count contradicts the initial armies");
        if (!canFight) require(attacker.losses() == 0 && defender.losses() == 0, "Casualties without combat");
        if (attackerAlive && defenderAlive) require(reply.getRounds() == input.maxRounds(), "Battle ended prematurely");
        return new BattleResult(input.battleId(), outcome, reply.getRounds(), attacker, defender, rules, input.seed());
    }

    private static BattleArmySnapshot army(Army a) {
        text(a.ownerId());
        var out = BattleArmySnapshot.newBuilder().setOwnerId(a.ownerId())
                .setAttackMultiplier(a.attackMultiplier()).setDefenseMultiplier(a.defenseMultiplier());
        for (TroopGroup g : a.groups()) {
            text(g.groupId());
            out.addGroups(BattleGroupSnapshot.newBuilder().setGroupId(g.groupId()).setKind(kind(g.type()))
                    .setTier(g.tier()).setCount(g.count()));
        }
        return out.build();
    }

    private static Army army(BattleArmySnapshot a) {
        text(a.getOwnerId());
        require(a.getGroupsCount() <= 64, "Too many troop groups");
        return new Army(a.getOwnerId(), a.getGroupsList().stream().map(g -> {
            text(g.getGroupId());
            return new TroopGroup(g.getGroupId(), type(g.getKind()), g.getTier(), g.getCount());
        }).toList(), a.getAttackMultiplier(), a.getDefenseMultiplier());
    }

    private static BattleArmyLosses losses(ArmyResult a) {
        var out = BattleArmyLosses.newBuilder().setOwnerId(a.ownerId());
        for (GroupResult g : a.groups()) {
            out.addGroups(BattleGroupLosses.newBuilder().setGroupId(g.groupId()).setKind(kind(g.type()))
                    .setTier(g.tier()).setInitialCount(g.initialCount()).setSurvivors(g.survivors())
                    .setWounded(g.wounded()).setDead(g.dead()));
        }
        return out.build();
    }

    private static ArmyResult losses(BattleArmyLosses a, Army source) {
        require(a.getOwnerId().equals(source.ownerId()) && a.getGroupsCount() == source.groups().size(),
                "Reply army differs from snapshot");
        Map<String, TroopGroup> groups = new HashMap<>();
        for (TroopGroup g : source.groups()) groups.put(g.groupId(), g);
        return new ArmyResult(a.getOwnerId(), a.getGroupsList().stream().map(g -> {
            TroopGroup original = groups.remove(g.getGroupId());
            require(original != null && g.getInitialCount() == original.count() && g.getTier() == original.tier()
                    && type(g.getKind()) == original.type(), "Reply group differs from snapshot");
            return new GroupResult(g.getGroupId(), original.type(), original.tier(), g.getInitialCount(),
                    g.getSurvivors(), g.getWounded(), g.getDead());
        }).toList());
    }

    private static BattleTroopKind kind(TroopType type) {
        return switch (type) {
            case INFANTRY -> BattleTroopKind.BATTLE_INFANTRY;
            case LANCER -> BattleTroopKind.BATTLE_LANCER;
            case MARKSMAN -> BattleTroopKind.BATTLE_MARKSMAN;
        };
    }

    private static TroopType type(BattleTroopKind kind) {
        return switch (kind) {
            case BATTLE_INFANTRY -> TroopType.INFANTRY;
            case BATTLE_LANCER -> TroopType.LANCER;
            case BATTLE_MARKSMAN -> TroopType.MARKSMAN;
            default -> throw new IllegalArgumentException("Unknown troop type");
        };
    }

    private static void text(String s) {
        require(s != null && !s.isBlank() && s.length() <= 128 && s.codePoints().noneMatch(Character::isISOControl),
                "Invalid battle identifier");
    }

    private static void require(boolean test, String message) {
        if (!test) throw new IllegalArgumentException(message);
    }
}

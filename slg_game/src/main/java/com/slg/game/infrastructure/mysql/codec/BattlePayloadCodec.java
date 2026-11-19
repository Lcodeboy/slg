package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.battle.*;
import com.slg.game.domain.persistence.BattleState;
import com.slg.game.domain.persistence.JsonPayload;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;

import static com.slg.game.infrastructure.mysql.codec.StrictJson.*;

/**
 * 新权威库的辅助战斗 JSON v1。兵种使用固定 code；Outcome 的本格式编码为攻击胜1、防守胜2、平3。
 * 这里只验证固定输入/结果的结构、守恒及主行绑定，不重算战斗或授予结算资格。
 */
final class BattlePayloadCodec {
    private BattlePayloadCodec() {}

    static void validate(BattleState state, int maximum) {
        BattleInput input = input(parsePayload(state.input(), maximum));
        require(state.battleId().equals(input.battleId()) && state.playerId().equals(input.attacker().ownerId()),
                "Battle input identity does not match control row");
        String hash = HexFormat.of().formatHex(PersistenceCodec.hash(state.input().json().getBytes(StandardCharsets.UTF_8)));
        require(hash.equals(state.inputHash()), "Battle input hash does not match its exact UTF-8 text");
        if (state.result() == null) return;
        BattleResult result = result(parsePayload(state.result(), maximum));
        require(state.battleId().equals(result.battleId()) && state.rulesVersion().equals(result.rulesVersion())
                        && input.seed() == result.seed() && result.rounds() <= input.maxRounds(),
                "Battle result metadata does not match its input and control row");
        match(input.attacker(), result.attacker()); match(input.defender(), result.defender());
        boolean attackerLives = result.attacker().survivors() > 0;
        boolean defenderLives = result.defender().survivors() > 0;
        Outcome expected = attackerLives == defenderLives ? Outcome.DRAW
                : attackerLives ? Outcome.ATTACKER_WIN : Outcome.DEFENDER_WIN;
        require(result.outcome() == expected && (!attackerLives || !defenderLives || result.rounds() == input.maxRounds()),
                "Battle outcome contradicts survivors or round limit");
    }

    static JsonNode parsePayload(JsonPayload payload, int maximum) {
        if (payload.schemaVersion() != 1) throw new CodecException("Unsupported battle payload schema", true);
        return parse(payload.json(), maximum);
    }

    static BattleInput input(JsonNode n) {
        fields(n, "battleId", "attacker", "defender", "seed", "maxRounds");
        return new BattleInput(text(n, "battleId"), army(n.get("attacker")), army(n.get("defender")),
                number(n, "seed"), integer(n, "maxRounds"));
    }
    private static Army army(JsonNode n) {
        fields(n, "ownerId", "groups", "attackMultiplier", "defenseMultiplier");
        var groups = new ArrayList<TroopGroup>();
        for (JsonNode group : list(n, "groups", 64)) {
            fields(group, "groupId", "type", "tier", "count");
            groups.add(new TroopGroup(text(group, "groupId"), TroopType.fromCode(integer(group, "type")),
                    integer(group, "tier"), number(group, "count")));
        }
        return new Army(text(n, "ownerId"), groups, decimal(n, "attackMultiplier"), decimal(n, "defenseMultiplier"));
    }
    static BattleResult result(JsonNode n) {
        fields(n, "battleId", "outcome", "rounds", "attacker", "defender", "rulesVersion", "seed");
        Outcome outcome = switch (integer(n, "outcome")) {
            case 1 -> Outcome.ATTACKER_WIN;
            case 2 -> Outcome.DEFENDER_WIN;
            case 3 -> Outcome.DRAW;
            default -> throw new CodecException("Unknown battle outcome code");
        };
        return new BattleResult(text(n, "battleId"), outcome, integer(n, "rounds"),
                armyResult(n.get("attacker")), armyResult(n.get("defender")), text(n, "rulesVersion"), number(n, "seed"));
    }
    private static ArmyResult armyResult(JsonNode n) {
        fields(n, "ownerId", "groups");
        var groups = new ArrayList<GroupResult>();
        for (JsonNode group : list(n, "groups", 64)) {
            fields(group, "groupId", "type", "tier", "initialCount", "survivors", "wounded", "dead");
            groups.add(new GroupResult(text(group, "groupId"), TroopType.fromCode(integer(group, "type")),
                    integer(group, "tier"), number(group, "initialCount"), number(group, "survivors"),
                    number(group, "wounded"), number(group, "dead")));
        }
        return new ArmyResult(text(n, "ownerId"), groups);
    }

    private static void match(Army input, ArmyResult result) {
        require(input.ownerId().equals(result.ownerId()) && input.groups().size() == result.groups().size(),
                "Battle result army does not match input");
        var groups = new HashMap<String, TroopGroup>();
        input.groups().forEach(group -> groups.put(group.groupId(), group));
        for (GroupResult row : result.groups()) {
            var group = groups.remove(row.groupId());
            require(group != null && row.type() == group.type() && row.tier() == group.tier()
                    && row.initialCount() == group.count(), "Battle result group does not match input");
        }
        require(groups.isEmpty(), "Battle result omitted input group");
    }

    static ObjectNode encode(BattleInput input) {
        var node = object().put("battleId", input.battleId()).put("seed", input.seed()).put("maxRounds", input.maxRounds());
        node.set("attacker", encode(input.attacker())); node.set("defender", encode(input.defender())); return node;
    }
    private static ObjectNode encode(Army army) {
        var node = object().put("ownerId", army.ownerId()).put("attackMultiplier", army.attackMultiplier())
                .put("defenseMultiplier", army.defenseMultiplier());
        var groups = array();
        for (TroopGroup group : army.groups()) groups.add(object().put("groupId", group.groupId())
                .put("type", group.type().code()).put("tier", group.tier()).put("count", group.count()));
        node.set("groups", groups); return node;
    }
    static ObjectNode encode(BattleResult result) {
        int outcome = switch (result.outcome()) { case ATTACKER_WIN -> 1; case DEFENDER_WIN -> 2; case DRAW -> 3; };
        var node = object().put("battleId", result.battleId()).put("outcome", outcome).put("rounds", result.rounds())
                .put("rulesVersion", result.rulesVersion()).put("seed", result.seed());
        node.set("attacker", encode(result.attacker())); node.set("defender", encode(result.defender())); return node;
    }
    private static ObjectNode encode(ArmyResult army) {
        var node = object().put("ownerId", army.ownerId()); var groups = array();
        for (GroupResult group : army.groups()) groups.add(object().put("groupId", group.groupId())
                .put("type", group.type().code()).put("tier", group.tier()).put("initialCount", group.initialCount())
                .put("survivors", group.survivors()).put("wounded", group.wounded()).put("dead", group.dead()));
        node.set("groups", groups); return node;
    }
    private static void require(boolean valid, String message) { if (!valid) throw new CodecException(message); }
}

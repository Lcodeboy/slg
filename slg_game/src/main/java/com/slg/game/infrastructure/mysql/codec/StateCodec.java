package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.core.type.IntCodeEnum;
import com.slg.game.domain.persistence.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static com.slg.game.infrastructure.mysql.codec.StrictJson.*;

/** 封闭状态的显式 schema v1；不启用 Jackson 多态类名或默认枚举反序列化。 */
final class StateCodec {
    private static final int MAX_CHILDREN = 10_000;
    private StateCodec() {}

    static ObjectNode key(ObjectKey key) {
        var node = object().put("kind", key.kind().code());
        switch (key) {
            case ObjectKey.Player k -> node.put("playerId", k.playerId());
            case ObjectKey.Resource k -> node.put("playerId", k.playerId()).put("resourceType", k.resourceType().code());
            case ObjectKey.Troop k -> node.put("playerId", k.playerId()).put("troopType", k.troopType().code());
            case ObjectKey.Hero k -> node.put("playerId", k.playerId()).put("heroId", k.heroId());
            case ObjectKey.March k -> node.put("marchId", k.marchId());
            case ObjectKey.GatherSite k -> node.put("x", k.x()).put("y", k.y());
            case ObjectKey.Task k -> node.put("taskId", k.taskId());
            case ObjectKey.Battle k -> node.put("battleId", k.battleId());
        }
        return node;
    }

    static ObjectKey key(JsonNode node) {
        return switch (ObjectKind.fromCode(integer(node, "kind"))) {
            case PLAYER -> {
                fields(node, "kind", "playerId"); yield new ObjectKey.Player(text(node, "playerId"));
            }
            case RESOURCE -> {
                fields(node, "kind", "playerId", "resourceType");
                yield new ObjectKey.Resource(text(node, "playerId"), ResourceType.fromCode(integer(node, "resourceType")));
            }
            case TROOP -> {
                fields(node, "kind", "playerId", "troopType");
                yield new ObjectKey.Troop(text(node, "playerId"), TroopType.fromCode(integer(node, "troopType")));
            }
            case HERO -> {
                fields(node, "kind", "playerId", "heroId"); yield new ObjectKey.Hero(text(node, "playerId"), text(node, "heroId"));
            }
            case MARCH -> {
                fields(node, "kind", "marchId"); yield new ObjectKey.March(text(node, "marchId"));
            }
            case GATHER_SITE -> {
                fields(node, "kind", "x", "y"); yield new ObjectKey.GatherSite(integer(node, "x"), integer(node, "y"));
            }
            case TASK -> {
                fields(node, "kind", "taskId"); yield new ObjectKey.Task(text(node, "taskId"));
            }
            case BATTLE -> {
                fields(node, "kind", "battleId"); yield new ObjectKey.Battle(text(node, "battleId"));
            }
        };
    }

    static ObjectNode state(ObjectState state) {
        var node = key(state.key());
        switch (state) {
            case PlayerState s -> node.put("accountId", s.accountId()).put("foundingRealmId", s.foundingRealmId())
                    .put("currentRealmId", s.currentRealmId()).put("name", s.name());
            case ResourceState s -> node.put("amount", s.amount());
            case TroopState s -> node.put("available", s.available()).put("wounded", s.wounded()).put("dead", s.dead());
            case HeroState s -> node.put("assignedMarchId", s.assignedMarchId());
            case MarchState s -> {
                node.put("playerId", s.playerId()).put("status", s.status().code()).put("mission", s.mission().code())
                        .put("fromX", s.fromX()).put("fromY", s.fromY()).put("targetX", s.targetX()).put("targetY", s.targetY())
                        .put("departAt", s.departAt()).put("arriveAt", s.arriveAt()).put("tier", s.tier())
                        .put("taskId", s.taskId()).put("rulesVersion", s.rulesVersion());
                node.set("troops", amounts(s.troops())); node.set("cargo", amounts(s.cargo()));
                node.set("heroIds", strings(s.heroIds()));
            }
            case GatherSiteState s -> node.put("occupantPlayerId", s.occupantPlayerId())
                    .put("occupantMarchId", s.occupantMarchId()).put("generation", s.generation());
            case TaskState s -> node.put("playerId", s.playerId()).put("marchId", s.marchId())
                    .put("type", s.type().code()).put("status", s.status().code()).put("generation", s.generation())
                    .put("dueAt", s.dueAt()).put("reason", s.reason());
            case BattleState s -> {
                node.put("marchId", s.marchId()).put("playerId", s.playerId()).put("status", s.status().code())
                        .put("inputHash", s.inputHash()).put("rulesVersion", s.rulesVersion()).put("marchVersion", s.marchVersion());
                node.set("heroIds", strings(s.heroIds())); node.set("input", payload(s.input()));
                node.set("result", s.result() == null ? MAPPER.nullNode() : payload(s.result()));
            }
        }
        return node;
    }

    static ObjectState state(JsonNode n) {
        return switch (ObjectKind.fromCode(integer(n, "kind"))) {
            case PLAYER -> {
                fields(n, "kind", "playerId", "accountId", "foundingRealmId", "currentRealmId", "name");
                yield new PlayerState(text(n, "playerId"), number(n, "accountId"), integer(n, "foundingRealmId"),
                        integer(n, "currentRealmId"), text(n, "name"));
            }
            case RESOURCE -> {
                fields(n, "kind", "playerId", "resourceType", "amount");
                yield new ResourceState(text(n, "playerId"), ResourceType.fromCode(integer(n, "resourceType")), number(n, "amount"));
            }
            case TROOP -> {
                fields(n, "kind", "playerId", "troopType", "available", "wounded", "dead");
                yield new TroopState(text(n, "playerId"), TroopType.fromCode(integer(n, "troopType")),
                        number(n, "available"), number(n, "wounded"), number(n, "dead"));
            }
            case HERO -> {
                fields(n, "kind", "playerId", "heroId", "assignedMarchId");
                yield new HeroState(text(n, "playerId"), text(n, "heroId"), nullableText(n, "assignedMarchId"));
            }
            case MARCH -> {
                fields(n, "kind", "marchId", "playerId", "status", "mission", "fromX", "fromY", "targetX", "targetY",
                        "departAt", "arriveAt", "tier", "troops", "heroIds", "taskId", "rulesVersion", "cargo");
                yield new MarchState(text(n, "marchId"), text(n, "playerId"), MarchState.Status.fromCode(integer(n, "status")),
                        MarchState.Mission.fromCode(integer(n, "mission")), decimal(n, "fromX"), decimal(n, "fromY"),
                        integer(n, "targetX"), integer(n, "targetY"), number(n, "departAt"), number(n, "arriveAt"), integer(n, "tier"),
                        amounts(n, "troops", TroopType::fromCode), strings(n, "heroIds"), nullableText(n, "taskId"),
                        text(n, "rulesVersion"), amounts(n, "cargo", ResourceType::fromCode));
            }
            case GATHER_SITE -> {
                fields(n, "kind", "x", "y", "occupantPlayerId", "occupantMarchId", "generation");
                yield new GatherSiteState(integer(n, "x"), integer(n, "y"), nullableText(n, "occupantPlayerId"),
                        nullableText(n, "occupantMarchId"), number(n, "generation"));
            }
            case TASK -> {
                fields(n, "kind", "taskId", "playerId", "marchId", "type", "status", "generation", "dueAt", "reason");
                yield new TaskState(text(n, "taskId"), text(n, "playerId"), text(n, "marchId"),
                        TaskState.Type.fromCode(integer(n, "type")), TaskState.Status.fromCode(integer(n, "status")),
                        number(n, "generation"), number(n, "dueAt"), nullableText(n, "reason"));
            }
            case BATTLE -> {
                fields(n, "kind", "battleId", "marchId", "playerId", "status", "heroIds", "inputHash", "rulesVersion",
                        "marchVersion", "input", "result");
                yield new BattleState(text(n, "battleId"), text(n, "marchId"), text(n, "playerId"),
                        BattleState.Status.fromCode(integer(n, "status")), strings(n, "heroIds"), text(n, "inputHash"),
                        text(n, "rulesVersion"), number(n, "marchVersion"), payload(n.get("input")),
                        n.get("result").isNull() ? null : payload(n.get("result")));
            }
        };
    }

    private static ObjectNode payload(JsonPayload value) {
        return object().put("schemaVersion", value.schemaVersion()).put("json", value.json());
    }
    private static JsonPayload payload(JsonNode n) {
        fields(n, "schemaVersion", "json");
        return new JsonPayload(integer(n, "schemaVersion"), text(n, "json"));
    }

    /** 枚举 Map 在磁盘上是按整数 code 升序的数组，不使用 JSON 的字符串属性名来冒充整数键。 */
    private static <E extends Enum<E> & IntCodeEnum> ArrayNode amounts(Map<E, Long> values) {
        if (values.size() > MAX_CHILDREN) throw new CodecException("Too many amount entries");
        var result = array();
        values.entrySet().stream().sorted(Comparator.comparingInt(e -> e.getKey().code()))
                .forEach(e -> result.add(object().put("code", e.getKey().code()).put("amount", e.getValue())));
        return result;
    }
    private static <E extends Enum<E> & IntCodeEnum> Map<E, Long> amounts(JsonNode n, String field, IntFunction<E> decode) {
        var result = new LinkedHashMap<E, Long>();
        int previous = 0;
        for (JsonNode item : list(n, field, MAX_CHILDREN)) {
            fields(item, "code", "amount");
            int code = integer(item, "code");
            if (code <= previous) throw new CodecException("Amount codes must be unique and ascending");
            previous = code;
            result.put(decode.apply(code), number(item, "amount"));
        }
        return result;
    }
    private static ArrayNode strings(List<String> values) {
        if (values.size() > MAX_CHILDREN) throw new CodecException("Too many child entries");
        var result = array(); values.forEach(result::add); return result;
    }
    private static List<String> strings(JsonNode n, String field) {
        var values = new ArrayList<String>();
        for (JsonNode value : list(n, field, MAX_CHILDREN)) {
            if (!value.isTextual()) throw new CodecException("Child identifier must be a string");
            values.add(value.textValue());
        }
        return values;
    }
}

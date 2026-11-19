package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.game.application.march.*;
import com.slg.game.application.player.PlayerAccess;
import com.slg.game.domain.march.PendingBattle;
import com.slg.game.domain.march.StartMarchOrder;
import com.slg.game.domain.persistence.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static com.slg.game.infrastructure.mysql.codec.StrictJson.*;

/**
 * 行军用例的 schema 1 有界编码。摘要只含版本化原请求，规划版本、会话、时钟均不参与。
 * 原请求保留 optional presence、未知整数码和重复兵种，交领域规则生成确定拒绝；持久状态严格解码。
 */
public final class MarchPayloadJsonCodec implements MarchPayloadCodec {
    private static final int INPUT_BYTES = 8192;
    private static final int RESULT_BYTES = 1024 * 1024;

    @Override public byte[] encodeInvocation(MarchInvocation invocation) {
        var node = object().put("schema", 1).put("marchId", invocation.marchId());
        node.set("access", access(invocation.access()));
        node.set("order", invocation.order() == null ? MAPPER.nullNode() : order(invocation.order()));
        node.set("plan", invocation.plan() == null ? MAPPER.nullNode() : plan(invocation.plan()));
        return write(node, INPUT_BYTES);
    }
    @Override public MarchInvocation decodeInvocation(byte[] bytes) {
        var node = schema(parse(utf8(bytes, INPUT_BYTES), INPUT_BYTES), "schema", "access", "order", "marchId", "plan");
        return new MarchInvocation(access(node.get("access")), node.get("order").isNull() ? null : order(node.get("order")),
                nullableText(node, "marchId"), node.get("plan").isNull() ? null : plan(node.get("plan")));
    }
    @Override public String startDigest(StartMarchOrder input) {
        // 兵组顺序不改变业务；只排序、不去重、不滤零，避免错误请求与合法请求碰撞。
        return digest("march.start.v1", write(order(input.canonical()), INPUT_BYTES));
    }
    @Override public String recallDigest(String marchId) {
        return digest("march.recall.v1", utf8Bytes(marchId, INPUT_BYTES));
    }
    @Override public byte[] encodePlan(MarchPlan value) {
        var node = object().put("schema", 1); node.set("plan", plan(value)); return write(node, INPUT_BYTES);
    }
    @Override public MarchPlan decodePlan(byte[] bytes) {
        return plan(schema(parse(utf8(bytes, INPUT_BYTES), INPUT_BYTES), "schema", "plan").get("plan"));
    }
    @Override public byte[] encodeMarch(MarchState value) {
        var node = object().put("schema", 1); node.set("march", StateCodec.state(value)); return write(node, RESULT_BYTES);
    }
    @Override public MarchState decodeMarch(byte[] bytes) {
        var value = StateCodec.state(schema(parse(utf8(bytes, RESULT_BYTES), RESULT_BYTES), "schema", "march").get("march"));
        if (!(value instanceof MarchState march)) throw new CodecException("Expected typed march result");
        return march;
    }
    @Override public String fact(String operation, long accountId, String marchId, List<String> phases, String rejectionCode) {
        if (phases.size() > 16) throw new CodecException("Too many march phases");
        var node = object().put("schema", 1).put("operation", operation).put("accountId", accountId)
                .put("marchId", marchId).put("rejectionCode", rejectionCode);
        var steps = array(); phases.forEach(steps::add); node.set("phases", steps);
        return utf8(write(node, RESULT_BYTES), RESULT_BYTES);
    }
    @Override public BattleState prepareBattle(PendingBattle pending) {
        // 纯规则固定输入之后才编码；哈希绑定这一份原始 UTF-8 文本，不能默认 Jackson 名称编码兵种。
        byte[] input = write(BattlePayloadCodec.encode(pending.input()), RESULT_BYTES);
        var battle = new BattleState(pending.marchId(), pending.marchId(), pending.playerId(), BattleState.Status.PENDING,
                pending.heroIds(), HexFormat.of().formatHex(PersistenceCodec.hash(input)), pending.rulesVersion(),
                pending.marchVersion(), new JsonPayload(1, utf8(input, RESULT_BYTES)), null);
        BattlePayloadCodec.validate(battle, RESULT_BYTES);
        return battle;
    }
    @Override public int maxResultBytes() { return RESULT_BYTES; }

    private static ObjectNode access(PlayerAccess value) {
        return object().put("accountId", value.accountId()).put("issuedAt", value.issuedAt()).put("expiresAt", value.expiresAt())
                .put("gameInstance", value.gameInstance()).put("generation", value.generation()).put("bindingId", value.bindingId());
    }
    private static PlayerAccess access(JsonNode node) {
        fields(node, "accountId", "issuedAt", "expiresAt", "gameInstance", "generation", "bindingId");
        return new PlayerAccess(number(node, "accountId"), number(node, "issuedAt"), number(node, "expiresAt"),
                text(node, "gameInstance"), number(node, "generation"), text(node, "bindingId"));
    }
    private static ObjectNode order(StartMarchOrder value) {
        var node = object().put("mission", value.mission()).put("targetX", value.targetX()).put("targetY", value.targetY()).put("tier", value.tier());
        var troops = array(); value.troops().forEach(group -> troops.add(object().put("type", group.type()).put("count", group.count())));
        node.set("troops", troops);
        if (value.heroIds() == null) node.putNull("heroIds");
        else { var heroes = array(); value.heroIds().forEach(heroes::add); node.set("heroIds", heroes); }
        return node;
    }
    private static StartMarchOrder order(JsonNode node) {
        fields(node, "mission", "targetX", "targetY", "tier", "troops", "heroIds");
        var troops = new ArrayList<StartMarchOrder.TroopSelection>();
        for (var group : list(node, "troops", 1024)) {
            fields(group, "type", "count");
            troops.add(new StartMarchOrder.TroopSelection(integer(group, "type"), number(group, "count")));
        }
        List<String> heroes = null;
        if (!node.get("heroIds").isNull()) {
            heroes = new ArrayList<>();
            for (var hero : list(node, "heroIds", 1024)) {
                if (!hero.isTextual()) throw new CodecException("Expected hero identifier");
                heroes.add(hero.textValue());
            }
        }
        return new StartMarchOrder(nullableInt(node, "mission"), nullableInt(node, "targetX"), nullableInt(node, "targetY"),
                nullableInt(node, "tier"), troops, heroes);
    }
    private static ObjectNode plan(MarchPlan value) {
        return object().put("marchId", value.marchId()).put("version", value.version()).put("siteX", value.siteX()).put("siteY", value.siteY());
    }
    private static MarchPlan plan(JsonNode node) {
        fields(node, "marchId", "version", "siteX", "siteY");
        return new MarchPlan(text(node, "marchId"), number(node, "version"), nullableInt(node, "siteX"), nullableInt(node, "siteY"));
    }
    private static Integer nullableInt(JsonNode node, String field) { return node.get(field).isNull() ? null : integer(node, field); }
    private static JsonNode schema(JsonNode node, String... names) {
        fields(node, names);
        if (integer(node, "schema") != 1) throw new CodecException("Unsupported march payload schema", true);
        return node;
    }
    private static String digest(String domain, byte[] payload) {
        var prefix = (domain + '\0').getBytes(StandardCharsets.US_ASCII);
        var bytes = new byte[prefix.length + payload.length];
        System.arraycopy(prefix, 0, bytes, 0, prefix.length); System.arraycopy(payload, 0, bytes, prefix.length, payload.length);
        return HexFormat.of().formatHex(PersistenceCodec.hash(bytes));
    }
}

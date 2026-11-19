package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.player.*;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.slg.game.infrastructure.mysql.codec.StrictJson.*;

/**
 * 新玩家用例的schema 1编码。复用存储的严格UTF-8/字段集合/整数code规则，但不访问数据库。
 * 原响应保存为明确JSON对象，取消旧Protobuf Base64标量格式；新区不双读旧数据。
 * 会话证明仅出现在内存命令中，摘要/事实/回执不包含证明，重连仍重放同一个业务结果。
 */
public final class PlayerPayloadJsonCodec implements PlayerPayloadCodec {
    private static final int INPUT_BYTES = 8192;
    private static final int RESULT_BYTES = 1024 * 1024;
    // 已验证的JSON文本再次放入字符串时，引号/反斜杠/合法空白至多扩为2字节；另外保留
    // 固定有界请求键/摘要/序号的外壳，不能沿用原响应1MiB而让合法回执变成不可查询。
    private static final int OBSERVATION_BYTES = 2 * RESULT_BYTES + 8192;
    private static final int MAX_MEMBERS = 10_000;

    @Override public byte[] encodeInvocation(PlayerInvocation invocation) {
        var actor = invocation.access();
        var node = object().put("schema", 1).put("value", invocation.value());
        node.set("access", object().put("accountId", actor.accountId()).put("issuedAt", actor.issuedAt())
                .put("expiresAt", actor.expiresAt()).put("gameInstance", actor.gameInstance())
                .put("generation", actor.generation()).put("bindingId", actor.bindingId()));
        return write(node, INPUT_BYTES);
    }
    @Override public PlayerInvocation decodeInvocation(byte[] bytes) {
        var node = schema(parse(utf8(bytes, INPUT_BYTES), INPUT_BYTES), "schema", "value", "access");
        var actor = fields(node.get("access"), "accountId", "issuedAt", "expiresAt", "gameInstance", "generation", "bindingId");
        return new PlayerInvocation(new PlayerAccess(number(actor, "accountId"), number(actor, "issuedAt"), number(actor, "expiresAt"),
                text(actor, "gameInstance"), number(actor, "generation"), text(actor, "bindingId")), text(node, "value"));
    }

    @Override public String createDigest(String rawName) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            // 类型和编码版本构成域分隔，保留原始昵称（不在摘要前strip）；会话/状态/排队预算不参与。
            digest.update("player.create.v1\0".getBytes(StandardCharsets.US_ASCII));
            return HexFormat.of().formatHex(digest.digest(utf8Bytes(rawName, INPUT_BYTES)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    @Override public String encodeOutcome(PlayerOutcome outcome) {
        var node = object().put("schema", 1).put("operation", outcome.operation());
        node.set("player", outcome.player() == null ? MAPPER.nullNode() : details(outcome.player()));
        node.set("march", outcome.march() == null ? MAPPER.nullNode() : StateCodec.state(outcome.march()));
        node.put("errorCode", outcome.errorCode()).put("errorMessage", outcome.errorMessage());
        return utf8(write(node, RESULT_BYTES), RESULT_BYTES);
    }
    @Override public PlayerOutcome decodeOutcome(String json) {
        var node = schema(parse(json, RESULT_BYTES), "schema", "operation", "player", "march", "errorCode", "errorMessage");
        return new PlayerOutcome(text(node, "operation"), node.get("player").isNull() ? null : details(node.get("player")),
                node.get("march").isNull() ? null : requireType(StateCodec.state(node.get("march")), MarchState.class),
                nullableText(node, "errorCode"), nullableText(node, "errorMessage"));
    }
    @Override public String creationFact(long accountId, String playerId, String rejectionCode) {
        return utf8(write(object().put("schema", 1).put("operation", PlayerOutcome.CREATE).put("accountId", accountId)
                .put("playerId", playerId).put("rejectionCode", rejectionCode), RESULT_BYTES), RESULT_BYTES);
    }
    @Override public byte[] encodeDetails(PlayerDetails details) {
        var node = object().put("schema", 1); node.set("player", details(details)); return write(node, RESULT_BYTES);
    }
    @Override public PlayerDetails decodeDetails(byte[] bytes) {
        return details(schema(parse(utf8(bytes, RESULT_BYTES), RESULT_BYTES), "schema", "player").get("player"));
    }

    @Override public byte[] encodeObservation(RealmReceiptReader.Observation observation) {
        var node = object().put("schema", 1);
        if (observation instanceof RealmReceiptReader.Observation.Found found) {
            var receipt = found.receipt();
            parse(receipt.resultJson(), RESULT_BYTES);
            node.set("receipt", object().put("scope", receipt.request().scope()).put("requestId", receipt.request().requestId())
                    .put("sequence", receipt.sequence()).put("digest", receipt.digest()).put("resultJson", receipt.resultJson()));
        } else node.putNull("receipt");
        return write(node, OBSERVATION_BYTES);
    }
    @Override public RealmReceiptReader.Observation decodeObservation(byte[] bytes) {
        var node = schema(parse(utf8(bytes, OBSERVATION_BYTES), OBSERVATION_BYTES), "schema", "receipt").get("receipt");
        if (node.isNull()) return new RealmReceiptReader.Observation.NotObserved();
        fields(node, "scope", "requestId", "sequence", "digest", "resultJson");
        parse(text(node, "resultJson"), RESULT_BYTES);
        return new RealmReceiptReader.Observation.Found(new CommitReceipt(new RequestKey(text(node, "scope"), text(node, "requestId")),
                number(node, "sequence"), text(node, "digest"), text(node, "resultJson")));
    }

    @Override public long measureSnapshot(PlayerSnapshot snapshot) {
        var node = object().put("realmId", snapshot.identity().realmId()).put("datasetId", snapshot.identity().datasetId().toString())
                .put("accountId", snapshot.accountId()).put("highSequence", snapshot.highSequence());
        var states = array(); node.set("states", states);
        for (var value : snapshot.ownedStates()) {
            var item = object().put("version", value.version()); item.set("state", StateCodec.state(value.state())); states.add(item);
        }
        // 计量完整持有的结构化根/明细的编码字节；目录另计固定元数据，不宣称精确JVM堆大小。
        return write(node, PersistenceCodec.DEFAULT_MAX_BYTES).length;
    }
    @Override public int maxDetailsBytes() { return RESULT_BYTES; }
    @Override public int maxResultBytes() { return OBSERVATION_BYTES; }

    private static ObjectNode details(PlayerDetails details) {
        var node = object().put("version", details.version()); node.set("profile", StateCodec.state(details.profile()));
        var resources = array(); var troops = array(); var heroes = array(); var marches = array();
        node.set("resources", resources); node.set("troops", troops); node.set("heroes", heroes); node.set("activeMarchIds", marches);
        details.resources().entrySet().stream().sorted(Comparator.comparingInt(entry -> entry.getKey().code()))
                .forEach(entry -> resources.add(StateCodec.state(new ResourceState(details.profile().playerId(), entry.getKey(), entry.getValue()))));
        details.troops().values().stream().sorted(Comparator.comparingInt(state -> state.troopType().code()))
                .forEach(state -> troops.add(StateCodec.state(state)));
        details.heroes().stream().sorted(Comparator.comparing(HeroState::heroId)).forEach(state -> heroes.add(StateCodec.state(state)));
        details.activeMarchIds().forEach(marches::add);
        return node;
    }
    private static PlayerDetails details(JsonNode node) {
        fields(node, "version", "profile", "resources", "troops", "heroes", "activeMarchIds");
        var profile = requireType(StateCodec.state(node.get("profile")), PlayerState.class);
        var resources = new EnumMap<ResourceType, Long>(ResourceType.class);
        var troops = new EnumMap<TroopType, TroopState>(TroopType.class);
        var heroes = new ArrayList<HeroState>(); var marches = new ArrayList<String>();
        for (var item : list(node, "resources", MAX_MEMBERS)) {
            var state = requireType(StateCodec.state(item), ResourceState.class);
            if (!profile.playerId().equals(state.playerId()) || resources.put(state.resourceType(), state.amount()) != null)
                throw new CodecException("Invalid or duplicate player resource");
        }
        for (var item : list(node, "troops", MAX_MEMBERS)) {
            var state = requireType(StateCodec.state(item), TroopState.class);
            if (troops.put(state.troopType(), state) != null) throw new CodecException("Duplicate player troop");
        }
        for (var item : list(node, "heroes", MAX_MEMBERS)) heroes.add(requireType(StateCodec.state(item), HeroState.class));
        for (var item : list(node, "activeMarchIds", MAX_MEMBERS)) {
            if (!item.isTextual()) throw new CodecException("Invalid active march ID");
            marches.add(item.textValue());
        }
        return new PlayerDetails(number(node, "version"), profile, resources, troops, heroes, marches);
    }
    private static <T> T requireType(Object value, Class<T> type) {
        if (!type.isInstance(value)) throw new CodecException("Unexpected player state type");
        return type.cast(value);
    }
    private static JsonNode schema(JsonNode node, String... names) {
        fields(node, names);
        if (integer(node, "schema") != 1) throw new CodecException("Unsupported player payload schema", true);
        return node;
    }
}

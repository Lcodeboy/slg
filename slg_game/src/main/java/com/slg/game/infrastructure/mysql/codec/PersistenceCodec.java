package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.game.domain.persistence.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Objects;

import static com.slg.game.infrastructure.mysql.codec.StrictJson.*;

/**
 * 完整决定的版本化存储 codec。变化、读取依赖和原始回执一起编码，不能拆开成功返回。
 * 原 factJson/resultJson 与战斗载荷文本逐字保留；只对封闭结构的键及枚举映射作规范编码。
 * 字节/条目预算同时用于写前检查和存量数据读取，失败不会产生截断的可用决定。
 */
public final class PersistenceCodec {
    public static final int ENTRY_SCHEMA = 1;
    public static final int DEFAULT_MAX_BYTES = 8 * 1024 * 1024;
    public static final int DEFAULT_MAX_JSON_BYTES = 1024 * 1024;
    private static final int MAX_ENTRIES = 10_000;
    private final int maximumBytes;
    private final int maximumJsonBytes;

    public PersistenceCodec() { this(DEFAULT_MAX_BYTES, DEFAULT_MAX_JSON_BYTES); }
    public PersistenceCodec(int maximumBytes, int maximumJsonBytes) {
        if (maximumBytes <= 0 || maximumBytes > DEFAULT_MAX_BYTES || maximumJsonBytes <= 0
                || maximumJsonBytes > DEFAULT_MAX_JSON_BYTES || maximumJsonBytes > maximumBytes)
            throw new IllegalArgumentException("Codec limits must be positive and within format limits");
        this.maximumBytes = maximumBytes; this.maximumJsonBytes = maximumJsonBytes;
    }

    public byte[] encodeDecision(CommitDecision decision) {
        Objects.requireNonNull(decision, "decision");
        if (decision.changes().size() > MAX_ENTRIES || decision.readVersions().size() > MAX_ENTRIES)
            throw new CodecException("Decision exceeds entry budget");
        validateJson(decision.factJson()); validateJson(decision.resultJson());
        var node = object().put("schema", ENTRY_SCHEMA).put("digest", decision.digest()).put("factType", decision.factType())
                .put("factJson", decision.factJson()).put("resultJson", decision.resultJson());
        node.set("request", object().put("scope", decision.request().scope()).put("requestId", decision.request().requestId()));
        var changes = array();
        var reads = array();
        node.set("changes", changes); node.set("readVersions", reads);
        long encodedBytes = write(node, maximumBytes).length;
        for (ObjectChange change : decision.changes()) {
            validateState(change.afterState());
            var entry = object().put("expectedVersion", change.expectedVersion());
            entry.set("key", StateCodec.key(change.key())); entry.set("afterState", StateCodec.state(change.afterState()));
            encodedBytes += write(entry, maximumBytes).length + (changes.isEmpty() ? 0 : 1);
            if (encodedBytes > maximumBytes) throw new CodecException("Decision exceeds byte budget");
            changes.add(entry);
        }
        for (var entry : decision.readVersions().entrySet().stream().sorted(java.util.Map.Entry.comparingByKey(ObjectKey.ORDER)).toList()) {
            var read = object().put("version", entry.getValue()); read.set("key", StateCodec.key(entry.getKey()));
            encodedBytes += write(read, maximumBytes).length + (reads.isEmpty() ? 0 : 1);
            if (encodedBytes > maximumBytes) throw new CodecException("Decision exceeds byte budget");
            reads.add(read);
        }
        return write(node, maximumBytes);
    }

    public CommitDecision decodeDecision(byte[] bytes) {
        try {
            JsonNode node = parse(utf8(bytes, maximumBytes), maximumBytes);
            if (integer(node, "schema") != ENTRY_SCHEMA) throw new CodecException("Unsupported decision schema", true);
            fields(node, "schema", "request", "digest", "factType", "factJson", "resultJson", "changes", "readVersions");
            JsonNode request = fields(node.get("request"), "scope", "requestId");
            var changes = new ArrayList<ObjectChange>();
            for (JsonNode entry : list(node, "changes", MAX_ENTRIES)) {
                fields(entry, "key", "expectedVersion", "afterState");
                var state = StateCodec.state(entry.get("afterState")); validateState(state);
                changes.add(new ObjectChange(StateCodec.key(entry.get("key")), number(entry, "expectedVersion"), state));
            }
            var reads = new LinkedHashMap<ObjectKey, Long>();
            ObjectKey previous = null;
            for (JsonNode entry : list(node, "readVersions", MAX_ENTRIES)) {
                fields(entry, "key", "version");
                var key = StateCodec.key(entry.get("key"));
                if (previous != null && previous.compareTo(key) >= 0) throw new CodecException("Read keys must be unique and sorted");
                previous = key; reads.put(key, number(entry, "version"));
            }
            String fact = text(node, "factJson"), result = text(node, "resultJson");
            validateJson(fact); validateJson(result);
            return new CommitDecision(new RequestKey(text(request, "scope"), text(request, "requestId")),
                    text(node, "digest"), text(node, "factType"), fact, result, changes, reads);
        } catch (CodecException bad) { throw bad;
        } catch (RuntimeException bad) { throw new CodecException("Invalid decision value", bad); }
    }

    public void validateDecision(CommitDecision decision) { encodeDecision(decision); }

    public byte[] encodeState(ObjectState state) {
        validateState(state); return write(StateCodec.state(state), maximumBytes);
    }
    public long encodedStateBytes(ObjectState state) { return encodeState(state).length; }

    public void validateState(ObjectState state) {
        Objects.requireNonNull(state, "state");
        try {
            if (state instanceof BattleState battle) BattlePayloadCodec.validate(battle, maximumJsonBytes);
            // 集合预算在构造树时检查，完整字节上限同时覆盖主行和全部明细。
            write(StateCodec.state(state), maximumBytes);
        } catch (CodecException bad) { throw bad;
        } catch (RuntimeException bad) { throw new CodecException("Invalid typed state payload", bad); }
    }

    public void validateJson(String json) { parse(json, maximumJsonBytes); }
    public byte[] encodeJson(String json) { validateJson(json); return utf8Bytes(json, maximumJsonBytes); }
    public String decodeJson(byte[] bytes) {
        String json = utf8(bytes, maximumJsonBytes); validateJson(json); return json;
    }
    public JsonPayload decodeJsonPayload(int schema, byte[] bytes) {
        if (schema != 1) throw new CodecException("Unsupported auxiliary payload schema", true);
        return new JsonPayload(schema, decodeJson(bytes));
    }
    public JsonPayload encodeBattleInput(BattleInput input) {
        return new JsonPayload(1, new String(write(BattlePayloadCodec.encode(input), maximumJsonBytes), StandardCharsets.UTF_8));
    }
    public JsonPayload encodeBattleResult(BattleResult result) {
        return new JsonPayload(1, new String(write(BattlePayloadCodec.encode(result), maximumJsonBytes), StandardCharsets.UTF_8));
    }

    public static byte[] hash(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable", impossible); }
    }
}

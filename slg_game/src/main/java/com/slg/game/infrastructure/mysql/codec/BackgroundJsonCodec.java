package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.battle.*;
import com.slg.game.application.background.*;
import com.slg.game.application.march.MarchPlan;
import com.slg.game.application.port.RealmWorkReader;
import com.slg.game.domain.march.BattleReport;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.util.*;
import static com.slg.game.infrastructure.mysql.codec.StrictJson.*;

/**
 * 后台 schema 1 编码。系统去重身份固定于持久候选，不混入本轮时间、规划或 Worker 返回值。
 * 完整工作包最多 4 MiB，战斗输入/结果各 1 MiB；候选中的原 JSON 再次转义也计入工作包。
 * 存储坏数据整份失败，不以空派发、空战报或默认枚举掩盖失败。
 */
public final class BackgroundJsonCodec implements BackgroundCodec {
    private static final int WORK_BYTES = 4 * 1024 * 1024;
    private static final int BATTLE_BYTES = 1024 * 1024;
    private static final int PLAN_BYTES = 8192;

    @Override public byte[] encodeInvocation(BackgroundInvocation value) {
        requireAccount(value.accountId(), value.candidate());
        var node = object().put("schema", 1).put("accountId", value.accountId());
        node.set("candidate", candidate(value.candidate()));
        node.set("plan", value.plan() == null ? MAPPER.nullNode() : plan(value.plan()));
        node.set("result", value.result() == null ? MAPPER.nullNode() : boundedResult(value.result()));
        return write(node, WORK_BYTES);
    }
    @Override public BackgroundInvocation decodeInvocation(byte[] bytes) {
        var node = schema(parse(utf8(bytes, WORK_BYTES), WORK_BYTES), "schema", "accountId", "candidate", "plan", "result");
        long account = number(node, "accountId"); var candidate = candidate(node.get("candidate"));
        requireAccount(account, candidate);
        var result = node.get("result").isNull() ? null : decodeResult(node.get("result"));
        return new BackgroundInvocation(account, candidate, node.get("plan").isNull() ? null : plan(node.get("plan")), result);
    }
    @Override public RequestKey request(VersionedState candidate) {
        String identity = switch (candidate.state()) {
            case TaskState task -> "due.v1\0" + task.taskId() + '\n' + task.generation() + '\n' + candidate.version();
            case BattleState battle -> "battle.v1\0" + battle.battleId() + '\n' + candidate.version();
            default -> throw new CodecException("Unsupported background candidate");
        };
        return new RequestKey((candidate.state() instanceof TaskState ? "due:" : "battle:") + account(candidate),
                hash(utf8Bytes(identity, PLAN_BYTES)));
    }
    @Override public String digest(BackgroundInvocation input) {
        requireAccount(input.accountId(), input.candidate());
        var node = object().put("schema", 1).put("operation", operation(input.candidate()));
        node.set("candidate", candidate(input.candidate()));
        // 完整候选绑定原输入/版本；不同合法重算结果不应改变同一持久任务的幂等身份。
        return hash(write(node, WORK_BYTES));
    }
    @Override public String encodeOutcome(BackgroundOutcome outcome) {
        validate(outcome);
        return utf8(write(object().put("schema", 1).put("operation", outcome.operation()).put("accountId", outcome.accountId())
                .put("objectId", outcome.objectId()).put("generation", outcome.generation()).put("status", outcome.status()), PLAN_BYTES), PLAN_BYTES);
    }
    @Override public BackgroundOutcome decodeOutcome(String json) {
        var node = schema(parse(json, PLAN_BYTES), "schema", "operation", "accountId", "objectId", "generation", "status");
        var result = new BackgroundOutcome(text(node, "operation"), number(node, "accountId"), text(node, "objectId"),
                number(node, "generation"), text(node, "status"));
        validate(result); return result;
    }
    @Override public String fact(BackgroundInvocation input, List<String> phases, String reason) {
        requireAccount(input.accountId(), input.candidate());
        if (phases.size() > 16 || (reason != null && reason.length() > 2048)) throw new CodecException("Oversized background fact");
        var node = object().put("schema", 1).put("operation", operation(input.candidate())).put("accountId", input.accountId())
                .put("requestId", request(input.candidate()).requestId()).put("reason", reason);
        var steps = array(); phases.forEach(steps::add); node.set("phases", steps);
        node.put("candidateVersion", input.candidate().version());
        switch (input.candidate().state()) {
            case TaskState task -> node.put("objectId", task.taskId()).put("generation", task.generation());
            case BattleState battle -> node.put("objectId", battle.battleId()).put("generation", battle.marchVersion())
                    .put("inputHash", battle.inputHash()).put("rulesVersion", battle.rulesVersion());
            default -> throw new CodecException("Unsupported background fact");
        }
        return utf8(write(node, BATTLE_BYTES), BATTLE_BYTES);
    }
    @Override public BattleInput input(BattleState battle) {
        BattlePayloadCodec.validate(battle, BATTLE_BYTES);
        return BattlePayloadCodec.input(BattlePayloadCodec.parsePayload(battle.input(), BATTLE_BYTES));
    }
    @Override public JsonPayload result(BattleResult result) {
        return new JsonPayload(1, utf8(write(boundedResult(result), BATTLE_BYTES), BATTLE_BYTES));
    }
    @Override public BattleReport report(BattleState battle) {
        input(battle); // 同时核对持久摘要、原结果元数据及守恒，不能只因为主行是 SETTLED 就返回结果。
        var result = battle.result() == null ? null : BattlePayloadCodec.result(BattlePayloadCodec.parsePayload(battle.result(), BATTLE_BYTES));
        return new BattleReport(battle.battleId(), battle.marchId(), battle.status(), battle.rulesVersion(), battle.inputHash(), result);
    }
    @Override public byte[] encodeDispatch(Optional<BattleDispatch> dispatch) {
        var node = object().put("schema", 1);
        if (dispatch.isEmpty()) node.putNull("dispatch");
        else {
            var value = dispatch.orElseThrow(); var saved = value.candidate();
            var decoded = input(saved.battle());
            if (!decoded.equals(value.input())) throw new CodecException("Dispatch differs from persisted fixed input");
            var item = object(); item.set("candidate", candidate(new VersionedState(saved.version(), saved.battle())));
            // 输入由候选原文解码重建，不复制第二份可独立修改的权威输入。
            node.set("dispatch", item);
        }
        return write(node, WORK_BYTES);
    }
    @Override public Optional<BattleDispatch> decodeDispatch(byte[] bytes) {
        var node = schema(parse(utf8(bytes, WORK_BYTES), WORK_BYTES), "schema", "dispatch").get("dispatch");
        if (node.isNull()) return Optional.empty();
        fields(node, "candidate"); var row = candidate(node.get("candidate"));
        if (!(row.state() instanceof BattleState battle)) throw new CodecException("Dispatch requires battle candidate");
        return Optional.of(new BattleDispatch(new RealmWorkReader.PendingBattle(row.version(), battle), input(battle)));
    }
    @Override public byte[] encodeReport(BattleReport report) {
        var node = object().put("schema", 1).put("battleId", report.battleId()).put("marchId", report.marchId())
                .put("status", report.status().code()).put("rulesVersion", report.rulesVersion()).put("inputHash", report.inputHash());
        node.set("result", report.result() == null ? MAPPER.nullNode() : boundedResult(report.result()));
        return write(node, WORK_BYTES);
    }
    @Override public BattleReport decodeReport(byte[] bytes) {
        var node = schema(parse(utf8(bytes, WORK_BYTES), WORK_BYTES), "schema", "battleId", "marchId", "status", "rulesVersion", "inputHash", "result");
        return new BattleReport(text(node, "battleId"), text(node, "marchId"), BattleState.Status.fromCode(integer(node, "status")),
                text(node, "rulesVersion"), text(node, "inputHash"), node.get("result").isNull() ? null : decodeResult(node.get("result")));
    }
    @Override public byte[] encodePlan(MarchPlan value) {
        var node = object().put("schema", 1); node.set("plan", plan(value)); return write(node, PLAN_BYTES);
    }
    @Override public MarchPlan decodePlan(byte[] bytes) {
        return plan(schema(parse(utf8(bytes, PLAN_BYTES), PLAN_BYTES), "schema", "plan").get("plan"));
    }
    @Override public int maxWorkBytes() { return WORK_BYTES; }

    private static ObjectNode candidate(VersionedState value) {
        account(value);
        if (value.state() instanceof BattleState battle) BattlePayloadCodec.validate(battle, BATTLE_BYTES);
        var node = object().put("version", value.version()); node.set("state", StateCodec.state(value.state())); return node;
    }
    private static VersionedState candidate(JsonNode node) {
        fields(node, "version", "state"); var row = new VersionedState(number(node, "version"), StateCodec.state(node.get("state")));
        account(row);
        if (row.state() instanceof BattleState battle) BattlePayloadCodec.validate(battle, BATTLE_BYTES);
        return row;
    }
    private static long account(VersionedState candidate) {
        return PlayerIdentity.accountOf(switch (candidate.state()) {
            case TaskState task -> task.playerId();
            case BattleState battle -> battle.playerId();
            default -> throw new CodecException("Unsupported background candidate");
        });
    }
    private static void requireAccount(long account, VersionedState candidate) {
        if (account <= 0 || account != account(candidate)) throw new CodecException("Candidate belongs to another account");
    }
    private static String operation(VersionedState candidate) { return candidate.state() instanceof TaskState ? "DUE_TASK" : "BATTLE_RESULT"; }
    private static void validate(BackgroundOutcome outcome) {
        if ((!outcome.operation().equals("DUE_TASK") && !outcome.operation().equals("BATTLE_RESULT"))
                || (!outcome.status().equals("APPLIED") && !outcome.status().equals("BLOCKED"))
                || outcome.accountId() <= 0 || outcome.generation() <= 0)
            throw new CodecException("Invalid background outcome");
        new ObjectKey.Task(outcome.objectId());
    }
    private static ObjectNode boundedResult(BattleResult value) {
        var node = BattlePayloadCodec.encode(value); write(node, BATTLE_BYTES); return node;
    }
    private static BattleResult decodeResult(JsonNode node) {
        write(node, BATTLE_BYTES); return BattlePayloadCodec.result(node);
    }
    private static ObjectNode plan(MarchPlan value) {
        return object().put("marchId", value.marchId()).put("version", value.version()).put("siteX", value.siteX()).put("siteY", value.siteY());
    }
    private static MarchPlan plan(JsonNode node) {
        fields(node, "marchId", "version", "siteX", "siteY");
        return new MarchPlan(text(node, "marchId"), number(node, "version"), node.get("siteX").isNull() ? null : integer(node, "siteX"),
                node.get("siteY").isNull() ? null : integer(node, "siteY"));
    }
    private static JsonNode schema(JsonNode node, String... names) {
        fields(node, names); if (integer(node, "schema") != 1) throw new CodecException("Unsupported background schema", true); return node;
    }
    private static String hash(byte[] bytes) { return HexFormat.of().formatHex(PersistenceCodec.hash(bytes)); }
}

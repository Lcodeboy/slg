package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceCodecTest {
    private final PersistenceCodec codec = new PersistenceCodec();
    private static final String PLAYER = "17_7";

    @Test void completeDecisionRoundTripsAllEightStateKindsWithoutChangingOriginalJson() {
        var decision = allStates();
        byte[] encoded = codec.encodeDecision(decision);
        assertEquals(decision, codec.decodeDecision(encoded));
        JsonNode root = tree(encoded);
        assertEquals(1, root.get("schema").intValue());
        for (JsonNode change : root.get("changes")) {
            assertTrue(change.get("key").get("kind").isInt());
            assertTrue(change.get("afterState").get("kind").isInt());
        }
        assertEquals(" { \"ok\" : true } \n", codec.decodeDecision(encoded).resultJson());
        assertEquals("{\"amount\":9007199254740993}", codec.decodeDecision(encoded).factJson());
    }

    @Test void enumMapsUseSortedNumericEntriesAndReadDependenciesUseTypedKeyOrder() {
        var decision = allStates();
        var march = tree(codec.encodeDecision(decision)).get("changes").get(4).get("afterState");
        assertEquals(1, march.get("cargo").get(0).get("code").intValue());
        assertEquals(2, march.get("cargo").get(1).get("code").intValue());
        assertEquals(1, march.get("troops").get(0).get("code").intValue());
        assertEquals(3, march.get("troops").get(1).get("code").intValue());
        var alternate = new HashMap<ObjectKey, Long>();
        decision.readVersions().entrySet().stream().sorted(Map.Entry.comparingByKey(ObjectKey.ORDER.reversed()))
                .forEach(entry -> alternate.put(entry.getKey(), entry.getValue()));
        var reordered = new CommitDecision(decision.request(), decision.digest(), decision.factType(), decision.factJson(),
                decision.resultJson(), decision.changes(), alternate);
        assertArrayEquals(codec.encodeDecision(decision), codec.encodeDecision(reordered));
    }

    @Test void schemaAndUnknownFieldsFailClosedAtEveryTypedBoundary() {
        var unsupported = mutate(root -> root.put("schema", 2));
        assertTrue(assertThrows(CodecException.class, () -> codec.decodeDecision(unsupported)).unsupportedSchema());
        rejects(root -> root.put("unexpected", true));
        rejects(root -> ((ObjectNode) root.get("request")).put("newField", 1));
        rejects(root -> ((ObjectNode) root.get("changes").get(0).get("afterState")).put("unknown", 1));
        rejects(root -> ((ObjectNode) root.get("changes").get(0).get("key")).put("newKeyPart", "x"));
        rejects(root -> ((ObjectNode) root.get("changes").get(0).get("afterState")).remove("name"));
    }

    @Test void integersAreCodesRatherThanOrdinalNamesOrCoercedNumbers() {
        rejects(root -> state(root, 1).put("resourceType", "WOOD"));
        rejects(root -> state(root, 1).put("resourceType", 0));
        rejects(root -> state(root, 1).put("resourceType", 99));
        rejects(root -> state(root, 1).put("resourceType", 1.0));
        rejects(root -> state(root, 1).putNull("resourceType"));
        rejects(root -> state(root, 1).put("amount", "42"));
        rejects(root -> state(root, 1).put("amount", new java.math.BigInteger("9223372036854775808")));
        rejects(root -> state(root, 1).put("kind", 99));
        rejects(root -> state(root, 6).put("type", 0));
        rejects(root -> state(root, 6).put("status", 99));
    }

    @Test void duplicateKeysMapCodesAndMalformedKeyStateBindingsAreNotSilentlyCollapsed() {
        rejects(root -> {
            var reads = (ArrayNode) root.get("readVersions"); reads.add(reads.get(0).deepCopy());
        });
        rejects(root -> {
            var cargo = (ArrayNode) state(root, 4).get("cargo"); cargo.add(cargo.get(0).deepCopy());
        });
        rejects(root -> ((ObjectNode) root.get("changes").get(1).get("key")).put("resourceType", 2));
        String json = new String(codec.encodeDecision(allStates()), StandardCharsets.UTF_8);
        assertThrows(CodecException.class, () -> codec.decodeDecision(json.replaceFirst("\"schema\":1", "\"schema\":1,\"schema\":1")
                .getBytes(StandardCharsets.UTF_8)));
    }

    @Test void invalidJsonTrailingDocumentsUtf8AndByteBudgetsAreRejected() {
        assertThrows(CodecException.class, () -> codec.validateJson("{} {}"));
        assertThrows(CodecException.class, () -> codec.validateJson("{\"x\":1,\"x\":2}"));
        assertThrows(CodecException.class, () -> codec.validateJson("[]"));
        assertThrows(CodecException.class, () -> codec.validateJson("{\"text\":\"\\ud800\"}"));
        assertThrows(CodecException.class, () -> codec.validateJson("{\"text\":\"" + '\ud800' + "\"}"));
        assertThrows(CodecException.class, () -> codec.decodeJson(new byte[]{(byte) 0xc3, 0x28}));
        assertThrows(CodecException.class, () -> new PersistenceCodec(128, 64).encodeDecision(allStates()));
        assertThrows(CodecException.class, () -> new PersistenceCodec(128, 64).decodeDecision(codec.encodeDecision(allStates())));
        assertThrows(CodecException.class, () -> new PersistenceCodec(128, 32).validateJson("{\"large\":\"" + "x".repeat(32) + "\"}"));
    }

    @Test void customBudgetsCanOnlyTightenSqlReadableFormatLimits() {
        assertThrows(IllegalArgumentException.class,
                () -> new PersistenceCodec(PersistenceCodec.DEFAULT_MAX_BYTES + 1, 1024));
        assertThrows(IllegalArgumentException.class,
                () -> new PersistenceCodec(PersistenceCodec.DEFAULT_MAX_BYTES, PersistenceCodec.DEFAULT_MAX_JSON_BYTES + 1));
        assertThrows(IllegalArgumentException.class, () -> new PersistenceCodec(128, 129));
        assertDoesNotThrow(() -> new PersistenceCodec(128, 64));
    }

    @Test void battleControlMetadataHashesAndSchemaMustMatchFixedPayloads() {
        BattleState battle = battle(false);
        codec.validateState(battle);
        assertThrows(CodecException.class, () -> codec.validateState(copyBattle(battle, "other", battle.inputHash(), battle.input())));
        assertThrows(CodecException.class, () -> codec.validateState(copyBattle(battle, battle.battleId(), "00", battle.input())));
        assertThrows(CodecException.class, () -> codec.validateState(copyBattle(battle, battle.battleId(), battle.inputHash(),
                new JsonPayload(2, battle.input().json()))));
        String changed = battle.input().json().replace("\"type\":1", "\"type\":\"INFANTRY\"");
        assertThrows(CodecException.class, () -> codec.validateState(copyBattle(battle, battle.battleId(),
                HexFormat.of().formatHex(PersistenceCodec.hash(changed.getBytes(StandardCharsets.UTF_8))), new JsonPayload(1, changed))));
    }

    @Test void settledBattleResultMustMatchInputGroupsRuleAndSeed() {
        BattleState settled = battle(true);
        codec.validateState(settled);
        String result = settled.result().json().replace("\"seed\":9", "\"seed\":10");
        var mismatch = new BattleState(settled.battleId(), settled.marchId(), settled.playerId(), settled.status(),
                settled.heroIds(), settled.inputHash(), settled.rulesVersion(), settled.marchVersion(), settled.input(), new JsonPayload(1, result));
        assertThrows(CodecException.class, () -> codec.validateState(mismatch));
    }

    @Test void hashUsesExactBytesAndReturnsIndependentDigest() {
        byte[] first = PersistenceCodec.hash("{}".getBytes(StandardCharsets.UTF_8));
        assertEquals("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", HexFormat.of().formatHex(first));
        assertFalse(java.util.Arrays.equals(first, PersistenceCodec.hash(" {}".getBytes(StandardCharsets.UTF_8))));
        first[0] ^= 1;
        assertNotEquals(first[0], PersistenceCodec.hash("{}".getBytes(StandardCharsets.UTF_8))[0]);
    }

    private CommitDecision allStates() {
        List<ObjectState> states = List.of(new PlayerState(PLAYER, 7, 17, 17, "角色"),
                new ResourceState(PLAYER, ResourceType.WOOD, Long.MAX_VALUE),
                new TroopState(PLAYER, TroopType.INFANTRY, 100, 2, 3), new HeroState(PLAYER, "hero", "march"),
                new MarchState("march", PLAYER, MarchState.Status.RETURNING, MarchState.Mission.GATHER,
                        1.25, 2.5, 3, 4, 1000, 2000, 1, Map.of(TroopType.MARKSMAN, 2L, TroopType.INFANTRY, 5L),
                        List.of("hero2", "hero1"), "task", "rules", Map.of(ResourceType.FOOD, 7L, ResourceType.WOOD, 9L)),
                new GatherSiteState(3, 4, PLAYER, "march", 1),
                new TaskState("task", PLAYER, "march", TaskState.Type.MARCH_RETURN, TaskState.Status.PENDING, 1, 2000, null),
                battle(false));
        var changes = new ArrayList<ObjectChange>(); var reads = new HashMap<ObjectKey, Long>();
        states.forEach(state -> { changes.add(new ObjectChange(state.key(), 0, state)); reads.put(state.key(), 0L); });
        return new CommitDecision(new RequestKey("player:" + PLAYER, "r1"), "digest", "FIXTURE",
                "{\"amount\":9007199254740993}", " { \"ok\" : true } \n", changes, reads);
    }
    private BattleState battle(boolean settled) {
        var input = new BattleInput("battle", new Army(PLAYER, List.of(new TroopGroup("a", TroopType.INFANTRY, 1, 100))),
                new Army("npc", List.of(new TroopGroup("d", TroopType.MARKSMAN, 1, 1))), 9, 100);
        var payload = codec.encodeBattleInput(input);
        return new BattleState("battle", "march", PLAYER, settled ? BattleState.Status.SETTLED : BattleState.Status.PENDING,
                List.of("hero"), HexFormat.of().formatHex(PersistenceCodec.hash(payload.json().getBytes(StandardCharsets.UTF_8))),
                ExpeditionBattleEngine.RULES_VERSION, 1, payload, settled ? codec.encodeBattleResult(new ExpeditionBattleEngine().simulate(input)) : null);
    }
    private static BattleState copyBattle(BattleState original, String battleId, String hash, JsonPayload input) {
        return new BattleState(battleId, original.marchId(), original.playerId(), original.status(), original.heroIds(),
                hash, original.rulesVersion(), original.marchVersion(), input, original.result());
    }
    private byte[] mutate(Consumer<ObjectNode> mutation) {
        ObjectNode root = (ObjectNode) tree(codec.encodeDecision(allStates())); mutation.accept(root);
        return StrictJson.write(root, PersistenceCodec.DEFAULT_MAX_BYTES);
    }
    private void rejects(Consumer<ObjectNode> mutation) { assertThrows(CodecException.class, () -> codec.decodeDecision(mutate(mutation))); }
    private static ObjectNode state(ObjectNode root, int index) { return (ObjectNode) root.get("changes").get(index).get("afterState"); }
    private static JsonNode tree(byte[] bytes) { return StrictJson.parse(new String(bytes, StandardCharsets.UTF_8), PersistenceCodec.DEFAULT_MAX_BYTES); }
}

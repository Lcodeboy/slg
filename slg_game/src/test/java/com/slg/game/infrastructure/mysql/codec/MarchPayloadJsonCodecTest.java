package com.slg.game.infrastructure.mysql.codec;

import com.slg.core.battle.TroopType;
import com.slg.game.application.march.*;
import com.slg.game.application.player.*;
import com.slg.game.domain.march.StartMarchOrder;
import com.slg.game.domain.persistence.MarchState;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MarchPayloadJsonCodecTest {
    private final MarchPayloadJsonCodec codec = new MarchPayloadJsonCodec();

    @Test void canonicalDigestPreservesPresenceUnknownCodesAndDuplicatesButExcludesPlanningAndSession() {
        var a = new StartMarchOrder.TroopSelection(1, 30);
        var b = new StartMarchOrder.TroopSelection(2, 10);
        var order = new StartMarchOrder(null, 150, 150, null, List.of(a, b), null);
        assertEquals(codec.startDigest(order), codec.startDigest(new StartMarchOrder(null, 150, 150, null, List.of(b, a), null)));
        assertNotEquals(codec.startDigest(order), codec.startDigest(new StartMarchOrder(1, 150, 150, null, List.of(a, b), null)));
        assertNotEquals(codec.startDigest(order), codec.startDigest(new StartMarchOrder(null, 150, 150, null, List.of(a, a, b), null)));
        assertNotEquals(codec.startDigest(order), codec.startDigest(new StartMarchOrder(null, 150, 150, null, List.of(a, b), List.of())));
        var actor = new PlayerAccess(7, 1, 100, UUID.randomUUID().toString(), 3, UUID.randomUUID().toString());
        var unknown = new MarchInvocation(actor, new StartMarchOrder(999, null, 150, 8,
                List.of(new StartMarchOrder.TroopSelection(998, -1)), List.of()), null, null);
        assertEquals(unknown, codec.decodeInvocation(codec.encodeInvocation(unknown)));
        var plan = new MarchPlan("march-1", 2, 150, 150);
        assertEquals(plan, codec.decodePlan(codec.encodePlan(plan)));
        var recall = new MarchInvocation(actor, null, "march-1", plan);
        assertEquals(recall, codec.decodeInvocation(codec.encodeInvocation(recall)));
        String valid = new String(codec.encodeInvocation(recall), StandardCharsets.UTF_8);
        assertThrows(CodecException.class, () -> codec.decodeInvocation(valid.replaceFirst("\\{", "{\"schema\":1,").getBytes(StandardCharsets.UTF_8)));
        assertThrows(CodecException.class, () -> codec.recallDigest("\ud800"));
    }

    @Test void typedOriginalMarchResponsesRoundTripAndCannotBeReclassifiedAsCreationOrAnotherStage() {
        var players = new PlayerPayloadJsonCodec();
        var outbound = march(MarchState.Status.OUTBOUND);
        var returning = march(MarchState.Status.RETURNING);
        assertEquals(outbound, codec.decodeMarch(codec.encodeMarch(outbound)));
        for (var outcome : List.of(PlayerOutcome.started(outbound), PlayerOutcome.recalled(returning),
                PlayerOutcome.rejected(PlayerOutcome.RECALL, "INVALID_MARCH_STATE", "Already returning"))) {
            String json = players.encodeOutcome(outcome);
            assertEquals(outcome, players.decodeOutcome(json));
            assertFalse(json.contains("INFANTRY"));
        }
        assertThrows(IllegalArgumentException.class, () -> PlayerOutcome.started(returning));
        assertThrows(IllegalArgumentException.class, () -> new PlayerOutcome(PlayerOutcome.CREATE, null, outbound, null, null));
        String valid = new String(codec.encodeMarch(outbound), StandardCharsets.UTF_8);
        assertThrows(RuntimeException.class, () -> codec.decodeMarch(valid.replace("\"status\":1", "\"status\":999").getBytes(StandardCharsets.UTF_8)));
    }

    private static MarchState march(MarchState.Status status) {
        return new MarchState("march-1", "1_7", status, MarchState.Mission.GATHER, 100, 100, 150, 150,
                1, 3001, 1, Map.of(TroopType.INFANTRY, 30L), List.of(), "task-1", "gather-v1", Map.of());
    }
}

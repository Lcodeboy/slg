package com.slg.game.infrastructure.mysql.codec;

import com.slg.game.application.player.*;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PlayerPayloadJsonCodecTest {
    private final PlayerPayloadJsonCodec codec = new PlayerPayloadJsonCodec();
    private final RealmIdentity realm = new RealmIdentity(1, UUID.randomUUID());
    private PlayerDetails details() { return PlayerDetails.created(PlayerCreation.initialStates(realm, 7, "城主")); }

    @Test void integerCodedDetailsAndOriginalOutcomeRoundTripWithoutFrameworkTypes() {
        var details = details();
        assertEquals(details, codec.decodeDetails(codec.encodeDetails(details)));
        var outcome = PlayerOutcome.created(details);
        String json = codec.encodeOutcome(outcome);
        assertEquals(outcome, codec.decodeOutcome(json));
        assertTrue(json.contains("\"resourceType\":1")); assertTrue(json.contains("\"troopType\":1"));
        assertFalse(json.contains("INFANTRY")); assertFalse(json.contains("WOOD"));
        var rejected = PlayerOutcome.rejected("PLAYER_EXISTS", "Character already exists");
        assertEquals(rejected, codec.decodeOutcome(codec.encodeOutcome(rejected)));
    }

    @Test void sessionProofStaysOnlyInMemoryInputAndDoesNotChangeRequestDigest() {
        var access = new PlayerAccess(7, 1, 100, UUID.randomUUID().toString(), 3, UUID.randomUUID().toString());
        var invocation = new PlayerInvocation(access, " 城主 ");
        assertEquals(invocation, codec.decodeInvocation(codec.encodeInvocation(invocation)));
        String result = codec.encodeOutcome(PlayerOutcome.created(details()));
        String fact = codec.creationFact(7, "1_7", null);
        assertFalse(result.contains(access.bindingId())); assertFalse(fact.contains(access.gameInstance()));
        assertEquals(codec.createDigest(invocation.value()), codec.createDigest(" 城主 "));
        assertNotEquals(codec.createDigest(" 城主 "), codec.createDigest("城主"));
    }

    @Test void receiptObservationPreservesExactOriginalResultTextAndAbsenceIsDistinct() {
        String text = " { \"ok\" : true } ";
        var receipt = new CommitReceipt(new RequestKey("account:7", "request-1"), 8, "digest", text);
        var found = new RealmReceiptReader.Observation.Found(receipt);
        assertEquals(found, codec.decodeObservation(codec.encodeObservation(found)));
        var absent = new RealmReceiptReader.Observation.NotObserved();
        assertEquals(absent, codec.decodeObservation(codec.encodeObservation(absent)));
    }

    @Test void badSchemaUnknownCodesDuplicateFieldsAndLegacyScalarCannotBecomePlayerData() {
        String valid = codec.encodeOutcome(PlayerOutcome.created(details()));
        assertThrows(CodecException.class, () -> codec.decodeOutcome(valid.replaceFirst("\"schema\":1", "\"schema\":2")));
        assertThrows(RuntimeException.class, () -> codec.decodeOutcome(valid.replace("\"troopType\":1", "\"troopType\":999")));
        assertThrows(CodecException.class, () -> codec.decodeOutcome(valid.replaceFirst("\\{", "{\"schema\":1,")));
        assertThrows(CodecException.class, () -> codec.decodeOutcome("\"old-base64-receipt\""));
    }

    @Test void invalidUnicodeAndOversizedInputFailBeforeQueueAdmission() {
        var access = new PlayerAccess(7, 1, 100, UUID.randomUUID().toString(), 3, UUID.randomUUID().toString());
        assertThrows(CodecException.class, () -> codec.encodeInvocation(new PlayerInvocation(access, "x".repeat(9000))));
        assertThrows(CodecException.class, () -> codec.createDigest("\ud800"));
        assertThrows(CodecException.class, () -> codec.decodeInvocation(new byte[]{(byte) 0xc3, (byte) 0x28}));
        assertThrows(CodecException.class, () -> codec.decodeDetails("{}{}".getBytes(StandardCharsets.UTF_8)));
    }

    @Test void largeLegalOriginalReceiptStillFitsItsEscapedQueryEnvelope() {
        String original = codec.encodeOutcome(PlayerOutcome.rejected("REJECTED", "\"".repeat(400_000)));
        assertTrue(original.getBytes(StandardCharsets.UTF_8).length < 1024 * 1024);
        var found = new RealmReceiptReader.Observation.Found(new CommitReceipt(
                new RequestKey("account:7", "large"), 3, "digest", original));
        byte[] envelope = codec.encodeObservation(found);
        assertTrue(envelope.length > 1024 * 1024);
        assertTrue(envelope.length <= codec.maxResultBytes());
        assertEquals(found, codec.decodeObservation(envelope));
    }
}

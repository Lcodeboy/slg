package com.slg.game.infrastructure.mysql;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.domain.persistence.CommitDecision;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.persistence.WriterToken;
import com.slg.game.infrastructure.mysql.codec.CodecException;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MySqlCommitPayloadSizerTest {
    private final PersistenceCodec codec = new PersistenceCodec();
    private final MySqlCommitPayloadSizer sizer = new MySqlCommitPayloadSizer(codec);

    @Test void countsRealEncodedUtf8AndSeparateRequestSchedulingMetadata() {
        CommitRequest ascii = request("scope", "{\"name\":\"wood\"}");
        CommitRequest unicode = request("scope", "{\"name\":\"木材\"}");
        long asciiBytes = sizer.measure(ascii), unicodeBytes = sizer.measure(unicode);
        assertTrue(asciiBytes > codec.encodeDecision(ascii.decision()).length);
        assertEquals(codec.encodeDecision(unicode.decision()).length - codec.encodeDecision(ascii.decision()).length,
                unicodeBytes - asciiBytes);
        CommitRequest longerScope = request("scope-with-longer-key", "{\"name\":\"wood\"}");
        long payloadDifference = codec.encodeDecision(longerScope.decision()).length - codec.encodeDecision(ascii.decision()).length;
        assertTrue(sizer.measure(longerScope) - asciiBytes > payloadDifference,
                "The separate scheduling request index must also be budgeted");
    }

    @Test void writerAndWaitingPolicyAreFixedMetadataWithoutChangingPersistentDecision() {
        CommitRequest base = request("scope", "{}");
        var alternate = new CommitRequest(new WriterToken(base.writer().realm(), UUID.randomUUID(), Long.MAX_VALUE),
                base.decision(), CommitRequest.Origin.BATTLE_RESULT, Duration.ofSeconds(Long.MAX_VALUE));
        assertEquals(sizer.measure(base), sizer.measure(alternate));
        assertEquals(base.decision(), alternate.decision());
    }

    @Test void invalidAndOversizedJsonAreRejectedDuringMeasurement() {
        assertThrows(CodecException.class, () -> sizer.measure(request("scope", "not-json")));
        String tooLarge = "{\"text\":\"" + "x".repeat(PersistenceCodec.DEFAULT_MAX_JSON_BYTES) + "\"}";
        assertThrows(CodecException.class, () -> sizer.measure(request("scope", tooLarge)));
        assertThrows(NullPointerException.class, () -> sizer.measure(null));
    }

    private CommitRequest request(String scope, String fact) {
        var identity = new RealmIdentity(1, UUID.randomUUID());
        var decision = new CommitDecision(new RequestKey(scope, "request-1"), "digest", "test", fact, "{}", List.of(), Map.of());
        return new CommitRequest(new WriterToken(identity, UUID.randomUUID(), 1), decision,
                CommitRequest.Origin.CLIENT, Duration.ofSeconds(1));
    }
}

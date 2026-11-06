package com.slg.core.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.protocol.Json;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import static org.junit.jupiter.api.Assertions.*;

class TokenServiceTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-11-02T00:00:00Z"), ZoneOffset.UTC);
    private final String testSecret = "unit-test-only-secret-32-characters-or-more";

    @Test void rejectsTamperingWrongKeyAndExpiredTokens() {
        TokenService service = new TokenService(testSecret, clock);
        String token = service.issue("9007199254740993", Duration.ofSeconds(60));
        assertEquals("9007199254740993", service.verify(token).accountId());
        assertThrows(IllegalArgumentException.class, () -> service.verify(token.substring(0, 8) + "x" + token.substring(9)));
        assertThrows(IllegalArgumentException.class, () -> new TokenService(testSecret + "-other", clock).verify(token));
        TokenService expired = new TokenService(testSecret, Clock.offset(clock, Duration.ofSeconds(60)));
        assertThrows(IllegalArgumentException.class, () -> expired.verify(token));
        TokenService beforeIssue = new TokenService(testSecret, Clock.offset(clock, Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> beforeIssue.verify(token));
    }

    @Test void onlyCanonicalPositiveBigintIdsMayIdentifyAnAccount() {
        TokenService service = new TokenService(testSecret, clock);
        String max = Long.toString(Long.MAX_VALUE);
        assertEquals(max, service.verify(service.issue(max, Duration.ofMinutes(1))).accountId());
        for (String invalid : java.util.List.of("", "0", "01", "+1", "-1", "１", "1_1",
                "9223372036854775808", "6c02b34b-c220-4078-9257-522a236ff80c"))
            assertThrows(IllegalArgumentException.class, () -> service.issue(invalid, Duration.ofMinutes(1)));
    }

    @Test void validSignatureCannotUpgradeNumericOrLegacyUuidClaimsIntoNewSessions() throws Exception {
        var tokens = new TokenService(testSecret, clock);
        var encoder = java.util.Base64.getUrlEncoder().withoutPadding();
        var mac = javax.crypto.Mac.getInstance("HmacSHA256");
        var utf8 = java.nio.charset.StandardCharsets.UTF_8;
        mac.init(new javax.crypto.spec.SecretKeySpec(testSecret.getBytes(utf8), "HmacSHA256"));
        long now = clock.instant().getEpochSecond();
        for (Object account : java.util.List.of(1L, "01", "55c8df33-a9cf-4ec0-a6d9-c49219ea440f")) {
            String payload = Json.write(java.util.Map.of("accountId", account, "issuedAt", now, "expiresAt", now + 60));
            String signed = "slg1." + encoder.encodeToString(payload.getBytes(utf8));
            String token = signed + "." + encoder.encodeToString(mac.doFinal(signed.getBytes(utf8)));
            assertThrows(IllegalArgumentException.class, () -> tokens.verify(token));
        }
    }

    @Test void secretMustBeExplicitAndErrorsNeverEchoInput() {
        assertThrows(IllegalArgumentException.class, () -> new TokenService(null, clock));
        assertThrows(IllegalArgumentException.class, () -> new TokenService("short", clock));
        String sensitive = "sensitive-value-not-to-log";
        var error = assertThrows(IllegalArgumentException.class, () -> new TokenService(testSecret, clock).verify(sensitive));
        assertFalse(error.toString().contains(sensitive));
        assertNull(error.getCause());
    }

    @Test void rejectsAmbiguousAndTrailingJson() {
        assertThrows(IllegalArgumentException.class, () -> Json.read(
                "{\"username\":\"a\",\"username\":\"b\"}", JsonNode.class));
        assertThrows(IllegalArgumentException.class, () -> Json.read("{} {}", JsonNode.class));
    }
}

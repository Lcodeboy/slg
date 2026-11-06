package com.slg.game.projection;

import com.slg.core.protocol.Json;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RetentionCursorProbeTest {
    private final UUID volume = UUID.randomUUID();

    @Test void connectionSetupCannotRunPluginsCreateSchemasOrInheritUnboundedWaits() {
        String base = "jdbc:mysql://127.0.0.1:13306/slg_projection";
        assertEquals(base + "?sslMode=REQUIRED&connectTimeout=3000&socketTimeout=3000",
                RetentionCursorProbe.boundedJdbcUrl(base + "?connectTimeout=0&sslMode=REQUIRED&socketTimeout=0"));
        for (String url : new String[]{base + "?createDatabaseIfNotExist=true", base + "?sessionVariables=x=1",
                base + "?queryInterceptors=custom.Hook", base + "?useConfigs=custom", "jdbc:mysql://first,second/db",
                "jdbc:mysql://user@localhost/db", "jdbc:mysql://address=(host=localhost)(user=someone)/db"}) {
            assertThrows(IllegalArgumentException.class, () -> RetentionCursorProbe.boundedJdbcUrl(url));
        }
    }

    @Test void neverFollowsArbitraryHealthTargetsAndDisabledConsumerDoesNotOpenConnections() {
        for (String bind : new String[]{"mysql.example", "10.0.0.1", "127.0.0.1@other"}) {
            var evidence = RetentionCursorProbe.observe(Map.of("SLG_BIND", bind), 1, volume,
                    seq -> { throw new AssertionError("Unexpected original-volume read"); }, 5);
            assertEquals("HEALTH_UNAVAILABLE", evidence.state());
        }
        assertEquals("DISABLED", RetentionCursorProbe.observe(Map.of("SLG_PROJECTION_ENABLED", "false"),
                1, volume, seq -> null, 5).state());
    }

    @Test void unavailableConsumerCannotPromoteItsOldRememberedPosition() throws Exception {
        var http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var health = Map.of("status", "UP", "realmId", 1, "volumeId", volume, "journalSeq", 9,
                "projection", Map.of("state", "RETRYING", "lastAppliedSeq", 5));
        http.createContext("/health", exchange -> {
            byte[] response = Json.write(health).getBytes(StandardCharsets.UTF_8);
            try (exchange) {
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        try {
            http.start();
            var env = Map.of("SLG_GAME_PORT", Integer.toString(http.getAddress().getPort()));
            var cursor = RetentionCursorProbe.observe(env, 1, volume, seq -> null, 9);
            assertEquals("RETRYING", cursor.state());
            assertEquals(5, cursor.appliedSeq());
            assertEquals("HEALTH_IDENTITY_MISMATCH",
                    RetentionCursorProbe.observe(env, 1, UUID.randomUUID(), seq -> null, 9).state());
        } finally {
            http.stop(0);
        }
    }
}

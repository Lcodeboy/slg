package com.slg.gate;

import com.slg.rpc.RpcTransport;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 配置只定义有界传输预算，不能被解释为 AOI 索引或万人混合负载已经验收。 */
class GatewaySettingsTest {
    @Test void defaultMapPoolCoversConfiguredConnectionsWithBoundedChannels() {
        var defaults = GatewaySettings.defaults();
        assertEquals(47, defaults.mapWatchChannels());
        assertEquals(256, defaults.mapWatchStreamsPerChannel());
        assertEquals(defaults, GatewaySettings.fromEnvironment(Map.of()));

        var adjusted = GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAX_CONNECTIONS", "513"));
        assertEquals(3, adjusted.mapWatchChannels(), "默认池应按实际连接预算向上取整");
        assertEquals(256, adjusted.mapWatchStreamsPerChannel());
        var perChannel = GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAX_CONNECTIONS", "513",
                "SLG_GATE_MAP_STREAMS_PER_CHANNEL", "512"));
        assertEquals(2, perChannel.mapWatchChannels());
    }

    @Test void explicitSmallerPoolIsAllowedAndDoesNotInflateConnectionOrQueryBudgets() {
        var configured = GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAX_CONNECTIONS", "12000",
                "SLG_GATE_MAP_CHANNELS", "2", "SLG_GATE_MAP_STREAMS_PER_CHANNEL", "64"));
        assertEquals(2, configured.mapWatchChannels());
        assertEquals(64, configured.mapWatchStreamsPerChannel());
        assertEquals(12000, configured.maxConnections());
        assertEquals(GatewaySettings.defaults().gameInflight(), configured.gameInflight());
        assertEquals(GatewaySettings.defaults().rpcTimeoutMillis(), configured.rpcTimeoutMillis());
    }

    @Test void invalidPoolsFailAtConfigurationBoundary() {
        for (String channels : new String[] {"0", "-1", "129", "not-a-number"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAP_CHANNELS", channels)), channels);
        }
        for (String streams : new String[] {"0", "-1", "513", "not-a-number"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAP_STREAMS_PER_CHANNEL", streams)), streams);
        }
        assertThrows(IllegalArgumentException.class,
                () -> GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAX_CONNECTIONS", "12000",
                        "SLG_GATE_MAP_STREAMS_PER_CHANNEL", "1")), "隐式池也不能超过固定的连接数上限");
        var upper = GatewaySettings.fromEnvironment(Map.of("SLG_GATE_MAP_CHANNELS", "128",
                "SLG_GATE_MAP_STREAMS_PER_CHANNEL", Integer.toString(RpcTransport.MAX_CONCURRENT_CALLS_PER_CONNECTION)));
        assertEquals(128, upper.mapWatchChannels());
        assertEquals(512, upper.mapWatchStreamsPerChannel());
    }
}

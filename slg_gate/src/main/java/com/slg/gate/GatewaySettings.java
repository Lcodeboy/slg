package com.slg.gate;

import java.util.Map;

/** 网关容量与调用预算；这些是可测量的保护边界，不代表已经达到目标吞吐量。 */
record GatewaySettings(int maxConnections, int ipRequestsPerMinute, int authRequestsPerMinute,
                       int commandsPerSecond, int controlFramesPerSecond, int rpcTimeoutMillis,
                       int ucInflight, int gameInflight) {
    GatewaySettings {
        positive(maxConnections, 1_000_000); positive(ipRequestsPerMinute, 10_000_000);
        positive(authRequestsPerMinute, 1_000_000); positive(commandsPerSecond, 100_000);
        positive(controlFramesPerSecond, 100_000); positive(ucInflight, 4096);
        positive(gameInflight, 4096);
        if (rpcTimeoutMillis < 100 || rpcTimeoutMillis > 5000)
            throw new IllegalArgumentException("Invalid gateway timeout configuration");
    }
    static GatewaySettings defaults() { return new GatewaySettings(12_000, 6000, 60, 30, 10, 3000, 32, 256); }
    static GatewaySettings fromEnvironment() { return fromEnvironment(System.getenv()); }
    static GatewaySettings fromEnvironment(Map<String, String> environment) {
        var d = defaults();
        return new GatewaySettings(number(environment, "SLG_GATE_MAX_CONNECTIONS", d.maxConnections),
                number(environment, "SLG_GATE_IP_REQUESTS_PER_MINUTE", d.ipRequestsPerMinute),
                number(environment, "SLG_GATE_AUTH_PER_MINUTE", d.authRequestsPerMinute),
                number(environment, "SLG_GATE_COMMANDS_PER_SECOND", d.commandsPerSecond),
                number(environment, "SLG_GATE_CONTROL_FRAMES_PER_SECOND", d.controlFramesPerSecond),
                number(environment, "SLG_RPC_TIMEOUT_MS", d.rpcTimeoutMillis),
                number(environment, "SLG_RPC_UC_MAX_INFLIGHT", d.ucInflight),
                number(environment, "SLG_RPC_GAME_MAX_INFLIGHT", d.gameInflight));
    }
    private static int number(Map<String, String> env, String name, int fallback) {
        String value = env.get(name);
        if (value == null) return fallback;
        try { return Integer.parseInt(value); }
        catch (NumberFormatException ignored) { throw new IllegalArgumentException("Invalid " + name); }
    }
    private static void positive(int value, int max) {
        if (value < 1 || value > max) throw new IllegalArgumentException("Invalid gateway capacity configuration");
    }
}

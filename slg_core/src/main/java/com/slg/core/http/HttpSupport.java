package com.slg.core.http;

import com.slg.core.protocol.Json;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** JDK HTTP 服务共用的配置读取、请求边界和 JSON 响应工具，不负责路由或身份授权。 */
public final class HttpSupport {
    /** 按原始请求体字节数限制为 64 KiB，不能与 JSON 字符串长度限制混用。 */
    public static final int MAX_BODY_BYTES = 65_536;
    private HttpSupport() {}

    /** 读取普通环境配置，未设置或全为空白时回退；需保留空密码的数据库配置不使用此方法。 */
    public static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    /** 解析实际监听端口，范围为 1～65535；非整数或越界会在启动阶段抛出 IllegalArgumentException。 */
    public static int port(String key, int fallback) {
        int value = Integer.parseInt(env(key, Integer.toString(fallback)));
        if (value < 1 || value > 65_535) throw new IllegalArgumentException("Invalid port: " + key);
        return value;
    }

    /**
     * 从单个 Authorization 值提取 Bearer 令牌，拒绝空令牌、首尾空白和逗号合并值。
     * 仅校验头部格式，不验证签名或时效；调用方还须交给 TokenService 校验。
     */
    public static String bearer(String header) {
        if (header == null || header.length() > 2055 || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            throw new IllegalArgumentException("Bearer session required");
        }
        String token = header.substring(7);
        if (token.isBlank() || !token.equals(token.trim()) || token.indexOf(',') >= 0) {
            throw new IllegalArgumentException("Bearer session required");
        }
        return token;
    }

    /**
     * 最多读取上限加一字节，以实际读取量识别超限，不信任 Content-Length 声明。
     * 返回 UTF-8 解码后的文本；请求交换的关闭仍由调用方负责。
     *
     * @throws BodyTooLargeException 实际请求体超过 64 KiB
     */
    public static String body(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readNBytes(MAX_BODY_BYTES + 1);
        if (bytes.length > MAX_BODY_BYTES) throw new BodyTooLargeException();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 写入完整 JSON 响应，按 UTF-8 字节数设置长度，并禁止缓存和内容类型嗅探。
     * 此方法会关闭响应输出流；序列化失败在发送响应头之前抛出，I/O 错误由调用方处理。
     */
    public static void json(HttpExchange exchange, int status, Object value) throws IOException {
        byte[] bytes = Json.write(value).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }

    /** 独立标记请求体超限，供 HTTP 边界准确映射为 413，而非普通解析错误。 */
    public static final class BodyTooLargeException extends IOException {
        /** 创建不携带请求原文的固定错误。 */
        public BodyTooLargeException() { super("Request body exceeds 64 KiB"); }
    }
}

package com.slg.core.protocol;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 统一 JSON 编解码配置与脱敏异常，避免各入口对重复字段、尾随数据和复杂输入采用不同规则。 */
public final class Json {
    /**
     * 初始化完成后供并发请求复用，运行期不应重新配置。
     * 拒绝重复字段和尾随 JSON；读取深度最多 48 层、单字符串最多 65536 字符、数字文本最多 32 字符。
     * 这些解析约束不替代 HTTP/帧层的整体字节数限制，也不承担业务字段校验。
     */
    public static final ObjectMapper MAPPER = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxNestingDepth(48).maxStringLength(65_536).maxNumberLength(32).build())
            .build()).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private Json() {}

    /** 序列化为 JSON 文本；失败统一转换为不附带原始对象信息的 IllegalArgumentException。 */
    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot serialize response");
        }
    }

    /** 按指定类型解析单个完整 JSON 值；解析失败只返回固定错误，调用方仍须校验业务约束。 */
    public static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            // Jackson 诊断可能包含原始输入中的密码或令牌，不能保留其消息和异常原因链。
            throw new IllegalArgumentException("Invalid JSON request");
        }
    }
}

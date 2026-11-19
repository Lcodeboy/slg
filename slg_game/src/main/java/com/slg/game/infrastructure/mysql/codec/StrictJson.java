package com.slg.game.infrastructure.mysql.codec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** 此 mapper 仅属于新存储格式，不能改动旧协议使用的全局 Json.MAPPER。 */
final class StrictJson {
    static final ObjectMapper MAPPER = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(64)
                    .maxStringLength(8 * 1024 * 1024).maxNumberLength(128).build()).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private StrictJson() {}

    static String utf8(byte[] bytes, int maximum) {
        if (bytes == null || bytes.length == 0 || bytes.length > maximum)
            throw new CodecException("Missing or oversized JSON payload");
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException bad) {
            throw new CodecException("Payload is not valid UTF-8", bad);
        }
    }

    static JsonNode parse(String text, int maximum) {
        if (text == null) throw new CodecException("Missing JSON text");
        byte[] bytes = utf8Bytes(text, maximum);
        if (bytes.length == 0 || bytes.length > maximum) throw new CodecException("JSON payload exceeds byte budget");
        try {
            JsonNode node = MAPPER.readTree(text);
            if (node == null || !node.isObject()) throw new CodecException("Payload must be a JSON object");
            validUnicode(node, maximum);
            return node;
        } catch (CodecException bad) { throw bad;
        } catch (Exception bad) { throw new CodecException("Invalid JSON object", bad); }
    }

    static byte[] write(JsonNode node, int maximum) {
        try {
            validUnicode(node, maximum);
            var bytes = new ByteArrayOutputStream();
            // 限制编码过程本身的分配，而不等任意大的 byte[] 已经生成之后再判断上限。
            var bounded = new OutputStream() {
                @Override public void write(int value) {
                    if (bytes.size() >= maximum) throw new CodecException("Encoded payload exceeds byte budget");
                    bytes.write(value);
                }
                @Override public void write(byte[] buffer, int offset, int length) {
                    if (length > maximum - bytes.size()) throw new CodecException("Encoded payload exceeds byte budget");
                    bytes.write(buffer, offset, length);
                }
            };
            MAPPER.writeValue(bounded, node);
            return bytes.toByteArray();
        } catch (CodecException bad) { throw bad;
        } catch (Exception bad) { throw new CodecException("Cannot encode payload", bad); }
    }

    static ObjectNode object() { return MAPPER.createObjectNode(); }
    static ArrayNode array() { return MAPPER.createArrayNode(); }

    static byte[] utf8Bytes(String text, int maximum) {
        if (text == null || text.length() > maximum) throw new CodecException("Missing or oversized JSON text");
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(text));
            if (buffer.remaining() > maximum) throw new CodecException("JSON text exceeds byte budget");
            byte[] bytes = new byte[buffer.remaining()]; buffer.get(bytes); return bytes;
        } catch (CharacterCodingException bad) { throw new CodecException("JSON text contains invalid Unicode", bad); }
    }

    private static void validUnicode(JsonNode node, int maximum) {
        if (node.isTextual()) utf8Bytes(node.textValue(), maximum);
        else if (node.isObject()) node.fields().forEachRemaining(entry -> {
            utf8Bytes(entry.getKey(), maximum); validUnicode(entry.getValue(), maximum);
        });
        else if (node.isArray()) node.forEach(value -> validUnicode(value, maximum));
    }

    /** 字段全集必须精确匹配，包括可空字段；缺字段与显式 null 不能互相掩盖格式升级。 */
    static JsonNode fields(JsonNode node, String... names) {
        if (node == null || !node.isObject()) throw new CodecException("Expected JSON object");
        Set<String> expected = Set.of(names);
        Set<String> actual = new HashSet<>();
        node.fieldNames().forEachRemaining(actual::add);
        if (!actual.equals(expected)) throw new CodecException("Unknown or missing payload field");
        return node;
    }

    static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) throw new CodecException("Expected string: " + field);
        return value.textValue();
    }
    static String nullableText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value != null && value.isNull()) return null;
        return text(node, field);
    }
    static long number(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong())
            throw new CodecException("Expected exact long: " + field);
        return value.longValue();
    }
    static int integer(JsonNode node, String field) {
        long value = number(node, field);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) throw new CodecException("Integer overflow: " + field);
        return (int) value;
    }
    static double decimal(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isNumber() || !Double.isFinite(value.doubleValue()))
            throw new CodecException("Expected finite number: " + field);
        return value.doubleValue();
    }
    static ArrayNode list(JsonNode node, String field, int maximum) {
        JsonNode value = node.get(field);
        if (value == null || !value.isArray() || value.size() > maximum)
            throw new CodecException("Missing or oversized array: " + field);
        return (ArrayNode) value;
    }
}

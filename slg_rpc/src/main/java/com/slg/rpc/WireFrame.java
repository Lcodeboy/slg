package com.slg.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.slg.rpc.v1.BusinessError;

import java.util.Objects;
import java.util.UUID;

/**
 * 一个完整逻辑消息；网络编码器一次接收它，业务代码不能把头和正文分两次交给 channel。
 * body 只包含独立 protobuf 正文。请求 ID 同时用于关联响应和业务幂等，绝不是连接内流水号。
 */
public record WireFrame(int kind, int msgId, UUID requestId, int status, ByteString body) {
    public static final int REQUEST = 1, RESPONSE = 2, PUSH = 3;
    public static final UUID NO_REQUEST = new UUID(0, 0);

    public WireFrame {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(body, "body");
        if (kind != REQUEST && kind != RESPONSE && kind != PUSH)
            throw new IllegalArgumentException("Unknown message kind");
        if (msgId <= 0) throw new IllegalArgumentException("msgId must be in 1..2147483647");
        if (kind == PUSH ? !requestId.equals(NO_REQUEST) : requestId.equals(NO_REQUEST))
            throw new IllegalArgumentException("Only PUSH has a zero requestId");
        if (!WireStatus.valid(status) || kind != RESPONSE && status != WireStatus.OK)
            throw new IllegalArgumentException("Invalid status for message kind");
        // 失败回包不会再用成功类型解码；空正文虽是合法 protobuf，不能冒充有原因的失败。
        if (status != WireStatus.OK) {
            try {
                if (BusinessError.parseFrom(body).getCode().isBlank())
                    throw new IllegalArgumentException("Error response requires a business error code");
            } catch (InvalidProtocolBufferException invalid) {
                throw new IllegalArgumentException("Malformed error response body", invalid);
            }
        }
    }

    public static WireFrame request(int msgId, UUID requestId, MessageLite body) {
        return new WireFrame(REQUEST, msgId, requestId, WireStatus.OK, body.toByteString());
    }
    public static WireFrame response(int msgId, UUID requestId, MessageLite body) {
        return new WireFrame(RESPONSE, msgId, requestId, WireStatus.OK, body.toByteString());
    }
    public static WireFrame failure(int msgId, UUID requestId, int status, BusinessError error) {
        if (status == WireStatus.OK) throw new IllegalArgumentException("Failure status must be nonzero");
        return new WireFrame(RESPONSE, msgId, requestId, status, error.toByteString());
    }
    public static WireFrame push(int msgId, MessageLite body) {
        return new WireFrame(PUSH, msgId, NO_REQUEST, WireStatus.OK, body.toByteString());
    }
}

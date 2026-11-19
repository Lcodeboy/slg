package com.slg.game.application.execution;

import java.util.Objects;

/** 队列只保留不可变范围、注册的用例类型和私有字节副本，不接受每请求 Runnable/活跃实体闭包。 */
public final class OperationCommand {
    private final OperationScope scope;
    private final String type;
    private final byte[] payload;

    public OperationCommand(OperationScope scope, String type, byte[] payload) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.type = Objects.requireNonNull(type, "type");
        if (type.isEmpty() || type.length() > 128 || type.chars().anyMatch(c -> c < 33 || c > 126))
            throw new IllegalArgumentException("Invalid operation type");
        this.payload = Objects.requireNonNull(payload, "payload").clone();
    }
    public OperationScope scope() { return scope; }
    public String type() { return type; }
    public byte[] payload() { return payload.clone(); }
    public int payloadLength() { return payload.length; }
    long retainedBytes() { return scope.metadataBytes() + 64L + type.length() + payload.length; }
}

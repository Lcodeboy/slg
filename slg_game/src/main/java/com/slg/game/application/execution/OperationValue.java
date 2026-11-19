package com.slg.game.application.execution;

import java.util.Objects;

/** 查询/会话控制的不可变结果；不携带活跃实体，调用方不能修改执行器持有的结果字节。 */
public final class OperationValue {
    private final byte[] bytes;
    public OperationValue(byte[] bytes) { this.bytes = Objects.requireNonNull(bytes, "bytes").clone(); }
    public byte[] bytes() { return bytes.clone(); }
    public int size() { return bytes.length; }
}

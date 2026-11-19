package com.slg.game.infrastructure.mysql.codec;

/** 载荷错误与不支持版本分开分类；不含原始资产或连接信息，不能作为提交已回滚的证明。 */
public final class CodecException extends IllegalArgumentException {
    private final boolean unsupportedSchema;

    public CodecException(String message) { this(message, false, null); }
    public CodecException(String message, Throwable cause) { this(message, false, cause); }
    public CodecException(String message, boolean unsupportedSchema) { this(message, unsupportedSchema, null); }
    private CodecException(String message, boolean unsupportedSchema, Throwable cause) {
        super(message, cause);
        this.unsupportedSchema = unsupportedSchema;
    }
    public boolean unsupportedSchema() { return unsupportedSchema; }
}

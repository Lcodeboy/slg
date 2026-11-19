package com.slg.game.application.port;

import java.util.Objects;

/**
 * 存储访问不能按契约完成的分类错误；消息须适合诊断，不应携带连接凭据或敏感记录。
 * 本异常自身不证明任何已启动的业务事务是否提交；事务结果未知必须由事务端口明确
 * 返回 Uncertain，调用方不能仅凭异常类型释放未决操作的执行资格。
 */
public class StorageAccessException extends Exception {
    private static final long serialVersionUID = 1L;

    public enum Code {
        UNAVAILABLE, IDENTITY_MISMATCH, UNSUPPORTED_SCHEMA, INVALID_DATA,
        CURSOR_EXPIRED, OWNERSHIP_CONFLICT, READ_BUDGET_EXCEEDED
    }

    private final Code code;

    public StorageAccessException(Code code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public StorageAccessException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    public Code code() { return code; }
}

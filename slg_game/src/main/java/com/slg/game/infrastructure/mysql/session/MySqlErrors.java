package com.slg.game.infrastructure.mysql.session;

import com.slg.game.application.port.StorageAccessException;
import java.sql.SQLDataException;
import java.sql.SQLException;

/** 基础设施异常分类；不把驱动消息、URL 或记录内容带到应用端口。 */
public final class MySqlErrors {
    private MySqlErrors() {}

    public static StorageAccessException access(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof StorageAccessException access) return access;
            if (cause instanceof SQLDataException)
                return new StorageAccessException(StorageAccessException.Code.INVALID_DATA, "Invalid authority column value");
            // 移动扫描依赖显式索引，缺表/列/索引均为部署 schema 不完整；不能伪装临时断连
            // 让 AOI 永久重试，更不能绕开索引退化成全历史扫描。本层不自动建表或补索引。
            if (cause instanceof SQLException sql && (sql.getErrorCode() == 1146 || sql.getErrorCode() == 1054
                    || sql.getErrorCode() == 1176))
                return new StorageAccessException(StorageAccessException.Code.UNSUPPORTED_SCHEMA, "Authority schema is incomplete");
        }
        return new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "Authority database access failed");
    }

    public static boolean constraintConflict(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause())
            if (cause instanceof SQLException sql && sql.getErrorCode() == 1062) return true;
        return false;
    }

    /**
     * 仅识别正常锁竞争；调用者仍须确认整笔 rollback 完成且从未发出 COMMIT。
     * 1205 默认只回滚当前语句，不能直接据错误号宣布整笔业务未提交；1213 也不触发自动重投。
     */
    public static boolean lockConflict(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause())
            if (cause instanceof SQLException sql && (sql.getErrorCode() == 1213 || sql.getErrorCode() == 1205)) return true;
        return false;
    }
}

package com.slg.uc.persistence;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * 为 MySQL 受控建表语句显式指定事务引擎和区分大小写的排序规则，拒绝其他数据库。
 * 本工具只负责建表和表级配置检查，不承担版本迁移或已有列定义的校验。
 */
public final class JdbcSchema {
    private static final Pattern CREATE = Pattern.compile(
            "(?is)^CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(.*\\)$");

    private JdbcSchema() { }

    /**
     * 执行一条不带末尾分号的 CREATE TABLE IF NOT EXISTS 语句，表名必须是简单字母数字标识符。
     * MySQL 建表后还会检查已有表的引擎和默认排序规则，防止 IF NOT EXISTS 掩盖不兼容配置。
     * 调用方持有并负责关闭连接；DDL 不应放入依赖整体回滚的业务事务。
     *
     * @throws IllegalArgumentException 语句不符合本工具支持的建表格式
     * @throws SQLException 建表失败，或已有 MySQL 表需要先迁移存储配置
     */
    public static void createTable(Connection connection, String sql) throws SQLException {
        String statement = sql.strip();
        var matcher = CREATE.matcher(statement);
        if (!matcher.matches()) throw new IllegalArgumentException("Expected a CREATE TABLE statement");
        if (!"MySQL".equalsIgnoreCase(connection.getMetaData().getDatabaseProductName()))
            throw new SQLException("Only MySQL is supported", "08001");
        try (var command = connection.createStatement()) {
            command.execute(statement + " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin");
        }
        // IF NOT EXISTS 不会迁移旧表；拒绝不支持事务或不区分请求编号大小写的表级配置。
        // 此处只读取表级元数据，不检查列是否单独覆盖了排序规则。
        try (var check = connection.prepareStatement(
                "SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?")) {
            check.setString(1, matcher.group(1));
            try (var rows = check.executeQuery()) {
                if (!rows.next() || !"InnoDB".equalsIgnoreCase(rows.getString(1))
                        || !"utf8mb4_0900_bin".equalsIgnoreCase(rows.getString(2)))
                    throw new SQLException("Existing table storage settings require migration", "HY000");
            }
        }
    }
}

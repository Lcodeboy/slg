package com.slg.core.auth;

/** UC 使用 MySQL 有符号 BIGINT 自增；在线协议使用其十进制字符串，避免客户端把大 ID 舍入。 */
public final class AccountIds {
    private static final String MAX = Long.toString(Long.MAX_VALUE);
    private AccountIds() {}

    /** 一个数据库 ID 只能有一种文本表示，否则同一账号可能得到两个邮箱、两份回执或两个角色。 */
    public static boolean isValid(String id) {
        if (id == null || id.isEmpty() || id.length() > MAX.length() || id.charAt(0) == '0') return false;
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return id.length() < MAX.length() || id.compareTo(MAX) <= 0;
    }
}

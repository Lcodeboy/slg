package com.slg.game.domain;

/** 可预期的业务拒绝，携带稳定错误码；与数据库不可用等基础设施故障区分处理。 */
public final class GameException extends RuntimeException {
    private final String code;
    /** message 会返回给客户端，因此不得包含密码、SQL 或内部堆栈等敏感信息。 */
    public GameException(String code, String message) { super(message); this.code = code; }
    /** 返回供客户端判断和运维聚合的稳定业务错误码。 */
    public String code() { return code; }
    /** 业务前置条件断言，失败后由命令或任务边界决定回滚、保存回执或隔离任务。 */
    public static void require(boolean condition, String code, String message) {
        if (!condition) throw new GameException(code, message);
    }
}

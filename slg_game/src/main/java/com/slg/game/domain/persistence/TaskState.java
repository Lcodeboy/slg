package com.slg.game.domain.persistence;

import com.slg.core.type.EnumCodes;
import com.slg.core.type.IntCodeEnum;

import java.util.Map;
import java.util.Objects;

/**
 * 当前行军定时任务的权威行，不再由通用 JSON 对象派生第二份待办状态。按状态/到期时间
 * 建索引；发现 PENDING 只是候选，实际执行仍复核行军 generation 和归属。
 * 完成、取消、阻塞通过状态转换保留原行与版本，不能通过删除记录表示业务已经成功。
 */
public record TaskState(String taskId, String playerId, String marchId, Type type, Status status,
                        long generation, long dueAt, String reason) implements ObjectState {
    public TaskState {
        new ObjectKey.Task(taskId);
        PersistenceChecks.identifier(playerId, "playerId", 256);
        PersistenceChecks.identifier(marchId, "marchId", 256);
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(status, "status");
        PersistenceChecks.positive(generation, "generation");
        PersistenceChecks.nonNegative(dueAt, "dueAt");
        if (reason != null && (reason.isBlank() || reason.length() > 2048))
            throw new IllegalArgumentException("Task reason must be nonblank and bounded when supplied");
        if (status == Status.BLOCKED && reason == null)
            throw new IllegalArgumentException("A blocked task must retain its reason");
    }

    @Override public ObjectKey.Task key() { return new ObjectKey.Task(taskId); }

    /** 类型编码决定后续执行语义；不取 ordinal、不改值或复用，未知编码须拒绝而非派发默认任务。 */
    public enum Type implements IntCodeEnum {
        MARCH_ARRIVE(1), GATHER_DONE(2), MARCH_RETURN(3);

        private static final Map<Integer, Type> BY_CODE = EnumCodes.index(values());
        private final int code;
        Type(int code) { this.code = code; }
        @Override public int code() { return code; }
        public static Type fromCode(int code) { return EnumCodes.require(BY_CODE, code, "TaskState.Type"); }
    }

    /** 状态编码独立于 ordinal；发布后不改值、不复用，未知编码不能解释为待执行或已完成。 */
    public enum Status implements IntCodeEnum {
        PENDING(1), DONE(2), CANCELLED(3), BLOCKED(4);

        private static final Map<Integer, Status> BY_CODE = EnumCodes.index(values());
        private final int code;
        Status(int code) { this.code = code; }
        @Override public int code() { return code; }
        public static Status fromCode(int code) { return EnumCodes.require(BY_CODE, code, "TaskState.Status"); }
    }
}

package com.slg.rpc;

import com.slg.rpc.v1.PlayReply;
import com.slg.rpc.v1.WireResultStatus;

/**
 * 外部协议只暴露应用结果语义；数值的唯一声明在 protobuf enum。
 * 内部 RPC 暂用既有 PlayReply 状态。转换必须优先看具体错误原因，不能把所有 503 当成未执行：
 * 写操作已送到 GAME 后的网络失败或提交确认失败，都只能标为 OUTCOME_UNKNOWN。
 */
public final class WireStatus {
    public static final int OK = WireResultStatus.WIRE_OK_VALUE;
    public static final int REJECTED = WireResultStatus.WIRE_REJECTED_VALUE;
    public static final int NOT_ACCEPTED = WireResultStatus.WIRE_NOT_ACCEPTED_VALUE;
    public static final int OUTCOME_UNKNOWN = WireResultStatus.WIRE_OUTCOME_UNKNOWN_VALUE;
    public static final int PROTOCOL_ERROR = WireResultStatus.WIRE_PROTOCOL_ERROR_VALUE;
    public static final int AUTH_REQUIRED = WireResultStatus.WIRE_AUTH_REQUIRED_VALUE;
    private WireStatus() {}

    public static boolean valid(int status) { return WireResultStatus.forNumber(status) != null; }

    public static int fromReply(PlayReply reply) {
        if (reply.hasBody() && reply.getStatus() == 200) return OK;
        if (!reply.hasError() || reply.getError().getCode().isBlank())
            throw new IllegalArgumentException("Incomplete internal GAME reply");
        return fromError(reply.getStatus(), reply.getError().getCode());
    }

    public static int fromError(int internalStatus, String code) {
        return switch (code) {
            case "RESULT_UNKNOWN", "STORAGE_UNAVAILABLE", "UPSTREAM_TIMEOUT_UNKNOWN", "UPSTREAM_UNAVAILABLE_UNKNOWN" -> OUTCOME_UNKNOWN;
            case "UPSTREAM_BUSY", "SERVER_BUSY", "RATE_LIMITED", "PLAYER_LOAD_UNAVAILABLE", "TARGET_LOAD_UNAVAILABLE",
                 "DEADLINE_EXCEEDED", "REQUEST_IN_FLIGHT", "DISK_PRESSURE", "PROJECTION_BACKLOG", "STATE_CHANGED" -> NOT_ACCEPTED;
            // 确定未提交的版本冲突可以刷新后重试；原ID已用于别的输入则是确定拒绝，均非UNKNOWN。
            case "REQUEST_CONFLICT" -> REJECTED;
            case "UNAUTHORIZED", "SESSION_EXPIRED", "AUTH_REQUIRED" -> AUTH_REQUIRED;
            case "INVALID_REQUEST", "UNKNOWN_MSG_ID", "PAYLOAD_TOO_LARGE", "INVALID_FRAME" -> PROTOCOL_ERROR;
            default -> internalStatus == 200 ? REJECTED
                    : internalStatus == 401 || internalStatus == 403 ? AUTH_REQUIRED
                    : internalStatus == 400 || internalStatus == 413 ? PROTOCOL_ERROR
                    : OUTCOME_UNKNOWN;
        };
    }
}

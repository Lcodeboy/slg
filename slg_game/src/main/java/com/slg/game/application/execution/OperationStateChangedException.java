package com.slg.game.application.execution;

/**
 * 只读规划到完整资格之间发生变化。仅允许在提交前抛出：当前尝试没有 SQL，调用方可以用原
 * requestId 重新规划；不能借此把已开始事务的超时或未知结果伪装成版本拒绝。
 */
public final class OperationStateChangedException extends RuntimeException {
    public OperationStateChangedException() { super("State changed between planning and qualified preparation"); }
}

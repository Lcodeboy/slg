package com.slg.core.protocol;

/**
 * 对外公开的稳定错误结构，不应携带堆栈、SQL、密码或令牌等内部信息。
 *
 * @param code 供客户端判断处理分支的错误码
 * @param message 可展示的错误说明，不作为客户端逻辑判断依据
 */
public record ApiError(String code, String message) {}

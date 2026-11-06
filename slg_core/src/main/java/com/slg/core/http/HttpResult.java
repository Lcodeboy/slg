package com.slg.core.http;

import com.slg.core.protocol.ApiError;

/** 账号与区服目录 HTTP 接口的 JSON 响应。成功数据由对应端点决定，失败只返回公开错误。 */
public record HttpResult<T>(boolean success, T data, ApiError error) {
    public static <T> HttpResult<T> ok(T data) {
        return new HttpResult<>(true, data, null);
    }

    public static <T> HttpResult<T> fail(String code, String message) {
        return new HttpResult<>(false, null, new ApiError(code, message));
    }
}

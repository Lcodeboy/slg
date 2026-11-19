package com.slg.game.domain.persistence;

import java.util.Objects;

/**
 * 可按原请求查回的确定结果，包含正数提交序号及原始响应文本。重放回执不重新应用业务
 * 变化；同键不同摘要必须由提交端口拒绝。回执的保留窗口独立于可回收的事件日志。
 * 本值应来自已确认提交或权威存储读取，单独构造它不能证明提交成功。
 */
public record CommitReceipt(RequestKey request, long sequence, String digest, String resultJson) {
    public CommitReceipt {
        Objects.requireNonNull(request, "request");
        PersistenceChecks.positive(sequence, "sequence");
        PersistenceChecks.identifier(digest, "digest", 128);
        PersistenceChecks.jsonText(resultJson, "resultJson");
    }
}

package com.slg.game.domain.persistence;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 已完成业务计算、尚待提交的不可变决定；入队不等于成功，持久化适配器须原子验证全部
 * 版本并写入变化、事实及回执。集合被防御复制，不能在排队后跟随可变实体继续变化。
 *
 * <p>每个变更键只能出现一次。只读依赖中的版本 0 表示对象必须不存在，正数表示必须
 * 匹配该已存在版本；同一键也发生变化时，两处预期版本必须一致。只读依赖即使没有
 * 状态变化也须在提交时验证，避免用过期状态持久化业务拒绝或奖励判定。</p>
 *
 * <p>readVersions 只描述具体对象，不能证明某个查询集合没有新成员。行军槽位等集合
 * 判定必须在玩家级执行资格内完成，并保持到事务核对及发布结束；即使 PlayerState
 * 没有变化也不能省略该资格。提交变更键不等于本业务所需的全部调度键。</p>
 *
 * <p>digest 标识规范化后的原请求类型与参数，不包含状态版本、计算结果、写者任期或
 * 排队预算；否则同一请求重试会被误判为不同业务。摘要和事实类型各限 1 至 128 个
 * 非空格可打印 ASCII 字符，具体编码与摘要算法由版本化请求编码器固定。
 * 只有事实/原始响应载荷保留 JSON 文本；业务 changes 必须是封闭的类型化状态。
 * JSON 只要求非空白，语法与 schema 由 codec 校验；编码和队列预算由适配器及提交端口负责。</p>
 */
public record CommitDecision(RequestKey request, String digest, String factType, String factJson,
                             String resultJson, List<ObjectChange> changes,
                             Map<ObjectKey, Long> readVersions) {
    public CommitDecision {
        Objects.requireNonNull(request, "request");
        PersistenceChecks.identifier(digest, "digest", 128);
        PersistenceChecks.identifier(factType, "factType", 128);
        PersistenceChecks.jsonText(factJson, "factJson");
        PersistenceChecks.jsonText(resultJson, "resultJson");
        changes = List.copyOf(Objects.requireNonNull(changes, "changes"));
        readVersions = Map.copyOf(Objects.requireNonNull(readVersions, "readVersions"));
        for (long version : readVersions.values()) {
            if (version < 0) throw new IllegalArgumentException("Read dependency version must not be negative");
        }
        var changedKeys = new HashSet<ObjectKey>();
        for (ObjectChange change : changes) {
            if (!changedKeys.add(change.key()))
                throw new IllegalArgumentException("An object may change only once in one decision");
            Long readVersion = readVersions.get(change.key());
            if (readVersion != null && readVersion.longValue() != change.expectedVersion())
                throw new IllegalArgumentException("Read and change versions must agree for the same object");
        }
    }
}

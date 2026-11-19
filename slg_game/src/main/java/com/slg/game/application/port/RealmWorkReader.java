package com.slg.game.application.port;

import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.BattleState;
import com.slg.game.domain.persistence.TaskState;
import java.util.List;
import java.util.Objects;

/**
 * 有界发现已持久化的任务与待结算战斗。候选只用于调度，不授予执行权；执行时仍须
 * 读取并校验归属、状态、generation、对象版本和写者任期。发现或入队不改变任务状态。
 * 直接读取结构化任务/战斗权威表的状态索引，不再维护通用 JSON 对象的第二份待办状态。
 * dueAt 与 dueThrough 均为 UTC Unix 毫秒时间戳，不能与排队预算的单调计时混用。
 */
public interface RealmWorkReader {
    /**
     * 按 (dueAt, taskId 的二进制顺序) 返回到期 PENDING 候选，after 为排他的翻页位置，
     * null 表示从头开始。dueThrough 必须非负；一轮扫描固定它，避免新任务让队尾无限
     * 后移。limit 必须为正，返回不可变列表；读失败不能返回成功空页。
     * 字节预算可使非空页短于 limit，调用方仍须从末项 cursor 续读，不能把短页当作
     * 扫描结束；只有空页表示本次读取没有后续候选，连一条也放不下时应明确报预算错误。
     */
    List<DueTask> dueTasks(RealmIdentity identity, long dueThrough, DueCursor after, int limit, ReadBudget budget)
            throws StorageAccessException;

    /**
     * 按 battleId 的二进制顺序发现待处理战斗，afterId 排他，null 表示从头开始。
     * limit 必须为正；返回不可变列表。已派发但尚未结算的任务可再次出现，由调度器的
     * 有界在途集合和事务内幂等判定处理，不能通过扫描删除记录来宣称完成。
     * 非空短页仍须从末项 battleId 续读；读取失败或第一项超出预算不能返回成功空页。
     */
    List<PendingBattle> pendingBattles(RealmIdentity identity, String afterId, int limit, ReadBudget budget)
            throws StorageAccessException;

    record DueCursor(long dueAt, String taskId) {
        public DueCursor {
            if (dueAt < 0) throw new IllegalArgumentException("dueAt must not be negative");
            identifier(taskId, "taskId", 256);
        }
    }

    /** 候选保留扫描时的任务版本；它不是行军 generation，也不能作为当前执行授权。 */
    record DueTask(long version, TaskState task) {
        public DueTask {
            if (version <= 0) throw new IllegalArgumentException("Task version must be positive");
            Objects.requireNonNull(task, "task");
            if (task.status() != TaskState.Status.PENDING)
                throw new IllegalArgumentException("Only pending tasks may be discovered");
        }

        public DueCursor cursor() { return new DueCursor(task.dueAt(), task.taskId()); }
    }

    /** 输入快照与归属来自同一条已提交战斗，避免独立摘要字段与内部载荷各说一套。 */
    record PendingBattle(long version, BattleState battle) {
        public PendingBattle {
            if (version <= 0) throw new IllegalArgumentException("Battle version must be positive");
            Objects.requireNonNull(battle, "battle");
            if (battle.status() != BattleState.Status.PENDING)
                throw new IllegalArgumentException("Only pending battles may be discovered");
        }
    }

    private static void identifier(String value, String name, int maxLength) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty() || value.length() > maxLength || value.chars().anyMatch(c -> c < 33 || c > 126))
            throw new IllegalArgumentException(name + " must be a nonblank printable ASCII identifier");
    }
}

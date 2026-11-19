package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmWorkReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.row.StateRows;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.session.ReadMeter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.slg.game.application.port.StorageAccessException.Code.READ_BUDGET_EXCEEDED;

/**
 * 直接发现权威表的 PENDING 候选，不认领、不删除任务。每页一份 RR 视图；下页由调用者
 * 携带末项游标续读，短页不能表示扫描结束，候选执行仍须再次取得写权并核验版本/generation。
 */
public final class MySqlRealmWorkReader implements RealmWorkReader {
    private final MySqlSessions sessions;
    private final MySqlStateRepository repository;

    public MySqlRealmWorkReader(MySqlSessions sessions, MySqlStateRepository repository) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override public List<DueTask> dueTasks(RealmIdentity identity, long dueThrough, DueCursor after,
                                           int limit, ReadBudget budget) throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        if (dueThrough < 0 || limit <= 0) throw new IllegalArgumentException("Invalid due task bounds");
        var meter = new ReadMeter(budget);
        var result = new ArrayList<DueTask>(Math.min(limit, 64));
        try (var session = sessions.openRead()) {
            sessions.validateIdentity(session, identity);
            meter.add(1, 32);
            DueCursor cursor = after;
            while (result.size() < limit) {
                meter.checkTime();
                if (!result.isEmpty() && meter.remainingRows() == 0) break;
                var query = new StateRows.Query(identity.realmId(), null, null, null, false, null, null,
                        dueThrough, cursor == null ? null : cursor.dueAt(), cursor == null ? null : cursor.taskId(),
                        true, 1, meter.remainingBytes());
                var roots = repository.roots(session, ObjectKind.TASK, query);
                if (roots.isEmpty()) break;
                var loaded = loadPageItem(session, identity, roots.getFirst(), meter, !result.isEmpty());
                if (loaded == null) break;
                var task = new DueTask(loaded.state().version(), (TaskState) loaded.state().state());
                result.add(task);
                cursor = task.cursor();
            }
            meter.checkTime();
            return List.copyOf(result);
        } catch (StorageAccessException failure) { throw failure; }
        catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }

    @Override public List<PendingBattle> pendingBattles(RealmIdentity identity, String afterId,
                                                       int limit, ReadBudget budget) throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        if (limit <= 0) throw new IllegalArgumentException("Battle page limit must be positive");
        ObjectKey.Battle initial = afterId == null ? null : new ObjectKey.Battle(afterId);
        var meter = new ReadMeter(budget);
        var result = new ArrayList<PendingBattle>(Math.min(limit, 64));
        try (var session = sessions.openRead()) {
            sessions.validateIdentity(session, identity);
            meter.add(1, 32);
            ObjectKey.Battle cursor = initial;
            while (result.size() < limit) {
                meter.checkTime();
                if (!result.isEmpty() && meter.remainingRows() == 0) break;
                // 逐个完整聚合计量；不先拉取 limit 个大 BLOB 再尝试裁剪响应页。
                var query = new StateRows.Query(identity.realmId(), null, cursor, null, false, null, null,
                        null, null, null, true, 1, meter.remainingBytes());
                var roots = repository.roots(session, ObjectKind.BATTLE, query);
                if (roots.isEmpty()) break;
                var loaded = loadPageItem(session, identity, roots.getFirst(), meter, !result.isEmpty());
                if (loaded == null) break;
                var battle = new PendingBattle(loaded.state().version(), (BattleState) loaded.state().state());
                result.add(battle);
                cursor = battle.battle().key();
            }
            meter.checkTime();
            return List.copyOf(result);
        } catch (StorageAccessException failure) { throw failure; }
        catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }

    /** 页预算只在完整记录边界截断；时间/取消仍使整页失败，第一条超限不能伪装成空页。 */
    private MySqlStateRepository.Loaded loadPageItem(org.apache.ibatis.session.SqlSession session,
                                                    RealmIdentity identity, StateRows.Root row, ReadMeter meter,
                                                    boolean hasPrevious) throws StorageAccessException {
        try {
            var loaded = repository.load(session, identity.realmId(), row, meter);
            if (!meter.canFit(loaded.rows(), loaded.bytes()) && hasPrevious) return null;
            meter.add(loaded.rows(), loaded.bytes());
            return loaded;
        } catch (StorageAccessException failure) {
            meter.checkTime();
            if (hasPrevious && failure.code() == READ_BUDGET_EXCEEDED) return null;
            throw failure;
        }
    }
}

package com.slg.game.projection;

import com.slg.game.storage.JournalRetentionPreview.Consumer;
import com.slg.game.storage.RealmJournal;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/** 与投影器共用 SQL 身份、归档和指纹校验，只用于隔离测试库的清理实验。 */
public final class LockedTestProjectionCursor {
    private LockedTestProjectionCursor() {}

    public static Consumer read(Connection connection, int realmId, UUID volumeId, RealmJournal journal,
                                RealmJournal.Inspection history)
            throws SQLException, IOException {
        if (connection.getCatalog() == null || !connection.getCatalog().matches("slg_test_[0-9a-f]{32}"))
            throw new SQLException("Cleanup experiment requires a disposable integration-test schema");
        // 锁住已有行，绝不登记空游标。调用方在本地批次完成后才释放锁，防止同一 SQL 实例
        // 在取证与写入之间改动位置。演练库没有外部切换数据卷或恢复备份的管理操作。
        Long cursor = MySqlProjectionStore.readCheckpoint(connection, true, realmId, volumeId.toString());
        if (cursor == null) return new Consumer(realmId, volumeId, null, "SQL_CHECKPOINT_MISSING");
        long high = history.high();
        if (cursor < 0 || cursor > high) return new Consumer(realmId, volumeId, cursor, "CURSOR_OUT_OF_RANGE");
        if (history.prunedThrough() > 0 && cursor <= history.prunedThrough())
            return new Consumer(realmId, volumeId, cursor, "ORIGINAL_BOUNDARY_PRUNED");
        if (cursor > 0) MySqlProjectionStore.verifyReplay(connection, realmId, journal.entry(cursor));
        return new Consumer(realmId, volumeId, cursor, cursor == high ? "CAUGHT_UP" : "CATCHING_UP");
    }
}

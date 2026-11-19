package com.slg.game.application.read;

import com.slg.game.application.port.RealmStateReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.domain.persistence.ObjectKind;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.VersionedState;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 仅在一次 SnapshotQuery 的读取线程内有效的视图。没有 close 或数据库句柄，查询结束后
 * 撤销其代理引用；即使查询误捕获此视图，也无法在回调/其他线程继续持有数据库快照。
 */
public interface SnapshotView {
    RealmIdentity identity();
    long highSequence();
    Optional<VersionedState> find(ObjectKey key) throws StorageAccessException;
    Optional<VersionedState> findPlayerByAccount(long accountId) throws StorageAccessException;
    void scan(ObjectKind kind, Consumer<RealmStateReader.StateEntry> consumer) throws StorageAccessException;
    void scanOwned(ObjectKind kind, String playerId, Consumer<RealmStateReader.StateEntry> consumer) throws StorageAccessException;
    void scanActiveMarches(String playerId, Consumer<RealmStateReader.StateEntry> consumer) throws StorageAccessException;
    void scanMovingMarches(Consumer<RealmStateReader.StateEntry> consumer) throws StorageAccessException;
}

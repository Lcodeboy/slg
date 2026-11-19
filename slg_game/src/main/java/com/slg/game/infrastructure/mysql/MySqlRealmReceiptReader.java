package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.infrastructure.mysql.codec.CodecException;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.JournalMapper;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.session.ReadMeter;
import java.util.Objects;

import static com.slg.game.application.port.StorageAccessException.Code.*;

/** 普通一致读仅提供一次观察；没有回执不能证明另一个尚未结束的事务已经回滚。 */
public final class MySqlRealmReceiptReader implements RealmReceiptReader {
    private final MySqlSessions sessions;
    private final PersistenceCodec codec;

    public MySqlRealmReceiptReader(MySqlSessions sessions, PersistenceCodec codec) {
        this.sessions = Objects.requireNonNull(sessions, "sessions"); this.codec = Objects.requireNonNull(codec, "codec");
    }

    @Override public Observation observe(RealmIdentity identity, RequestKey key, ReadBudget budget)
            throws StorageAccessException {
        Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(key, "key");
        var meter = new ReadMeter(budget);
        try (var session = sessions.openRead()) {
            sessions.validateIdentity(session, identity);
            var cursor = session.getMapper(RealmMetadataMapper.class).cursor(identity.realmId());
            meter.add(2, 64);
            if (cursor == null || cursor.prunedThrough() < 0 || cursor.highSeq() < cursor.prunedThrough())
                throw new StorageAccessException(INVALID_DATA, "Invalid receipt read watermark");
            var row = session.getMapper(JournalMapper.class).findReceipt(identity.realmId(), key);
            meter.checkTime();
            if (row == null) return new Observation.NotObserved();
            var receipt = row.toDomain(codec);
            if (!receipt.request().equals(key) || receipt.sequence() > cursor.highSeq())
                throw new StorageAccessException(INVALID_DATA, "Receipt identity or sequence is invalid");
            long bytes = 8L + receipt.request().scope().length() + receipt.request().requestId().length()
                    + receipt.digest().length() + row.resultJson().length;
            meter.add(1, bytes);
            return new Observation.Found(receipt);
        } catch (CodecException invalid) {
            throw new StorageAccessException(invalid.unsupportedSchema() ? UNSUPPORTED_SCHEMA : INVALID_DATA,
                    "Invalid authority receipt payload");
        } catch (IllegalArgumentException invalid) {
            throw new StorageAccessException(INVALID_DATA, "Invalid authority receipt row");
        } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }
}

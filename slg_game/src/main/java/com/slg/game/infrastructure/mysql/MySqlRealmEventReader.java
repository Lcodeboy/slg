package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmEventReader;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.CommittedEvent;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.infrastructure.mysql.codec.CodecException;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.JournalMapper;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import com.slg.game.infrastructure.mysql.session.MySqlErrors;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.session.ReadMeter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Objects;

import static com.slg.game.application.port.StorageAccessException.Code.*;

/** 同一 RR 视图读取 F/H 与完整决定；没有地图变化的决定仍推进连续序号。 */
public final class MySqlRealmEventReader implements RealmEventReader {
    private final MySqlSessions sessions;
    private final PersistenceCodec codec;

    public MySqlRealmEventReader(MySqlSessions sessions, PersistenceCodec codec) {
        this.sessions = Objects.requireNonNull(sessions, "sessions"); this.codec = Objects.requireNonNull(codec, "codec");
    }

    @Override public EventPage readAfter(RealmIdentity identity, long after, int limit, ReadBudget budget)
            throws StorageAccessException {
        Objects.requireNonNull(identity, "identity");
        if (after < 0 || limit <= 0) throw new IllegalArgumentException("Invalid event cursor or limit");
        var meter = new ReadMeter(budget);
        try (var session = sessions.openRead()) {
            sessions.validateIdentity(session, identity);
            var cursor = session.getMapper(RealmMetadataMapper.class).cursor(identity.realmId());
            meter.add(2, 64);
            if (cursor == null || cursor.prunedThrough() < 0 || cursor.highSeq() < cursor.prunedThrough())
                throw new StorageAccessException(INVALID_DATA, "Invalid authority event cursor");
            if (after < cursor.prunedThrough()) throw new RealmEventReader.CursorExpired(cursor.prunedThrough());
            if (after > cursor.highSeq()) throw new RealmEventReader.CursorAhead(cursor.highSeq());
            var events = new ArrayList<CommittedEvent>();
            var mapper = session.getMapper(JournalMapper.class);
            long through = after;
            while (through < cursor.highSeq() && events.size() < limit) {
                if (!meter.canFit(1, 44)) {
                    if (events.isEmpty()) throw new StorageAccessException(READ_BUDGET_EXCEEDED, "No complete event fits read budget");
                    break;
                }
                // 一次最多取一个完整事件，SQL 先按剩余预算限制 blob；页 limit 不能放大 JDBC 分配。
                long payloadBudget = Math.max(0, meter.remainingBytes() - 44);
                var rows = mapper.readEventsBounded(identity.realmId(), through, 1, payloadBudget);
                meter.checkTime();
                if (rows.size() != 1) throw new StorageAccessException(INVALID_DATA, "Retained event prefix has a hole");
                var row = rows.getFirst();
                if (row.sequence() != through + 1 || row.sequence() > cursor.highSeq() || row.payloadBytes() <= 0
                        || row.payloadBytes() > PersistenceCodec.DEFAULT_MAX_BYTES)
                    throw new StorageAccessException(INVALID_DATA, "Invalid retained event sequence or size");
                if (row.entrySchema() != PersistenceCodec.ENTRY_SCHEMA)
                    throw new StorageAccessException(UNSUPPORTED_SCHEMA, "Unsupported event entry schema");
                long cost = row.payloadBytes() + 44;
                if (!meter.canFit(1, cost)) {
                    if (events.isEmpty()) throw new StorageAccessException(READ_BUDGET_EXCEEDED, "No complete event fits read budget");
                    // SQL 只返回这一项的长度/序号/哈希头；它也占本次读取成本，不能免费探测下一页。
                    meter.add(1, 44);
                    break;
                }
                byte[] payload = row.payload(), hash = row.hash();
                if (payload == null || payload.length != row.payloadBytes() || hash == null || hash.length != 32
                        || !MessageDigest.isEqual(PersistenceCodec.hash(payload), hash))
                    throw new StorageAccessException(INVALID_DATA, "Event payload integrity check failed");
                var decision = codec.decodeDecision(payload);
                meter.add(1, cost);
                events.add(new CommittedEvent(row.sequence(), decision)); through = row.sequence();
            }
            meter.checkTime();
            return new EventPage(identity, after, cursor.prunedThrough(), cursor.highSeq(), through, events);
        } catch (CodecException invalid) {
            throw new StorageAccessException(invalid.unsupportedSchema() ? UNSUPPORTED_SCHEMA : INVALID_DATA,
                    "Invalid authority event payload");
        } catch (RuntimeException failure) { throw MySqlErrors.access(failure); }
    }
}

package com.slg.game.infrastructure.mysql;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.RealmTransaction;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.CommitDecision;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.persistence.WriterToken;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.JournalMapper;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.slg.game.application.port.StorageAccessException.Code.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真库验证页面预算与坏行拒绝；所有注入只在 fixture 分配的随机测试库内进行。 */
@Timeout(60)
class MySqlEventReadIT extends AuthoritySchemaFixture {
    private static final ReadBudget ROOM = new ReadBudget(100, 16L * 1024 * 1024, Duration.ofSeconds(10));
    private PersistenceCodec codec;
    private MySqlRealmTransaction transaction;
    private MySqlRealmEventReader events;
    private MySqlRealmReceiptReader receipts;
    private WriterToken writer;

    @BeforeEach void wireReaders() throws Exception {
        codec = new PersistenceCodec();
        transaction = new MySqlRealmTransaction(sessions, new MySqlStateRepository(codec), codec);
        writer = new MySqlRealmOwnership(sessions, Duration.ofSeconds(30)).acquire(identity, UUID.randomUUID());
        events = new MySqlRealmEventReader(sessions, codec); receipts = new MySqlRealmReceiptReader(sessions, codec);
    }

    @Test void rowBudgetReturnsOneCompleteDecisionAndItsExactProgress() throws Exception {
        seed("one"); seed("two");
        var page = events.readAfter(identity, 0, Integer.MAX_VALUE, budget(3, 100_000));
        assertEquals(2, page.high()); assertEquals(1, page.through()); assertEquals(1, page.events().size());
        assertEquals("one", page.events().getFirst().decision().request().requestId());
        var next = events.readAfter(identity, page.through(), 10, budget(3, 100_000));
        assertEquals(2, next.through()); assertEquals("two", next.events().getFirst().decision().request().requestId());
    }

    @Test void noRoomForFirstCompleteDecisionIsAnErrorRatherThanEmptyProgress() throws Exception {
        seed("one");
        assertEquals(READ_BUDGET_EXCEEDED, assertThrows(StorageAccessException.class,
                () -> events.readAfter(identity, 0, 10, budget(2, 100_000))).code());
        long length = scalar("SELECT OCTET_LENGTH(entry_payload) FROM game_journal WHERE journal_seq=1");
        assertEquals(READ_BUDGET_EXCEEDED, assertThrows(StorageAccessException.class,
                () -> events.readAfter(identity, 0, 10, budget(10, 64 + 44 + length - 1))).code());
    }

    @Test void byteBudgetStopsAtDecisionBoundaryAndSqlDoesNotMaterializeNextBlob() throws Exception {
        seed("one"); seed("two");
        long length = scalar("SELECT OCTET_LENGTH(entry_payload) FROM game_journal WHERE journal_seq=1");
        var page = events.readAfter(identity, 0, 100, budget(100, 64 + 44 + length + 44));
        assertEquals(1, page.through()); assertEquals(2, page.high()); assertEquals(1, page.events().size());
        try (var session = sessions.openRead()) {
            var row = session.getMapper(JournalMapper.class).readEventsBounded(identity.realmId(), 1, 1, 44).getFirst();
            assertNull(row.payload()); assertTrue(row.payloadBytes() > 44); assertEquals(32, row.hash().length);
        }
    }

    @Test void unsupportedEntrySchemaIsDistinctFromDamagedPayload() throws Exception {
        seed("one"); execute("UPDATE game_journal SET entry_schema=2 WHERE realm_id=1 AND journal_seq=1");
        assertEquals(UNSUPPORTED_SCHEMA, assertThrows(StorageAccessException.class,
                () -> events.readAfter(identity, 0, 10, ROOM)).code());
    }

    @Test void receiptAheadOfSameViewWatermarkCannotBecomeAFoundResultOrReplay() throws Exception {
        var decision = seed("one");
        execute("UPDATE game_receipt SET journal_seq=2 WHERE realm_id=1 AND request_id='one'");
        assertEquals(INVALID_DATA, assertThrows(StorageAccessException.class,
                () -> receipts.observe(identity, decision.request(), ROOM)).code());
        assertEquals(INVALID_DATA, assertThrows(StorageAccessException.class,
                () -> transaction.commit(writer, decision)).code());
    }

    @Test void oversizedStoredEventIsRejectedWithoutFetchingItsBlob() throws Exception {
        seed("one"); execute("UPDATE game_journal SET entry_payload=REPEAT('x',8388609) WHERE realm_id=1 AND journal_seq=1");
        try (var session = sessions.openRead()) {
            var row = session.getMapper(JournalMapper.class).readEvents(identity.realmId(), 0, 1).getFirst();
            assertNull(row.payload()); assertEquals(8_388_609, row.payloadBytes());
        }
        assertEquals(INVALID_DATA, assertThrows(StorageAccessException.class,
                () -> events.readAfter(identity, 0, 10, ROOM)).code());
    }

    @Test void oversizedStoredReceiptIsRejectedWithoutFetchingItsBlob() throws Exception {
        var decision = seed("one"); execute("UPDATE game_receipt SET result_json=REPEAT('x',1048577) WHERE realm_id=1 AND request_id='one'");
        try (var session = sessions.openRead()) {
            assertNull(session.getMapper(JournalMapper.class).findReceipt(identity.realmId(), decision.request()).resultJson());
        }
        assertEquals(INVALID_DATA, assertThrows(StorageAccessException.class,
                () -> receipts.observe(identity, decision.request(), ROOM)).code());
    }

    private CommitDecision seed(String id) throws Exception {
        var decision = new CommitDecision(new RequestKey("scope", id), "digest", "NO_CHANGE", "{}", "{\"ok\":true}", List.of(), Map.of());
        assertInstanceOf(CommitResult.Committed.class,
                assertInstanceOf(RealmTransaction.CommitAttempt.Settled.class, transaction.commit(writer, decision)).result());
        return decision;
    }
    private static ReadBudget budget(int rows, long bytes) { return new ReadBudget(rows, bytes, Duration.ofSeconds(10)); }
}

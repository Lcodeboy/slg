package com.slg.game.infrastructure.mysql.mapper;

import com.slg.game.domain.persistence.CommitReceipt;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.infrastructure.mysql.row.JournalRows;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 回执与事件 SQL 共用外层业务 Session。普通查重不锁缺失请求的 gap；并发唯一冲突由
 * 协调器整笔回滚后用新视图核对，不能在这里重试或自行提交事件/回执的一半。
 */
public interface JournalMapper {
    @Select("""
            SELECT scope_id AS scope, request_id AS requestId, journal_seq AS sequence,
                   digest, CASE WHEN OCTET_LENGTH(result_json) <= 1048576 THEN result_json ELSE NULL END AS resultJson
              FROM game_receipt
             WHERE realm_id=#{realmId} AND scope_id=#{request.scope} AND request_id=#{request.requestId}
            """)
    JournalRows.Receipt findReceipt(@Param("realmId") int realmId, @Param("request") RequestKey request);

    default int insertReceipt(int realmId, CommitReceipt receipt) {
        return insertReceiptRow(realmId, JournalRows.Receipt.fromDomain(receipt));
    }

    @Insert("""
            INSERT INTO game_receipt(realm_id,scope_id,request_id,digest,journal_seq,result_json)
            VALUES(#{realmId},#{receipt.scope},#{receipt.requestId},#{receipt.digest},#{receipt.sequence},#{receipt.resultJson})
            """)
    int insertReceiptRow(@Param("realmId") int realmId, @Param("receipt") JournalRows.Receipt receipt);

    @Insert("""
            INSERT INTO game_journal(realm_id,journal_seq,entry_schema,entry_payload,entry_hash)
            VALUES(#{realmId},#{sequence},#{schema},#{payload},#{hash})
            """)
    int insertEvent(@Param("realmId") int realmId, @Param("sequence") long sequence, @Param("schema") int schema,
                    @Param("payload") byte[] payload, @Param("hash") byte[] hash);

    default List<JournalRows.Event> readEvents(int realmId, long after, int limit) {
        if (limit <= 0 || limit > 1000) throw new IllegalArgumentException("Event query batch must be within 1..1000");
        return readEventsBounded(realmId, after, limit, 8388608);
    }

    /** 在服务器端先检查长度；损坏 LONGBLOB 不能在 Java 检查预算前占满 JDBC 堆。 */
    @Select("""
            SELECT journal_seq AS sequence, entry_schema AS entrySchema,
                   CASE WHEN OCTET_LENGTH(entry_payload) <= LEAST(8388608, #{maximumBytes})
                        THEN entry_payload ELSE NULL END AS payload,
                   entry_hash AS hash, OCTET_LENGTH(entry_payload) AS payloadBytes
              FROM game_journal WHERE realm_id=#{realmId} AND journal_seq>#{after}
             ORDER BY journal_seq LIMIT #{limit}
            """)
    List<JournalRows.Event> readEventsBounded(@Param("realmId") int realmId, @Param("after") long after,
                                            @Param("limit") int limit, @Param("maximumBytes") long maximumBytes);
}

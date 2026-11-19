package com.slg.game.infrastructure.mysql.row;

import com.slg.game.domain.persistence.CommitReceipt;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.codec.CodecException;

/** Mapper 专用行，二进制正文避免 MySQL JSON 归一化破坏原回执和事件哈希。 */
public final class JournalRows {
    private JournalRows() {}

    public record Receipt(String scope, String requestId, long sequence, String digest, byte[] resultJson) {
        public Receipt { resultJson = resultJson == null ? null : resultJson.clone(); }
        @Override public byte[] resultJson() { return resultJson == null ? null : resultJson.clone(); }
        public CommitReceipt toDomain() { return toDomain(new PersistenceCodec()); }
        public CommitReceipt toDomain(PersistenceCodec codec) {
            try {
                return new CommitReceipt(new RequestKey(scope, requestId), sequence, digest, codec.decodeJson(resultJson));
            } catch (CodecException bad) { throw bad;
            } catch (RuntimeException bad) { throw new CodecException("Invalid receipt row", bad); }
        }
        public static Receipt fromDomain(CommitReceipt receipt) {
            return new Receipt(receipt.request().scope(), receipt.request().requestId(), receipt.sequence(),
                    receipt.digest(), new PersistenceCodec().encodeJson(receipt.resultJson()));
        }
    }
    public record Event(long sequence, int entrySchema, byte[] payload, byte[] hash, long payloadBytes) {
        public Event {
            payload = payload == null ? null : payload.clone(); hash = hash == null ? null : hash.clone();
        }
        @Override public byte[] payload() { return payload == null ? null : payload.clone(); }
        @Override public byte[] hash() { return hash == null ? null : hash.clone(); }
    }
}

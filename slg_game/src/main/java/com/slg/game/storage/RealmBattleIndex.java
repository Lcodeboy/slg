package com.slg.game.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.protocol.Json;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** 只索引待计算的战斗。任务、预留与索引一起提交，重启无需遍历全部历史战报。 */
final class RealmBattleIndex {
    private static final byte[] EMPTY = {};
    private static final byte PREFIX = 'B';
    private RealmBattleIndex() {}

    static void update(WriteBatch batch, String id, String json) throws IOException, RocksDBException {
        byte[] key = bytes("B" + id);
        JsonNode job = Json.MAPPER.readTree(json);
        if (isPending(id, job)) batch.put(key, EMPTY); else batch.delete(key);
    }

    static List<RealmJournal.PendingBattle> page(RocksDB db, ReadOptions options, String after, int limit)
            throws IOException, RocksDBException {
        var jobs = new ArrayList<RealmJournal.PendingBattle>(limit);
        try (RocksIterator it = db.newIterator(options)) {
            byte[] start = after == null ? new byte[]{PREFIX} : bytes("B" + after);
            it.seek(start);
            if (after != null && it.isValid() && Arrays.equals(start, it.key())) it.next();
            while (it.isValid() && jobs.size() < limit) {
                byte[] key = it.key();
                if (key.length == 0 || key[0] != PREFIX) break;
                String id = new String(key, 1, key.length - 1, StandardCharsets.US_ASCII);
                byte[] raw = db.get(options, bytes("Sbattle:" + id));
                if (raw == null || it.value().length != 0) throw new IOException("Invalid pending battle index");
                var job = pending(id, RealmJournal.decodeValue(raw));
                if (job == null) throw new IOException("Battle index references a completed job");
                jobs.add(job);
                it.next();
            }
            it.status();
        }
        return List.copyOf(jobs);
    }

    private static RealmJournal.PendingBattle pending(String id, RealmJournal.Value value) throws IOException {
        JsonNode job = Json.MAPPER.readTree(value.json());
        if (!isPending(id, job)) return null;
        return new RealmJournal.PendingBattle(id, job.path("playerId").textValue(),
                job.path("inputHash").textValue(), job.path("rulesVersion").textValue(), value);
    }

    private static boolean isPending(String id, JsonNode job) throws IOException {
        if (!id.equals(job.path("battleId").textValue()) || !id.equals(job.path("marchId").textValue()))
            throw new IOException("Battle identity mismatch");
        return switch (job.path("state").asText("")) {
            case "PENDING" -> {
                if (!job.path("playerId").asText("").matches("[A-Za-z0-9_-]{1,64}") ||
                        !job.path("inputHash").asText("").matches("[0-9a-f]{64}") ||
                        job.path("rulesVersion").asText("").isBlank()) throw new IOException("Invalid pending battle");
                yield true;
            }
            case "SETTLED", "BLOCKED" -> false;
            default -> throw new IOException("Unknown battle state");
        };
    }

    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
}

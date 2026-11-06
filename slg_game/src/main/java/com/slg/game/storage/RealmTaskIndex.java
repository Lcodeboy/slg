package com.slg.game.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.protocol.Json;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** D + 大端到期时间 + ASCII 任务 ID。索引是任务状态的派生记录，随状态在同一批次更新。 */
final class RealmTaskIndex {
    private static final byte PREFIX = 'D';
    private static final byte[] EMPTY = {};

    private RealmTaskIndex() {}
    private record Indexed(RealmJournal.DueTask task, boolean pending) {}

    static void update(WriteBatch batch, String key, byte[] oldState, String json) throws RocksDBException, IOException {
        String id = key.substring("task:".length());
        Indexed next = parse(id, Json.read(json, JsonNode.class));
        if (oldState != null) {
            Indexed old = fromStoredState(id, oldState);
            if (!old.task.playerId().equals(next.task.playerId()) || !old.task.marchId().equals(next.task.marchId()))
                throw new IllegalArgumentException("Task cannot change its owner or march");
            if (old.pending) batch.delete(indexKey(old.task));
        }
        if (next.pending) batch.put(indexKey(next.task), EMPTY);
    }

    static List<RealmJournal.DueTask> due(RocksDB db, ReadOptions options, long now, RealmJournal.DueTask after,
                                          int limit) throws IOException, RocksDBException {
        var result = new ArrayList<RealmJournal.DueTask>(limit);
        try (RocksIterator it = db.newIterator(options)) {
            byte[] start = after == null ? new byte[]{PREFIX} : indexKey(after);
            it.seek(start);
            if (after != null && it.isValid() && Arrays.equals(start, it.key())) it.next();
            while (it.isValid() && result.size() < limit) {
                byte[] key = it.key();
                if (key.length == 0 || key[0] != PREFIX) break;
                if (key.length < 10) throw new IOException("Malformed due task index key");
                long dueAt = ByteBuffer.wrap(key, 1, Long.BYTES).getLong();
                if (dueAt < 0) throw new IOException("Negative due time in task index");
                if (dueAt > now) break;
                String id = new String(key, 9, key.length - 9, StandardCharsets.US_ASCII);
                if (it.value().length != 0) throw new IOException("Unexpected due task index payload");
                byte[] raw = db.get(options, bytes("Stask:" + id));
                if (raw == null) throw new IOException("Due task index has no authoritative row");
                Indexed row = fromStoredState(id, raw);
                if (!row.pending || !Arrays.equals(indexKey(row.task), key))
                    throw new IOException("Due task index key and value disagree");
                result.add(row.task);
                if (result.size() < limit) it.next();
            }
            it.status();
        }
        return List.copyOf(result);
    }

    private static Indexed fromStoredState(String id, byte[] raw) throws IOException {
        return parseStored(id, Json.MAPPER.readTree(RealmJournal.decodeValue(raw).json()));
    }

    private static Indexed parseStored(String id, JsonNode n) throws IOException {
        try { return parse(id, n); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid persisted task " + id, invalid); }
    }

    private static Indexed parse(String id, JsonNode n) {
        if (n == null || !n.isObject() || !id.equals(string(n, "taskId")))
            throw new IllegalArgumentException("Task key and ID disagree");
        JsonNode generation = n.path("generation"), time = n.path("dueAt");
        if (!generation.isIntegralNumber() || !generation.canConvertToInt() ||
                !time.isIntegralNumber() || !time.canConvertToLong())
            throw new IllegalArgumentException("Task generation and due time must be integers");
        var task = new RealmJournal.DueTask(id, string(n, "playerId"), string(n, "marchId"), string(n, "type"),
                generation.intValue(), time.longValue());
        boolean pending = switch (string(n, "state")) {
            case "PENDING" -> true;
            case "DONE", "CANCELLED", "BLOCKED" -> false;
            default -> throw new IllegalArgumentException("Unknown task state");
        };
        return new Indexed(task, pending);
    }

    private static String string(JsonNode n, String field) {
        JsonNode value = n.path(field);
        if (!value.isTextual()) throw new IllegalArgumentException("Missing task field: " + field);
        return value.textValue();
    }

    private static byte[] indexKey(RealmJournal.DueTask task) {
        byte[] id = bytes(task.taskId());
        return ByteBuffer.allocate(9 + id.length).put(PREFIX).putLong(task.dueAt()).put(id).array();
    }

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
}

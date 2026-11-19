package com.slg.game.infrastructure.mysql;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.application.port.CommitPayloadSizer;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import java.util.Objects;

/**
 * 在准入前使用真实版本化 codec 校验/编码完整决定，不读数据库，不信任调用者提供的长度。
 * 计费是载荷字节加明确的调度元数据预算，不宣称等于 JVM 对象堆大小；同一决定换任期或
 * 等待预算不改变持久字节，但仍须计入 writer、请求索引键和生命周期元数据。
 */
public final class MySqlCommitPayloadSizer implements CommitPayloadSizer {
    private static final long SCHEDULING_OVERHEAD = 256;
    // realm INT、dataset UUID、instance UUID、epoch LONG；origin INT 与 Duration 秒/纳秒。
    private static final long WRITER_METADATA = 4 + 16 + 16 + 8;
    private static final long EXECUTION_METADATA = 4 + 8 + 4;
    private final PersistenceCodec codec;

    public MySqlCommitPayloadSizer(PersistenceCodec codec) { this.codec = Objects.requireNonNull(codec, "codec"); }

    @Override public long measure(CommitRequest request) {
        Objects.requireNonNull(request, "request");
        long bytes = codec.encodeDecision(request.decision()).length;
        // request 字段也在日志中，但调度中的独立去重索引要另留预算；领域已限制为 ASCII。
        bytes = Math.addExact(bytes, request.decision().request().scope().length());
        bytes = Math.addExact(bytes, request.decision().request().requestId().length());
        bytes = Math.addExact(bytes, WRITER_METADATA);
        bytes = Math.addExact(bytes, EXECUTION_METADATA);
        return Math.addExact(bytes, SCHEDULING_OVERHEAD);
    }
}

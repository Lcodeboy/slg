-- 001：区服日志的只读查询模型。由建表账号在独立游戏投影库执行，GAME 运行账号只写业务事务。
-- 绑定区服和原卷，防止同一区号重建空卷后把另一段从 1 开始的历史接到旧记录后面。
CREATE TABLE IF NOT EXISTS projection_checkpoint (
    realm_id INT NOT NULL,
    volume_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    last_applied_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id),
    UNIQUE KEY uq_projection_volume (volume_id),
    CHECK (realm_id > 0 AND last_applied_seq >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 每个决定都有一行，包括无状态变更的确定性业务拒绝。作用域与请求 ID 区分大小写，
-- 与原卷去重规则相同。完整输入指纹还覆盖各对象的预期版本和快照，用于核验重复投递。
CREATE TABLE IF NOT EXISTS projection_event (
    realm_id INT NOT NULL,
    journal_seq BIGINT NOT NULL,
    schema_version INT NOT NULL,
    scope_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    digest VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    fact_type VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    fact_json JSON NOT NULL,
    result_json JSON NOT NULL,
    entry_hash BINARY(32) NOT NULL,
    PRIMARY KEY (realm_id, journal_seq),
    UNIQUE KEY uq_projection_request (realm_id, scope_id, request_id),
    CONSTRAINT fk_projection_event_realm FOREIGN KEY (realm_id) REFERENCES projection_checkpoint (realm_id),
    CHECK (journal_seq > 0 AND schema_version > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- 每个对象存最后一份完整的已提交状态。归档流水与对象最新值分别可查：
-- 连续消费位置属于整个区，对象版本只属于一个对象，不能拿一个对象的版本代替区服水位。
CREATE TABLE IF NOT EXISTS projection_object (
    realm_id INT NOT NULL,
    object_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    object_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    object_version BIGINT NOT NULL,
    journal_seq BIGINT NOT NULL,
    state_json JSON NOT NULL,
    PRIMARY KEY (realm_id, object_type, object_id),
    CONSTRAINT fk_projection_object_event FOREIGN KEY (realm_id, journal_seq)
        REFERENCES projection_event (realm_id, journal_seq),
    CHECK (object_version > 0 AND journal_seq > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

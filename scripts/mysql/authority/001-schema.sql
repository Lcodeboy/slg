-- MySQL 权威存储 schema v1。仅由独立空库初始化工具执行，GAME 运行账号不得拥有 DDL 权限。
-- 不使用 IF NOT EXISTS：非空库、旧表或半次初始化必须失败，由操作者调查，不能覆盖身份。
-- 业务 ID 使用 ASCII 二进制排序；UUID 使用 BINARY(16) 配合 UUID_TO_BIN/BIN_TO_UUID。
-- 根行 last_seq=0 仅允许事务内新建占位；提交协调器必须在 COMMIT 前最终化，读取器拒绝已提交的0。
-- 根对象之间不加 FK，避免跨对象外键检查隐式逆转统一锁序；行军/战斗明细只引用其已锁定根行。
-- 状态/回执不引用事件表：它们独立保留，事件将来归档不能级联删除玩法事实。

CREATE TABLE realm_identity (
    realm_id INT NOT NULL PRIMARY KEY,
    dataset_id BINARY(16) NOT NULL UNIQUE,
    schema_version INT NOT NULL,
    CHECK (realm_id > 0 AND schema_version > 0)
) ENGINE=InnoDB;

CREATE TABLE realm_owner (
    realm_id INT NOT NULL PRIMARY KEY,
    writer_epoch BIGINT NOT NULL,
    owner_instance BINARY(16) NULL,
    active BOOLEAN NOT NULL,
    CHECK (realm_id > 0 AND writer_epoch >= 0),
    CHECK ((active = 0 AND owner_instance IS NULL) OR
           (active = 1 AND writer_epoch > 0 AND owner_instance IS NOT NULL))
) ENGINE=InnoDB;

CREATE TABLE realm_owner_lease (
    realm_id INT NOT NULL PRIMARY KEY,
    writer_epoch BIGINT NOT NULL,
    owner_instance BINARY(16) NULL,
    expires_at_millis BIGINT NOT NULL,
    CHECK (realm_id > 0 AND writer_epoch >= 0),
    CHECK ((owner_instance IS NULL AND expires_at_millis = 0) OR
           (owner_instance IS NOT NULL AND writer_epoch > 0 AND expires_at_millis > 0))
) ENGINE=InnoDB;

CREATE TABLE realm_commit_cursor (
    realm_id INT NOT NULL PRIMARY KEY,
    high_seq BIGINT NOT NULL,
    pruned_through BIGINT NOT NULL,
    CHECK (realm_id > 0 AND high_seq >= 0 AND pruned_through >= 0 AND pruned_through <= high_seq)
) ENGINE=InnoDB;

CREATE TABLE game_player (
    realm_id INT NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    account_id BIGINT NOT NULL,
    founding_realm_id INT NOT NULL,
    current_realm_id INT NOT NULL,
    name VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, player_id),
    UNIQUE KEY uq_player_account (realm_id, account_id),
    CHECK (realm_id > 0 AND account_id > 0 AND founding_realm_id > 0 AND current_realm_id = realm_id),
    CHECK (player_id = CONCAT(founding_realm_id, '_', account_id)),
    CHECK (CHAR_LENGTH(name) > 0 AND version > 0 AND last_seq >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_player_resource (
    realm_id INT NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    resource_type INT NOT NULL,
    amount BIGINT NOT NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, player_id, resource_type),
    CHECK (realm_id > 0 AND resource_type IN (1, 2, 3) AND amount >= 0 AND version > 0 AND last_seq >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_player_troop (
    realm_id INT NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    troop_type INT NOT NULL,
    available BIGINT NOT NULL,
    wounded BIGINT NOT NULL,
    dead BIGINT NOT NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, player_id, troop_type),
    CHECK (realm_id > 0 AND troop_type IN (1, 2, 3) AND available >= 0 AND wounded >= 0 AND dead >= 0),
    CHECK (version > 0 AND last_seq >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_player_hero (
    realm_id INT NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    hero_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    assigned_march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, player_id, hero_id),
    CHECK (realm_id > 0 AND version > 0 AND last_seq >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_march (
    realm_id INT NOT NULL,
    march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status INT NOT NULL,
    mission INT NOT NULL,
    from_x DOUBLE NOT NULL,
    from_y DOUBLE NOT NULL,
    target_x INT NOT NULL,
    target_y INT NOT NULL,
    depart_at BIGINT NOT NULL,
    arrive_at BIGINT NOT NULL,
    tier INT NOT NULL,
    task_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NULL,
    rules_version VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, march_id),
    KEY ix_march_owned (realm_id, player_id, march_id),
    KEY ix_march_active (realm_id, player_id, status, march_id),
    -- MYSQL-05 新空库地图扫描使用；不对现有开发库自动执行 ALTER 或迁移。
    KEY ix_march_moving (realm_id, status, march_id),
    CHECK (realm_id > 0 AND status IN (1, 2, 3, 4, 5) AND mission IN (1, 2)),
    CHECK (from_x >= 0 AND from_y >= 0 AND target_x >= 0 AND target_y >= 0),
    CHECK (depart_at >= 0 AND arrive_at >= depart_at AND tier > 0 AND version > 0 AND last_seq >= 0),
    CHECK ((status IN (1, 2, 4) AND task_id IS NOT NULL) OR (status IN (3, 5) AND task_id IS NULL)),
    CHECK ((status <> 2 OR mission = 1) AND (status <> 3 OR mission = 2))
) ENGINE=InnoDB;

CREATE TABLE game_march_troop (
    realm_id INT NOT NULL,
    march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    troop_type INT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (realm_id, march_id, troop_type),
    CONSTRAINT fk_march_troop_root FOREIGN KEY (realm_id, march_id) REFERENCES game_march (realm_id, march_id),
    CHECK (troop_type IN (1, 2, 3) AND count >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_march_cargo (
    realm_id INT NOT NULL,
    march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    resource_type INT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (realm_id, march_id, resource_type),
    CONSTRAINT fk_march_cargo_root FOREIGN KEY (realm_id, march_id) REFERENCES game_march (realm_id, march_id),
    CHECK (resource_type IN (1, 2, 3) AND count >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_march_hero (
    realm_id INT NOT NULL,
    march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    hero_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    slot_index INT NOT NULL,
    PRIMARY KEY (realm_id, march_id, hero_id),
    UNIQUE KEY uq_march_hero_slot (realm_id, march_id, slot_index),
    CONSTRAINT fk_march_hero_root FOREIGN KEY (realm_id, march_id) REFERENCES game_march (realm_id, march_id),
    CHECK (slot_index >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_gather_site (
    realm_id INT NOT NULL,
    x INT NOT NULL,
    y INT NOT NULL,
    occupant_player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NULL,
    occupant_march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NULL,
    generation BIGINT NOT NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, x, y),
    KEY ix_gather_owned (realm_id, occupant_player_id, x, y),
    CHECK (realm_id > 0 AND x >= 0 AND y >= 0 AND version > 0 AND last_seq >= 0),
    CHECK ((occupant_player_id IS NULL AND occupant_march_id IS NULL AND generation = 0) OR
           (occupant_player_id IS NOT NULL AND occupant_march_id IS NOT NULL AND generation > 0))
) ENGINE=InnoDB;

CREATE TABLE game_due_task (
    realm_id INT NOT NULL,
    task_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    type INT NOT NULL,
    status INT NOT NULL,
    generation BIGINT NOT NULL,
    due_at BIGINT NOT NULL,
    reason VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, task_id),
    KEY ix_task_due (realm_id, status, due_at, task_id),
    KEY ix_task_owned (realm_id, player_id, task_id),
    CHECK (realm_id > 0 AND type IN (1, 2, 3) AND status IN (1, 2, 3, 4)),
    CHECK (generation > 0 AND due_at >= 0 AND version > 0 AND last_seq >= 0),
    CHECK ((status <> 4 OR reason IS NOT NULL) AND (reason IS NULL OR CHAR_LENGTH(reason) > 0))
) ENGINE=InnoDB;

CREATE TABLE game_battle (
    realm_id INT NOT NULL,
    battle_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    player_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    march_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status INT NOT NULL,
    input_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    rules_version VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    march_version BIGINT NOT NULL,
    input_schema INT NOT NULL,
    input_json LONGBLOB NOT NULL,
    result_schema INT NULL,
    result_json LONGBLOB NULL,
    version BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, battle_id),
    KEY ix_battle_pending (realm_id, status, battle_id),
    KEY ix_battle_owned (realm_id, player_id, battle_id),
    CHECK (realm_id > 0 AND status IN (1, 2) AND march_version > 0 AND input_schema > 0),
    CHECK (version > 0 AND last_seq >= 0 AND OCTET_LENGTH(input_json) > 0),
    CHECK ((status = 1 AND result_schema IS NULL AND result_json IS NULL) OR
           (status = 2 AND result_schema IS NOT NULL AND result_schema > 0 AND result_json IS NOT NULL
            AND OCTET_LENGTH(result_json) > 0))
) ENGINE=InnoDB;

CREATE TABLE game_battle_hero (
    realm_id INT NOT NULL,
    battle_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    hero_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    slot_index INT NOT NULL,
    PRIMARY KEY (realm_id, battle_id, hero_id),
    UNIQUE KEY uq_battle_hero_slot (realm_id, battle_id, slot_index),
    CONSTRAINT fk_battle_hero_root FOREIGN KEY (realm_id, battle_id) REFERENCES game_battle (realm_id, battle_id),
    CHECK (slot_index >= 0)
) ENGINE=InnoDB;

CREATE TABLE game_receipt (
    realm_id INT NOT NULL,
    scope_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    digest VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    journal_seq BIGINT NOT NULL,
    result_json LONGBLOB NOT NULL,
    PRIMARY KEY (realm_id, scope_id, request_id),
    CHECK (realm_id > 0 AND journal_seq > 0 AND OCTET_LENGTH(result_json) > 0)
) ENGINE=InnoDB;

CREATE TABLE game_journal (
    realm_id INT NOT NULL,
    journal_seq BIGINT NOT NULL,
    entry_schema INT NOT NULL,
    entry_payload LONGBLOB NOT NULL,
    entry_hash BINARY(32) NOT NULL,
    PRIMARY KEY (realm_id, journal_seq),
    CHECK (realm_id > 0 AND journal_seq > 0 AND entry_schema > 0 AND OCTET_LENGTH(entry_payload) > 0)
) ENGINE=InnoDB;

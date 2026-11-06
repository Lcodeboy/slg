-- 002：GAME 仍以 projection_object 保存完整快照。两类运营查询只读取下面的业务字段；
-- 写入口仍然仅有原投影事务。维护者应在专用迁移连接运行本文件，不要在业务事务中调用回填。
CREATE TABLE IF NOT EXISTS projection_player (
    realm_id INT NOT NULL,
    player_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    name VARCHAR(24) NOT NULL,
    object_version BIGINT NOT NULL,
    journal_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, player_id),
    UNIQUE KEY uq_player_account (realm_id, account_id),
    CONSTRAINT fk_player_projection_event FOREIGN KEY (realm_id, journal_seq)
        REFERENCES projection_event (realm_id, journal_seq),
    CHECK (object_version > 0 AND journal_seq > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS projection_resource_site (
    realm_id INT NOT NULL,
    x INT NOT NULL,
    y INT NOT NULL,
    occupant_player_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    occupant_march_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    generation INT NOT NULL,
    object_version BIGINT NOT NULL,
    journal_seq BIGINT NOT NULL,
    PRIMARY KEY (realm_id, x, y),
    KEY idx_resource_owner (realm_id, occupant_player_id, x, y),
    CONSTRAINT fk_site_projection_event FOREIGN KEY (realm_id, journal_seq)
        REFERENCES projection_event (realm_id, journal_seq),
    CHECK (x BETWEEN 0 AND 1199 AND y BETWEEN 0 AND 1199),
    CHECK ((occupant_player_id IS NULL AND occupant_march_id IS NULL AND generation = 0)
        OR (occupant_player_id IS NOT NULL AND occupant_march_id IS NOT NULL AND generation > 0)),
    CHECK (object_version > 0 AND journal_seq > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

DELIMITER $$
-- CREATE TABLE IF NOT EXISTS 不会修复错误的同名表。在触发器或回填第一次写专用表前确认
-- 所有参与者均支持同一事务回滚。单独重跑回填也复用这项检查。
CREATE PROCEDURE IF NOT EXISTS projection_preflight_v2()
SQL SECURITY INVOKER
BEGIN
    IF (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND engine='InnoDB'
        AND table_name IN ('projection_checkpoint','projection_event','projection_object',
                           'projection_player','projection_resource_site')) <> 5 THEN
        SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Five InnoDB projection tables are required before migration';
    END IF;
END$$
CALL projection_preflight_v2()$$

-- 插入、更新、旧库回填都经由这一个入口。调用方持有通用快照行锁，目标行在同一事务写入。
-- SQLSTATE 23000 与现有消费者的不可重试数据冲突分类一致，发生分歧整批回滚并停在原游标。
CREATE PROCEDURE IF NOT EXISTS projection_materialize_v2(
    IN p_realm INT, IN p_type VARCHAR(16) CHARACTER SET ascii, IN p_id VARCHAR(256) CHARACTER SET ascii,
    IN p_previous BIGINT, IN p_previous_seq BIGINT, IN p_before JSON,
    IN p_version BIGINT, IN p_seq BIGINT, IN p_state JSON
)
SQL SECURITY INVOKER
materialize: BEGIN
    DECLARE prev_version BIGINT DEFAULT NULL;
    DECLARE prev_seq BIGINT;
    DECLARE prev_name VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
    DECLARE prev_owner, prev_march, prev_account VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE prev_generation INT;
    DECLARE v_name TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
    DECLARE v_owner, v_march TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
    DECLARE v_x, v_y, v_generation DECIMAL(20,0);
    -- SELECT ... INTO 没有匹配专用行，表明首次应用，或正在回填一条尚未覆盖的旧快照。
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET prev_version = NULL;

    IF p_type = 'player' THEN
        -- 当前一区一账号一角色。绑定只来自 GAME 的对象键；将来拆分账号/角色需显式升级身份编码。
        IF CHAR_LENGTH(p_id) NOT BETWEEN 1 AND 64 OR REGEXP_LIKE(p_id, '[^A-Za-z0-9_-]', 'c')
            OR NOT (JSON_TYPE(JSON_EXTRACT(p_state,'$.playerId')) <=> 'STRING')
            OR NOT (CAST(JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.playerId')) AS BINARY) <=> CAST(p_id AS BINARY))
            OR NOT (JSON_TYPE(JSON_EXTRACT(p_state,'$.name')) <=> 'STRING') THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Invalid player identity or missing name in source snapshot';
        END IF;
        SET v_name = JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.name'));
        -- GAME 用 Java String.strip 和 UTF-16 code unit 计长。三个不换行空格属于 ICU 的空白，
        -- 但不属于 Java isWhitespace；已有合法名字必须原样投影。ISO 控制字符一律拒绝。
        IF OCTET_LENGTH(CONVERT(v_name USING utf16)) NOT BETWEEN 1 AND 48 OR REGEXP_LIKE(v_name,'[[:cntrl:]]')
            OR (REGEXP_LIKE(LEFT(v_name,1),'[[:space:]]') AND HEX(LEFT(v_name,1)) NOT IN ('C2A0','E28087','E280AF'))
            OR (REGEXP_LIKE(RIGHT(v_name,1),'[[:space:]]') AND HEX(RIGHT(v_name,1)) NOT IN ('C2A0','E28087','E280AF')) THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Invalid player name in source snapshot';
        END IF;
        SELECT object_version,journal_seq,account_id,name INTO prev_version,prev_seq,prev_account,prev_name
        FROM projection_player WHERE realm_id=p_realm AND player_id=p_id FOR UPDATE;
        IF prev_version IS NULL THEN
            INSERT INTO projection_player (realm_id,player_id,account_id,name,object_version,journal_seq)
            VALUES (p_realm,p_id,p_id,v_name,p_version,p_seq);
            LEAVE materialize;
        ELSEIF p_previous = p_version AND p_previous_seq = p_seq AND prev_version = p_version AND prev_seq = p_seq
               AND CAST(prev_account AS BINARY) = CAST(p_id AS BINARY)
               AND CAST(prev_name AS BINARY) = CAST(v_name AS BINARY) THEN
            LEAVE materialize;
        ELSEIF CAST(prev_account AS BINARY) <> CAST(p_id AS BINARY)
            OR NOT (JSON_CONTAINS(p_before, JSON_OBJECT('playerId',p_id,'name',prev_name)) <=> 1) THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Player query fields disagree with the previous source snapshot';
        END IF;
    ELSEIF p_type = 'resource' THEN
        -- 先核对 JSON 类型，禁止把缺失字段、数字串或小数转换为空闲状态或有效坐标。
        IF NOT (JSON_TYPE(JSON_EXTRACT(p_state,'$.x')) <=> 'INTEGER')
            OR NOT (JSON_TYPE(JSON_EXTRACT(p_state,'$.y')) <=> 'INTEGER')
            OR NOT (JSON_TYPE(JSON_EXTRACT(p_state,'$.generation')) <=> 'INTEGER')
            OR COALESCE(JSON_TYPE(JSON_EXTRACT(p_state,'$.occupantPlayerId')),'MISSING') NOT IN ('STRING','NULL')
            OR COALESCE(JSON_TYPE(JSON_EXTRACT(p_state,'$.occupantMarchId')),'MISSING') NOT IN ('STRING','NULL') THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Invalid resource field type in source snapshot';
        END IF;
        SET v_x = JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.x')), v_y = JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.y')),
            v_generation = JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.generation'));
        IF v_x NOT BETWEEN 0 AND 1199 OR v_y NOT BETWEEN 0 AND 1199
            OR CAST(p_id AS BINARY) <> CAST(CONCAT(v_x,'-',v_y) AS BINARY) THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Resource coordinates and object ID disagree';
        END IF;
        IF JSON_TYPE(JSON_EXTRACT(p_state,'$.occupantPlayerId')) = 'STRING' THEN
            SET v_owner = JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.occupantPlayerId'));
        END IF;
        IF JSON_TYPE(JSON_EXTRACT(p_state,'$.occupantMarchId')) = 'STRING' THEN
            SET v_march = JSON_UNQUOTE(JSON_EXTRACT(p_state,'$.occupantMarchId'));
        END IF;
        IF (v_owner IS NULL) <> (v_march IS NULL) OR (v_owner IS NULL AND v_generation <> 0)
            OR (v_owner IS NOT NULL AND (v_generation NOT BETWEEN 1 AND 2147483647
                OR CHAR_LENGTH(v_owner) NOT BETWEEN 1 AND 64 OR REGEXP_LIKE(v_owner,'[^A-Za-z0-9_-]', 'c')
                OR CHAR_LENGTH(v_march) NOT BETWEEN 1 AND 64 OR REGEXP_LIKE(v_march,'[^A-Za-z0-9_-]', 'c'))) THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Resource owner, march and generation disagree';
        END IF;
        SELECT object_version,journal_seq,occupant_player_id,occupant_march_id,generation
        INTO prev_version,prev_seq,prev_owner,prev_march,prev_generation
        FROM projection_resource_site WHERE realm_id=p_realm AND x=v_x AND y=v_y FOR UPDATE;
        IF prev_version IS NULL THEN
            INSERT INTO projection_resource_site
                (realm_id,x,y,occupant_player_id,occupant_march_id,generation,object_version,journal_seq)
            VALUES (p_realm,v_x,v_y,v_owner,v_march,v_generation,p_version,p_seq);
            LEAVE materialize;
        ELSEIF p_previous = p_version AND p_previous_seq = p_seq AND prev_version = p_version AND prev_seq = p_seq
            AND prev_generation = v_generation
            AND (CAST(prev_owner AS BINARY) <=> CAST(v_owner AS BINARY))
            AND (CAST(prev_march AS BINARY) <=> CAST(v_march AS BINARY)) THEN
            LEAVE materialize;
        ELSEIF NOT (JSON_CONTAINS(p_before, JSON_OBJECT('x',CAST(v_x AS SIGNED),'y',CAST(v_y AS SIGNED),
                'occupantPlayerId',prev_owner,'occupantMarchId',prev_march,'generation',prev_generation)) <=> 1) THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Resource query fields disagree with the previous source snapshot';
        END IF;
    ELSE
        LEAVE materialize;
    END IF;

    -- 同版本异值、目标表超前，或跟源行的更新前版本衔接不上，均保留现场。
    -- 合法的一页中同一对象可能变化多次，版本可以从 1 直接跳到 3。
    IF NOT (prev_version <=> p_previous) OR NOT (prev_seq <=> p_previous_seq)
        OR p_version <= prev_version OR p_seq <= prev_seq THEN
        SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Dedicated projection conflicts with source version or fields';
    END IF;
    IF p_type = 'player' THEN
        UPDATE projection_player SET account_id=p_id,name=v_name,object_version=p_version,journal_seq=p_seq
        WHERE realm_id=p_realm AND player_id=p_id;
    ELSE
        UPDATE projection_resource_site SET occupant_player_id=v_owner,occupant_march_id=v_march,
            generation=v_generation,object_version=p_version,journal_seq=p_seq
        WHERE realm_id=p_realm AND x=v_x AND y=v_y;
    END IF;
END$$

-- 先安装 UPDATE，后安装 INSERT，再开始回填。两次 DDL 之间新插入的对象可能暂时缺专用行，
-- 下一笔 UPDATE 或回填都能补上。如果先安装 INSERT，会使对象已有专用行却漏掉更新而分叉。
-- 重跑保留已就位的触发器，不为再次部署制造新的并发窗口。
CREATE TRIGGER IF NOT EXISTS projection_object_update_v2 AFTER UPDATE ON projection_object
FOR EACH ROW BEGIN
    IF NEW.realm_id <> OLD.realm_id OR NEW.object_type <> OLD.object_type
        OR CAST(NEW.object_id AS BINARY) <> CAST(OLD.object_id AS BINARY) THEN
        SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Projection object identity cannot change';
    END IF;
    IF NEW.object_type IN ('player','resource') THEN
        IF NEW.object_version < OLD.object_version OR NEW.journal_seq < OLD.journal_seq
            OR (NEW.object_version = OLD.object_version) <> (NEW.journal_seq = OLD.journal_seq) THEN
            SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Source object version and journal sequence must advance together';
        END IF;
        CALL projection_materialize_v2(NEW.realm_id,NEW.object_type,NEW.object_id,OLD.object_version,OLD.journal_seq,OLD.state_json,
                                       NEW.object_version,NEW.journal_seq,NEW.state_json);
    END IF;
END$$

CREATE TRIGGER IF NOT EXISTS projection_object_insert_v2 AFTER INSERT ON projection_object
FOR EACH ROW BEGIN
    IF NEW.object_type IN ('player','resource') THEN
        CALL projection_materialize_v2(NEW.realm_id,NEW.object_type,NEW.object_id,NULL,NULL,NULL,
                                       NEW.object_version,NEW.journal_seq,NEW.state_json);
    END IF;
END$$

-- 管理员迁移专用。每页只占用一个区的游标锁并最多触碰 128 行；投影器采用相同锁顺序。
-- 等值 UPDATE 在 MySQL 也会触发 AFTER UPDATE：迁移与在线写共用字段校验和版本判断。
-- 先前页已经提交；失败页整体回滚。重跑从头核对（包括同版本字段），不依赖临时表或游标文件。
CREATE PROCEDURE IF NOT EXISTS projection_backfill_v2()
SQL SECURITY INVOKER
BEGIN
    DECLARE v_realm, last_realm INT DEFAULT 0;
    DECLARE v_checkpoint BIGINT;
    DECLARE v_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE after_id, end_id VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RESIGNAL; END;

    CALL projection_preflight_v2();
    realms: LOOP
        SELECT MIN(realm_id) INTO v_realm FROM projection_checkpoint WHERE realm_id > last_realm;
        IF v_realm IS NULL THEN LEAVE realms; END IF;
        SET last_realm=v_realm, v_type='player';
        types_to_copy: LOOP
            SET after_id=NULL;
            pages: LOOP
                START TRANSACTION;
                SET v_checkpoint=NULL;
                SELECT last_applied_seq INTO v_checkpoint FROM projection_checkpoint WHERE realm_id=v_realm FOR UPDATE;
                IF v_checkpoint IS NULL OR (v_checkpoint > 0 AND NOT EXISTS (SELECT 1 FROM projection_event
                    WHERE realm_id=v_realm AND journal_seq=v_checkpoint)) THEN
                    SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Realm checkpoint or its archived boundary is missing';
                END IF;
                SELECT MAX(page.object_id) INTO end_id FROM (
                    SELECT object_id FROM projection_object
                    WHERE realm_id=v_realm AND object_type=v_type AND (after_id IS NULL OR object_id > after_id)
                    ORDER BY object_id LIMIT 128
                ) AS page;
                IF end_id IS NULL THEN COMMIT; LEAVE pages; END IF;
                IF EXISTS (SELECT 1 FROM projection_object WHERE realm_id=v_realm AND object_type=v_type
                    AND (after_id IS NULL OR object_id > after_id) AND object_id <= end_id AND journal_seq > v_checkpoint) THEN
                    SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Object row ahead of the locked realm checkpoint';
                END IF;
                UPDATE projection_object SET object_version=object_version
                WHERE realm_id=v_realm AND object_type=v_type AND (after_id IS NULL OR object_id > after_id) AND object_id <= end_id;
                COMMIT;
                SET after_id=end_id;
            END LOOP;
            IF v_type='resource' THEN LEAVE types_to_copy; END IF;
            SET v_type='resource';
        END LOOP;
    END LOOP;
    -- 从源表枚举无法发现查询表里多出的孤行。只读核验用一致快照，正常在线事务会同时显示
    -- 原行和专用行；孤行要求停下来核对来源，不能把它当成查询结果，也不能自行删除。
    COMMIT;
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;
    IF EXISTS (SELECT 1 FROM projection_object o LEFT JOIN projection_player p
        ON p.realm_id=o.realm_id AND p.player_id=o.object_id
        WHERE o.object_type='player' AND (p.realm_id IS NULL OR p.object_version <> o.object_version
            OR p.journal_seq <> o.journal_seq OR p.account_id <> p.player_id
            OR NOT (JSON_CONTAINS(o.state_json, JSON_OBJECT('playerId',p.player_id,'name',p.name)) <=> 1))
    ) OR EXISTS (SELECT 1 FROM projection_object o LEFT JOIN projection_resource_site s ON s.realm_id=o.realm_id
        AND s.x=CAST(JSON_EXTRACT(o.state_json,'$.x') AS SIGNED) AND s.y=CAST(JSON_EXTRACT(o.state_json,'$.y') AS SIGNED)
        WHERE o.object_type='resource' AND (s.realm_id IS NULL OR s.object_version <> o.object_version
            OR s.journal_seq <> o.journal_seq OR CAST(o.object_id AS BINARY) <> CAST(CONCAT(s.x,'-',s.y) AS BINARY)
            OR NOT (JSON_CONTAINS(o.state_json, JSON_OBJECT('x',s.x,'y',s.y,'occupantPlayerId',s.occupant_player_id,
                'occupantMarchId',s.occupant_march_id,'generation',s.generation)) <=> 1))
    ) THEN
        SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Missing or divergent player or resource query row after backfill';
    END IF;
    IF EXISTS (SELECT 1 FROM projection_player p WHERE NOT EXISTS (
        SELECT 1 FROM projection_object o WHERE o.realm_id=p.realm_id AND o.object_type='player' AND o.object_id=p.player_id
    )) OR EXISTS (SELECT 1 FROM projection_resource_site s WHERE NOT EXISTS (
        SELECT 1 FROM projection_object o WHERE o.realm_id=s.realm_id AND o.object_type='resource'
            AND o.object_id=CONCAT(s.x,'-',s.y)
    )) THEN
        SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'Orphan player or resource in dedicated projection';
    END IF;
    COMMIT;
END$$

DELIMITER ;
CALL projection_backfill_v2();

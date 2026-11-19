package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.repository.MySqlStateRepository;
import com.slg.game.infrastructure.mysql.session.MySqlSessions;
import com.slg.game.infrastructure.mysql.metrics.MySqlStorageMetrics;
import java.time.Duration;
import javax.sql.DataSource;

/**
 * 存储端口的组合入口，仅构造无会话状态的适配器，不在此执行 SQL、初始化库或启动服务。
 * DataSource（通常为 HikariCP）的连接/等待/网络预算与关闭由外部组合根负责。
 * MYSQL-03 的有界执行器分别注入所需端口；不能在网络线程直接调用这些阻塞实现。
 */
public final class MySqlRealmStorage {
    private final RealmStateReader states;
    private final RealmReceiptReader receipts;
    private final RealmEventReader events;
    private final RealmWorkReader work;
    private final RealmTransaction transactions;
    private final RealmOwnership ownership;

    public MySqlRealmStorage(DataSource dataSource, int statementTimeoutSeconds, Duration leaseDuration) {
        this(dataSource, statementTimeoutSeconds, leaseDuration, new MySqlStorageMetrics());
    }

    /** 四池装配可向各实例注入同一观测器，指标聚合不意味着连接池或 Session 被共享。 */
    public MySqlRealmStorage(DataSource dataSource, int statementTimeoutSeconds, Duration leaseDuration,
                            MySqlStorageMetrics metrics) {
        var sessions = new MySqlSessions(dataSource, statementTimeoutSeconds);
        var codec = new PersistenceCodec();
        var repository = new MySqlStateRepository(codec);
        states = new MySqlRealmStateReader(sessions, repository);
        receipts = new MySqlRealmReceiptReader(sessions, codec);
        events = new MySqlRealmEventReader(sessions, codec);
        work = new MySqlRealmWorkReader(sessions, repository);
        transactions = new MySqlRealmTransaction(sessions, repository, codec, metrics);
        ownership = new MySqlRealmOwnership(sessions, leaseDuration, metrics);
    }

    public RealmStateReader states() { return states; }
    public RealmReceiptReader receipts() { return receipts; }
    public RealmEventReader events() { return events; }
    public RealmWorkReader work() { return work; }
    public RealmTransaction transactions() { return transactions; }
    public RealmOwnership ownership() { return ownership; }
}

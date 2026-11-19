package com.slg.game;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

/** 将已有会话统计接到平台 MBeanServer，注册与注销跟随 GAME 生命周期。 */
final class SessionMetrics implements SessionMetricsMBean, AutoCloseable {
    private final MBeanServer server;
    private final ObjectName name;
    private final SessionDirectory sessions;
    private boolean closed;

    static SessionMetrics register(int realmId, SessionDirectory sessions) throws IOException {
        return new SessionMetrics(ManagementFactory.getPlatformMBeanServer(), realmId, sessions);
    }

    SessionMetrics(MBeanServer server, int realmId, SessionDirectory sessions) throws IOException {
        this.server = server;
        this.sessions = sessions;
        try {
            name = new ObjectName("com.slg.game:type=Sessions,realm=" + realmId);
            // 只公开接口中的属性，close() 不会成为 JMX 操作。
            server.registerMBean(new StandardMBean(this, SessionMetricsMBean.class), name);
        } catch (JMException failure) {
            throw new IOException("Cannot register session metrics for realm " + realmId, failure);
        }
    }

    @Override public int getSessionBindings() {
        // 与 /health 同一口径：有界内存快照，不查数据库，不扫描玩家。
        return (Integer) sessions.metrics().get("bindings");
    }

    @Override public synchronized void close() throws IOException {
        if (closed) return;
        try {
            server.unregisterMBean(name);
            closed = true;
        } catch (InstanceNotFoundException alreadyRemoved) {
            closed = true;
        } catch (JMException failure) {
            throw new IOException("Cannot unregister session metrics " + name, failure);
        }
    }
}

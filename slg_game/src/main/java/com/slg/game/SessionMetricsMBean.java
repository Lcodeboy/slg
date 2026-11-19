package com.slg.game;

/** JConsole 中的只读业务属性，不提供会话修改操作。 */
public interface SessionMetricsMBean {
    /** 当前 GAME 会话目录中的绑定数；租约失效后由已有清理逻辑移除。 */
    int getSessionBindings();
}

package com.slg.game.application.player;

import com.slg.game.application.execution.OperationHandler;
import com.slg.game.application.execution.QualifiedQueryHandler;
import java.util.Map;

/**
 * 启动时显式注册一组玩法；每区共用同一执行器及上下文，不按请求注册 handler 或自行创建邮箱。
 * 操作名称必须跨读写/控制唯一；注册失败使装配失败，不用覆盖行为隐藏两个模块的名称冲突。
 */
public interface PlayerUseCaseModule {
    default Map<String, OperationHandler> writeHandlers(PlayerOperationContext context) { return Map.of(); }
    default Map<String, QualifiedQueryHandler> queryHandlers(PlayerOperationContext context) { return Map.of(); }
    /** 系统入口单独列入白名单，客户端不能选择后台操作名以绕过会话或使用系统预留预算。 */
    default Map<String, OperationHandler> systemWriteHandlers(PlayerOperationContext context) { return Map.of(); }
    default Map<String, QualifiedQueryHandler> systemQueryHandlers(PlayerOperationContext context) { return Map.of(); }
}

package com.slg.game.application.execution;

import java.util.concurrent.CompletionStage;

/**
 * 装配时注册的只读或内存会话控制用例，与写操作共享账号/对象/请求资格。
 * 不得写权威数据库或伪造提交决定；冷读必须通过有界异步读取端口，不能阻塞准备线程等待 JDBC。
 */
public interface QualifiedQueryHandler {
    CompletionStage<OperationValue> execute(OperationCommand command) throws Exception;
    /** 注册时固定结果最大编码字节数，准入时预留；超限结果以异常失败，不进入提交器。 */
    int maxResultBytes();
}

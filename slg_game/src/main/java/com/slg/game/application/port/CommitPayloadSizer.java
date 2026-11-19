package com.slg.game.application.port;

import com.slg.game.application.commit.CommitRequest;

/**
 * 在存储准入前校验并计量完整不可变决定及调度元数据，返回正数计费字节。
 * 实现由版本化 codec 提供，不接受调用方自报长度；非法/不支持/超过格式上限抛非法参数异常。
 * 本端口不得访问数据库、修改决定或持有活跃玩家对象，应用层不因此依赖具体存储框架。
 */
@FunctionalInterface
public interface CommitPayloadSizer {
    long measure(CommitRequest request);
}

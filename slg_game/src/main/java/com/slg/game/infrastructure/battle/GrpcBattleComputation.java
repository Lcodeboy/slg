package com.slg.game.infrastructure.battle;

import com.slg.core.battle.*;
import com.slg.game.application.port.BattleComputation;
import com.slg.rpc.BattleWire;
import com.slg.rpc.BattleWorkerClient;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * 新应用到现有 Worker 协议的适配。权威 JSON 采用整数编码，其 persistedHash 与旧 wire
 * 规范摘要属于不同格式；这里另算传输摘要，不能覆盖调度器保留的持久摘要或用它结算。
 * 客户端连接/关闭仍由组合根管理，本适配器不新建线程池或重试计算。
 */
public final class GrpcBattleComputation implements BattleComputation {
    private final BattleWorkerClient client;
    public GrpcBattleComputation(BattleWorkerClient client) { this.client = Objects.requireNonNull(client); }
    @Override public CompletionStage<BattleResult> compute(BattleInput input, String persistedHash, String rulesVersion) {
        Objects.requireNonNull(input); Objects.requireNonNull(rulesVersion);
        if (persistedHash == null || !persistedHash.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Invalid persisted battle input digest");
        // 输入在存储 codec 中已按其原文验过持久摘要；wire 对 typed 输入另做往返摘要校验。
        return client.compute(input, BattleWire.inputHash(input), rulesVersion).minimalCompletionStage();
    }
}

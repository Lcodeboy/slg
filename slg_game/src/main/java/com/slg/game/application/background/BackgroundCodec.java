package com.slg.game.application.background;

import com.slg.core.battle.*;
import com.slg.game.application.march.MarchPlan;
import com.slg.game.domain.march.BattleReport;
import com.slg.game.domain.persistence.*;
import java.util.List;
import java.util.Optional;

/** 后台命令、系统回执与战斗固定载荷的有界编码端口；不持有 SQL、会话或 Worker 连接。 */
public interface BackgroundCodec {
    byte[] encodeInvocation(BackgroundInvocation invocation);
    BackgroundInvocation decodeInvocation(byte[] bytes);
    RequestKey request(VersionedState candidate);
    String digest(BackgroundInvocation invocation);
    String encodeOutcome(BackgroundOutcome outcome);
    BackgroundOutcome decodeOutcome(String json);
    String fact(BackgroundInvocation invocation, List<String> phases, String reason);
    BattleInput input(BattleState battle);
    JsonPayload result(BattleResult result);
    BattleReport report(BattleState battle);
    byte[] encodeDispatch(Optional<BattleDispatch> dispatch);
    Optional<BattleDispatch> decodeDispatch(byte[] bytes);
    byte[] encodeReport(BattleReport report);
    BattleReport decodeReport(byte[] bytes);
    byte[] encodePlan(MarchPlan plan);
    MarchPlan decodePlan(byte[] bytes);
    int maxWorkBytes();
}

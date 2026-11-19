package com.slg.game.application.march;

import com.slg.game.domain.march.PendingBattle;
import com.slg.game.domain.march.StartMarchOrder;
import com.slg.game.domain.persistence.BattleState;
import com.slg.game.domain.persistence.MarchState;
import java.util.List;

/** 行军输入、规划、原响应及事实编码端口；只编码有界值，不执行玩法或数据库操作。 */
public interface MarchPayloadCodec {
    byte[] encodeInvocation(MarchInvocation invocation);
    MarchInvocation decodeInvocation(byte[] bytes);
    String startDigest(StartMarchOrder order);
    String recallDigest(String marchId);
    byte[] encodePlan(MarchPlan plan);
    MarchPlan decodePlan(byte[] bytes);
    byte[] encodeMarch(MarchState march);
    MarchState decodeMarch(byte[] bytes);
    String fact(String operation, long accountId, String marchId, List<String> phases, String rejectionCode);
    BattleState prepareBattle(PendingBattle battle);
    int maxResultBytes();
}

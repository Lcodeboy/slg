package com.slg.game.realm;

import com.slg.rpc.v1.CreatePlayerReq;
import com.slg.rpc.v1.GetBattleReq;
import com.slg.rpc.v1.GetMarchReq;
import com.slg.rpc.v1.GetReceiptReq;
import com.slg.rpc.v1.RecallMarchReq;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopGroup;
import com.slg.rpc.v1.TroopType;
import com.slg.game.domain.GameException;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** 使用与客户端相同的独立请求类型构造测试输入，领域拒绝保留真实的业务错误码。 */
public final class GameRequests {
    private GameRequests() {}

    public static CreatePlayerReq named(String name) {
        return CreatePlayerReq.newBuilder().setName(name).build();
    }

    public static StartMarchReq gather(long infantry) {
        return StartMarchReq.newBuilder().setTargetX(150).setTargetY(150)
                .addTroops(troop(TroopType.TROOP_TYPE_INFANTRY, infantry)).build();
    }

    public static TroopGroup troop(TroopType type, long count) {
        return TroopGroup.newBuilder().setType(type).setCount(count).build();
    }

    public static GetReceiptReq receiptFor(String id) {
        return GetReceiptReq.newBuilder().setTargetRequestId(id).build();
    }

    public static GetMarchReq marchById(String id) {
        return GetMarchReq.newBuilder().setMarchId(id).build();
    }

    public static RecallMarchReq recallOrder(String id) {
        return RecallMarchReq.newBuilder().setMarchId(id).build();
    }

    public static GetBattleReq battleById(String id) {
        return GetBattleReq.newBuilder().setBattleId(id).build();
    }

    public static String rejection(Executable work) {
        return assertThrows(GameException.class, work).code();
    }
}

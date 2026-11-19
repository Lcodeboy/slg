package com.slg.game;

import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GameRpcObservationTest {
    @Test void successfulDecisionAndUnauthorizedReplyAreSeparateFromClientAcknowledgment() throws Exception {
        try (var fixture = new PlayerApplicationFixture()) {
            var denied = new PlayerApplicationFixture.Reply<PlayReply>();
            fixture.gameplay.play(PlayRequest.newBuilder().setMsgId(GameplayRoutes.CREATE_PLAYER).setRequestId("unauthorized")
                    .setBody(CreatePlayerReq.newBuilder().setName("observer").build().toByteString()).build(), denied);
            assertEquals(401, denied.get().getStatus());
            assertFalse(fixture.play(GameplayRoutes.CREATE_PLAYER, "accepted", CreatePlayerReq.newBuilder().setName("observer").build()).get().hasError());
            assertEquals(1L, fixture.gameplay.metrics().get("successfulReplies"));
            assertEquals(1L, fixture.gameplay.metrics().get("rejectedReplies"));
            assertEquals(2L, inbound(fixture.gameplay).get("count"));
            assertEquals(1L, inbound(fixture.gameplay).get("failures"));
            assertEquals(1, fixture.commits.sequence.get());
        }
    }

    @Test void cancelledQueuedRpcRecordsOnceWithoutInventingAReplyOrCommit() throws Exception {
        try (var fixture = new PlayerApplicationFixture(); var context = fixture.context.withCancellation()) {
            fixture.commits.automatic = false;
            var executing = fixture.play(GameplayRoutes.CREATE_PLAYER, "first", CreatePlayerReq.newBuilder().setName("kept").build());
            var pending = fixture.commits.next();
            var cancelled = fixture.play(context, GameplayRoutes.CREATE_PLAYER, "cancelled", CreatePlayerReq.newBuilder().setName("discarded").build());
            context.cancel(null);
            assertEquals(1L, fixture.gameplay.metrics().get("cancelledBeforeReply"));
            assertFalse(cancelled.value.isDone()); assertEquals(0, fixture.commits.sequence.get());
            // 取消排队请求不会取消前一项内部提交，也不为取消项补写拒绝回执。
            pending.commit(); assertFalse(executing.get().hasError());
            assertEquals(1, fixture.commits.sequence.get()); assertEquals(1, fixture.commits.submissions.get());
            assertEquals(2L, inbound(fixture.gameplay).get("count")); assertEquals(1L, inbound(fixture.gameplay).get("failures"));
        }
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> inbound(GameRpcService rpc) {
        return (Map<String, Object>) rpc.metrics().get("inbound");
    }
}

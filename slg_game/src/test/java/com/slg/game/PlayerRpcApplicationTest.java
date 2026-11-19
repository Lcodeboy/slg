package com.slg.game;

import com.slg.game.application.commit.*;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import io.grpc.Context;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/** 真实应用调度/缓存/编解码与 RPC，只有权威端口用确定性内存替身；不启动数据库。 */
@Timeout(15)
class PlayerRpcApplicationTest {
    @Test void playerRoutesPreserveOriginalReceiptsAndRejectUnregisteredGameplay() throws Exception {
        try (var fixture = new PlayerApplicationFixture()) {
            var created = fixture.play(GameplayRoutes.CREATE_PLAYER, "create", CreatePlayerReq.newBuilder().setName(" 城主 ").build()).get();
            assertEquals(200, created.getStatus()); assertFalse(created.hasError());
            var original = CreatePlayerRsp.parseFrom(created.getBody()).getPlayer();
            assertEquals("城主", original.getName()); assertEquals(1, original.getVersion());
            assertEquals(Map.of("WOOD", 0L), original.getResourcesMap());
            assertEquals(Map.of("INFANTRY", 3000L, "LANCER", 1000L, "MARKSMAN", 1000L), original.getTroopsMap());
            assertTrue(original.getWoundedMap().isEmpty()); assertTrue(original.getDeadMap().isEmpty());
            assertEquals("sergey", original.getHeroes(0).getHeroId()); assertFalse(original.getHeroes(0).hasAssignedMarch());

            var queried = fixture.play(GameplayRoutes.GET_PLAYER, "query", GetPlayerReq.getDefaultInstance()).get();
            assertEquals(original, GetPlayerRsp.parseFrom(queried.getBody()).getPlayer());
            var receipt = fixture.receipt("lookup", "create");
            assertEquals(1, receipt.getJournalSeq());
            assertEquals(GameplayRoutes.CREATE_PLAYER, receipt.getReceipt().getMsgId());
            assertEquals(original, receipt.getReceipt().getCreated().getPlayer());
            var absent = fixture.receipt("missing", "never-issued");
            assertFalse(absent.hasReceipt()); assertEquals(0, absent.getJournalSeq());

            var rejected = fixture.play(GameplayRoutes.CREATE_PLAYER, "second", CreatePlayerReq.newBuilder().setName("other").build()).get();
            assertEquals("PLAYER_EXISTS", rejected.getError().getCode());
            var rejectionReceipt = fixture.receipt("lookup-rejection", "second");
            assertEquals(GameplayRoutes.CREATE_PLAYER, rejectionReceipt.getReceipt().getMsgId());
            assertEquals("PLAYER_EXISTS", rejectionReceipt.getReceipt().getError().getCode());
            var replay = fixture.play(GameplayRoutes.CREATE_PLAYER, "create", CreatePlayerReq.newBuilder().setName(" 城主 ").build()).get();
            assertEquals(original, CreatePlayerRsp.parseFrom(replay.getBody()).getPlayer());

            int submissions = fixture.commits.submissions.get();
            for (int type : List.of(GameplayRoutes.START_MARCH, GameplayRoutes.GET_MARCH, GameplayRoutes.RECALL_MARCH, GameplayRoutes.GET_BATTLE)) {
                var unavailable = fixture.play(type, "unsupported-" + type, GetPlayerReq.getDefaultInstance()).get();
                assertEquals(503, unavailable.getStatus()); assertEquals("SERVER_BUSY", unavailable.getError().getCode());
            }
            assertEquals(submissions, fixture.commits.submissions.get(), "An unregistered module must never reach a storage path");
        }
    }

    @Test void sessionOpenSharesAccountQualificationUntilCommitAndPublicationThenFencesQueuedOldCommands() throws Exception {
        try (var fixture = new PlayerApplicationFixture()) {
            fixture.commits.automatic = false;
            var creation = fixture.play(GameplayRoutes.CREATE_PLAYER, "prior", CreatePlayerReq.newBuilder().setName("prior").build());
            var pending = fixture.commits.next();
            var replacement = fixture.open();
            var stale = fixture.play(GameplayRoutes.CREATE_PLAYER, "old-queued", CreatePlayerReq.newBuilder().setName("wrong").build());
            assertFalse(replacement.value.isDone()); assertFalse(stale.value.isDone());
            assertEquals(2, fixture.application.stats().waiting());
            pending.commit();
            assertFalse(creation.get().hasError());
            var next = replacement.get();
            var denied = stale.get();
            assertEquals(401, denied.getStatus()); assertEquals("SESSION_REPLACED", denied.getError().getCode());
            assertEquals(1, fixture.commits.submissions.get(), "Stale binding must not create a rejection receipt");
            assertTrue(fixture.sessions.isCurrent(fixture.tokens.verify(fixture.token), next.getKey()));
            assertEquals(1, fixture.commits.receipts.size());
        }
    }

    @Test void cancelledRpcCannotReleaseAnExecutingCommitOrLetSessionReplacementPassIt() throws Exception {
        try (var fixture = new PlayerApplicationFixture(); var cancelled = fixture.context.withCancellation()) {
            fixture.commits.automatic = false;
            var creation = fixture.play(cancelled, GameplayRoutes.CREATE_PLAYER, "cancel-running", CreatePlayerReq.newBuilder().setName("kept").build());
            var pending = fixture.commits.next();
            cancelled.cancel(null);
            var replacement = fixture.open();
            assertFalse(replacement.value.isDone());
            assertFalse(pending.settlement.isDone(), "Network cancellation cannot cancel the internal settlement");
            pending.commit();
            var next = replacement.get();
            assertFalse(creation.value.isDone(), "Cancelled transport must not receive a late response");
            assertEquals(1, fixture.commits.receipts.size());
            fixture.context = Context.current().withValues(RpcMetadata.TOKEN, fixture.token, RpcMetadata.SESSION, next.getKey());
            assertEquals("kept", fixture.receipt("lookup-after-cancel", "cancel-running").getReceipt().getCreated().getPlayer().getName());
        }
    }

    @Test void cancellingQueuedOpenDoesNotReplaceCurrentSession() throws Exception {
        try (var fixture = new PlayerApplicationFixture(); var cancelled = fixture.context.withCancellation()) {
            fixture.commits.automatic = false;
            var creation = fixture.play(GameplayRoutes.CREATE_PLAYER, "blocking", CreatePlayerReq.newBuilder().setName("kept").build());
            var pending = fixture.commits.next();
            var opening = new PlayerApplicationFixture.Reply<SessionBinding>();
            var request = SessionDirectoryTest.request(fixture.sessions, fixture.tokens.verify(fixture.token));
            cancelled.run(() -> fixture.controls.open(request, opening));
            assertEquals(1, fixture.application.stats().waiting());
            cancelled.cancel(null);
            pending.commit(); creation.get();
            var query = fixture.play(GameplayRoutes.GET_PLAYER, "still-current", GetPlayerReq.getDefaultInstance()).get();
            assertFalse(query.hasError()); assertFalse(opening.value.isDone());
            assertEquals(1L, fixture.sessions.metrics().get("opened"));
        }
    }

    @ParameterizedTest
    @CsvSource({"QUEUE_FULL,503,SERVER_BUSY", "QUEUE_TIMEOUT,504,DEADLINE_EXCEEDED", "VERSION_CONFLICT,409,STATE_CHANGED", "REQUEST_CONFLICT,409,REQUEST_CONFLICT"})
    void determinedCommitRejectionsHaveExplicitWireMeaning(CommitResult.Reason reason, int status, String code) throws Exception {
        try (var fixture = new PlayerApplicationFixture()) {
            fixture.commits.automatic = false;
            var response = fixture.play(GameplayRoutes.CREATE_PLAYER, "reject", CreatePlayerReq.newBuilder().setName("new").build());
            fixture.commits.next().settlement.complete(new CommitResult.NotCommitted(reason));
            var denied = response.get();
            assertEquals(status, denied.getStatus()); assertEquals(code, denied.getError().getCode());
            assertTrue(fixture.commits.receipts.isEmpty());
        }
    }

}

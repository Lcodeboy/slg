package com.slg.battle;

import com.slg.core.battle.*;
import com.slg.rpc.BattleRpcTransport;
import com.slg.rpc.BattleWire;
import com.slg.rpc.BattleWorkerClient;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.slg.rpc.v1.BattleComputeReply;
import com.slg.rpc.v1.BattleComputeRequest;
import com.slg.rpc.v1.BattleWorkerRpcGrpc;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class BattleWorkerTest {
    private static final String RULES = ExpeditionBattleEngine.RULES_VERSION;
    private final ExpeditionBattleEngine engine = new ExpeditionBattleEngine();
    private final BattleInput input = new BattleInput("test-battle",
            new Army("player", List.of(new TroopGroup("guards", TroopType.INFANTRY, 3, 10_000))),
            new Army("npc", List.of(new TroopGroup("archers", TroopType.MARKSMAN, 2, 8_000))), 87, 100);
    private final String hash = BattleWire.inputHash(input);

    @Test void duplicateRpcProducesSameResultAndRejectsInvalidInputsAndPlayerTokens() throws Exception {
        var pool = BattleWorker.pool(2, 8);
        try (var remote = new Remote(new BattleService(pool, engine::simulate))) {
            BattleResult expected = engine.simulate(input);
            assertEquals(expected, remote.client.compute(input, hash, RULES).get(5, TimeUnit.SECONDS));
            assertEquals(expected, remote.client.compute(input, hash, RULES).get(5, TimeUnit.SECONDS));

            var request = BattleWire.request(input, hash, RULES);
            var stub = BattleWorkerRpcGrpc.newBlockingStub(remote.channel);
            assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
                    () -> stub.compute(request.toBuilder().setSeed(99).build())).getStatus().getCode());
            assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
                    () -> stub.compute(request.toBuilder().setMaxRounds(1001).build())).getStatus().getCode());
            assertEquals(Status.Code.FAILED_PRECONDITION, assertThrows(StatusRuntimeException.class,
                    () -> stub.compute(request.toBuilder().setRulesVersion("future-rules").build())).getStatus().getCode());
            Metadata headers = new Metadata();
            headers.put(RpcMetadata.AUTHORIZATION, "Bearer player-session");
            var playerStub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            assertEquals(Status.Code.PERMISSION_DENIED, assertThrows(StatusRuntimeException.class,
                    () -> playerStub.compute(request)).getStatus().getCode());
        } finally { pool.shutdownNow(); }
    }

    @Test void saturatedWorkerRejectsPromptlyAndRemovesCancelledAndExpiredRequests() throws Exception {
        var pool = BattleWorker.pool(1, 1);
        var running = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var service = new BattleService(pool, battle -> {
            running.countDown();
            try {
                if (!release.await(12, TimeUnit.SECONDS)) throw new IllegalStateException("Test did not release worker");
            } catch (InterruptedException e) { throw new IllegalStateException(e); }
            return engine.simulate(battle);
        });
        try (var remote = new Remote(service)) {
            var request = BattleWire.request(input, hash, RULES);
            var stub = BattleWorkerRpcGrpc.newFutureStub(remote.channel);
            // Occupy the CPU worker independently of the normal GAME deadline.
            var first = stub.withDeadlineAfter(10, TimeUnit.SECONDS).compute(request);
            assertTrue(running.await(3, TimeUnit.SECONDS));
            var queued = remote.client.compute(input, hash, RULES);
            awaitQueueSize(pool, 1);
            var rejected = remote.client.compute(input, hash, RULES);
            var busy = assertThrows(ExecutionException.class, () -> rejected.get(2, TimeUnit.SECONDS));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, Status.fromThrowable(busy).getCode());

            assertTrue(queued.cancel(false));
            awaitQueueSize(pool, 0);
            var expiring = stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS).compute(request);
            awaitQueueSize(pool, 1);
            var expired = assertThrows(ExecutionException.class, () -> expiring.get(3, TimeUnit.SECONDS));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(expired).getCode());
            awaitQueueSize(pool, 0);

            release.countDown();
            assertEquals(BattleWire.reply(engine.simulate(input), hash), first.get(3, TimeUnit.SECONDS));
            assertEquals(engine.simulate(input), remote.client.compute(input, hash, RULES).get(3, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test void gameRejectsMismatchedReplyOverTheWire() throws Exception {
        var wrong = BattleWire.reply(engine.simulate(input), hash).toBuilder().setBattleId("another-battle").build();
        var service = new BattleWorkerRpcGrpc.BattleWorkerRpcImplBase() {
            @Override public void compute(BattleComputeRequest request, StreamObserver<BattleComputeReply> response) {
                response.onNext(wrong);
                response.onCompleted();
            }
        };
        try (var remote = new Remote(service)) {
            var submit = remote.client.compute(input, hash, RULES);
            var failure = assertThrows(ExecutionException.class, () -> submit.get(3, TimeUnit.SECONDS));
            assertEquals(Status.Code.DATA_LOSS, Status.fromThrowable(failure).getCode());
        }
    }

    @Test void replyMustConserveOriginalGroupsAndMatchRulesAndDigest() {
        var reply = BattleWire.reply(engine.simulate(input), hash);
        var swapped = reply.toBuilder().setAttacker(reply.getDefender()).build();
        var casualties = reply.getAttacker().getGroups(0).toBuilder().setSurvivors(Long.MAX_VALUE);
        var forged = reply.toBuilder().setAttacker(reply.getAttacker().toBuilder().setGroups(0, casualties)).build();
        for (var invalid : List.of(swapped, forged, reply.toBuilder().setRulesVersion("other").build(),
                reply.toBuilder().setInputHash("wrong").build(), reply.toBuilder().setRounds(0).build())) {
            assertThrows(IllegalArgumentException.class, () -> BattleWire.result(invalid, input, hash, RULES));
        }
    }

    private static void awaitQueueSize(ThreadPoolExecutor pool, int expected) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (pool.getQueue().size() != expected && System.nanoTime() < limit) Thread.sleep(5);
        assertEquals(expected, pool.getQueue().size());
    }

    private static final class Remote implements AutoCloseable {
        private final Server server;
        private final ManagedChannel channel;
        private final BattleWorkerClient client;
        Remote(BindableService service) throws Exception {
            server = BattleRpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0)).addService(service).build().start();
            String target = "127.0.0.1:" + server.getPort();
            channel = RpcTransport.channel(target);
            client = new BattleWorkerClient(target);
        }
        @Override public void close() throws InterruptedException {
            client.close();
            channel.shutdownNow();
            server.shutdownNow();
            channel.awaitTermination(3, TimeUnit.SECONDS);
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }
}

package com.slg.game.infrastructure.battle;

import com.slg.core.battle.*;
import com.slg.game.domain.march.PendingBattle;
import com.slg.game.infrastructure.mysql.codec.MarchPayloadJsonCodec;
import com.slg.rpc.*;
import com.slg.rpc.v1.BattleComputeReply;
import com.slg.rpc.v1.BattleComputeRequest;
import com.slg.rpc.v1.BattleWorkerRpcGrpc;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class GrpcBattleComputationTest {
    @Test void adaptsPersistedDigestToWireDigestWithoutChangingTheFixedInput() throws Exception {
        var input = new BattleInput("battle-1", new Army("1_7", List.of(new TroopGroup("a", TroopType.INFANTRY, 1, 200))),
                new Army("npc", List.of(new TroopGroup("d", TroopType.INFANTRY, 1, 20))), 7, 100);
        var persisted = new MarchPayloadJsonCodec().prepareBattle(new PendingBattle(input.battleId(), "1_7", List.of(),
                2, ExpeditionBattleEngine.RULES_VERSION, input));
        var observed = new CompletableFuture<BattleComputeRequest>();
        var worker = BattleRpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0))
                .addService(new BattleWorkerRpcGrpc.BattleWorkerRpcImplBase() {
                    @Override public void compute(BattleComputeRequest request, StreamObserver<BattleComputeReply> reply) {
                        observed.complete(request);
                        var result = new ExpeditionBattleEngine().simulate(BattleWire.input(request));
                        reply.onNext(BattleWire.reply(result, request.getInputHash())); reply.onCompleted();
                    }
                }).build().start();
        try (var client = new BattleWorkerClient("127.0.0.1:" + worker.getPort())) {
            var result = new GrpcBattleComputation(client).compute(input, persisted.inputHash(), persisted.rulesVersion())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            var request = observed.get(5, TimeUnit.SECONDS);
            assertNotEquals(persisted.inputHash(), request.getInputHash());
            assertEquals(BattleWire.inputHash(input), request.getInputHash());
            assertEquals(input, BattleWire.input(request));
            assertEquals(new ExpeditionBattleEngine().simulate(input), result);
        } finally {
            worker.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}

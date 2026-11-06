package com.slg.rpc;

import com.slg.rpc.v1.BattleComputeReply;
import com.slg.rpc.v1.BattleComputeRequest;
import com.slg.rpc.v1.BattleWorkerRpcGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BattleRpcTransportTest {
    @Test void onlyExplicitLoopbackMayUsePlaintext() {
        for (String local : List.of("127.0.0.1:9083", "127.9.8.7:9083", "localhost:9083", "[::1]:9083",
                "dns:///[0:0:0:0:0:0:0:1]:9083", "dns:///127.0.0.1:9083")) {
            assertTrue(BattleRpcTransport.loopbackTarget(local), local);
        }
        for (String remote : List.of("battle:9083", "dns:///battle.svc:9083", "[2001:db8::1]:9083",
                "dns:///[::]:9083", "0.0.0.0:9083", "localhost.example:9083", "127.0.0.1.example:9083",
                "2130706433:9083")) {
            assertFalse(BattleRpcTransport.loopbackTarget(remote), remote);
            assertThrows(IllegalArgumentException.class, () -> BattleRpcTransport.channel(remote, Map.of()));
            assertThrows(IllegalArgumentException.class, () -> BattleRpcTransport.channel(remote,
                    Map.of("SLG_RPC_TLS_ENABLED", "true", "SLG_RPC_TLS_CA", "dummy-ca")));
        }
        for (String invalid : List.of("dns://resolver/localhost:9083", "127.0.0.1@battle:9083", "dns:///x:9083/path",
                "localhost:9083?route=other", "unix:///test", "localhost:0", "127.1:9083", "127.0.0.1:65536",
                "127%2e0.0.1:9083")) {
            assertThrows(IllegalArgumentException.class, () -> BattleRpcTransport.loopbackTarget(invalid), invalid);
        }
    }

    @Test void remoteWorkerCannotUseGeneralServiceCaForCallerIdentity() {
        var remote = new InetSocketAddress("0.0.0.0", 0);
        assertThrows(IllegalArgumentException.class, () -> BattleRpcTransport.serverBuilder(remote, Map.of()));
        var generic = Map.of("SLG_RPC_TLS_CA", "ordinary", "SLG_RPC_TLS_CERT", "cert", "SLG_RPC_TLS_KEY", "key");
        var failure = assertThrows(IllegalArgumentException.class, () -> BattleRpcTransport.serverBuilder(remote, generic));
        assertTrue(failure.getMessage().contains("SLG_BATTLE_GAME_CLIENT_CA"));
    }

    @Test void workerTrustsOnlyClientsSignedByTheGameCa() throws Exception {
        String serviceCa = fixture("ca.pem");
        String gameCa = fixture("game-ca.pem");
        String serviceCert = fixture("peer.pem"), serviceKey = fixture("peer-key.pem");
        String gameCert = fixture("game-client.pem"), gameKey = fixture("game-client-key.pem");
        var worker = BattleRpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0), Map.of(
                "SLG_RPC_TLS_CA", serviceCa, "SLG_BATTLE_GAME_CLIENT_CA", gameCa,
                "SLG_RPC_TLS_CERT", serviceCert, "SLG_RPC_TLS_KEY", serviceKey))
                .addService(new BattleWorkerRpcGrpc.BattleWorkerRpcImplBase() {
                    @Override public void compute(BattleComputeRequest request, StreamObserver<BattleComputeReply> reply) {
                        reply.onNext(BattleComputeReply.newBuilder().setBattleId(request.getBattleId()).build());
                        reply.onCompleted();
                    }
                }).build().start();
        String target = "dns:///127.0.0.1:" + worker.getPort();
        var game = BattleRpcTransport.channel(target, Map.of("SLG_RPC_TLS_CA", serviceCa,
                "SLG_RPC_CLIENT_TLS_CERT", gameCert, "SLG_RPC_CLIENT_TLS_KEY", gameKey));
        var anotherService = BattleRpcTransport.channel(target, Map.of("SLG_RPC_TLS_CA", serviceCa,
                "SLG_RPC_CLIENT_TLS_CERT", serviceCert, "SLG_RPC_CLIENT_TLS_KEY", serviceKey));
        try {
            assertEquals("test", compute(game).getBattleId());
            assertThrows(StatusRuntimeException.class, () -> compute(anotherService));
        } finally {
            game.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            anotherService.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            worker.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static BattleComputeReply compute(ManagedChannel channel) {
        return BattleWorkerRpcGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                .compute(BattleComputeRequest.newBuilder().setBattleId("test").build());
    }

    private static String fixture(String file) throws Exception {
        return Path.of(BattleRpcTransportTest.class.getResource("/tls/" + file).toURI()).toString();
    }
}

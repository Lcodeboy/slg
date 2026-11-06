package com.slg.rpc;

import com.google.protobuf.ByteString;
import com.slg.rpc.v1.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RpcTransportTest {
    @Test void mutualTlsRequiresClientCertificateAndPreservesPeerVerification() throws Exception {
        String ca = fixture("ca.pem"), cert = fixture("peer.pem"), key = fixture("peer-key.pem");
        var server = RpcTransport.serverBuilder(new InetSocketAddress("127.0.0.1", 0), Map.of(
                "SLG_RPC_TLS_CA", ca, "SLG_RPC_TLS_CERT", cert, "SLG_RPC_TLS_KEY", key))
                .addService(new GameRpcGrpc.GameRpcImplBase() {
                    @Override public void play(PlayRequest request, StreamObserver<PlayReply> observer) {
                        observer.onNext(PlayReply.newBuilder().setRequestId(request.getRequestId())
                                .setMsgId(request.getMsgId()).setStatus(200).setBody(ByteString.EMPTY).build());
                        observer.onCompleted();
                    }
                }).build().start();
        var trusted = RpcTransport.channel("127.0.0.1:" + server.getPort(), Map.of("SLG_RPC_TLS_CA", ca,
                "SLG_RPC_CLIENT_TLS_CERT", cert, "SLG_RPC_CLIENT_TLS_KEY", key));
        var missingCertificate = RpcTransport.channel("127.0.0.1:" + server.getPort(), Map.of("SLG_RPC_TLS_CA", ca));
        try {
            var request = PlayRequest.newBuilder().setRequestId("tls-test").build();
            assertTrue(GameRpcGrpc.newBlockingStub(trusted).withDeadlineAfter(5, TimeUnit.SECONDS).play(request).hasBody());
            assertThrows(StatusRuntimeException.class, () -> GameRpcGrpc.newBlockingStub(missingCertificate)
                    .withDeadlineAfter(2, TimeUnit.SECONDS).play(request));
        } finally {
            trusted.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            missingCertificate.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
    @Test void explicitTlsCannotSilentlyStartPlaintext() {
        assertThrows(IllegalArgumentException.class, () -> RpcTransport.serverBuilder(
                new InetSocketAddress("127.0.0.1", 0), Map.of("SLG_RPC_TLS_ENABLED", "true")));
        assertThrows(IllegalArgumentException.class, () -> RpcTransport.channel("localhost:1",
                Map.of("SLG_RPC_CLIENT_TLS_CERT", "unpaired.pem")));
    }
    private static String fixture(String name) throws Exception {
        return Path.of(RpcTransportTest.class.getResource("/tls/" + name).toURI()).toString();
    }
}

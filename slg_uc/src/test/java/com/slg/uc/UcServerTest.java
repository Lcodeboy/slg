package com.slg.uc;

import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.auth.TokenService;
import com.slg.core.protocol.Json;
import com.slg.rpc.*;
import com.slg.rpc.v1.*;
import com.slg.test.MySqlTestDatabase;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.sql.*;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实 gRPC、独立 MySQL 和 HTTP 健康探针；断言不打印密码、令牌或认证响应全文。 */
@Timeout(30)
class UcServerTest {
    private static final Clock CLOCK=Clock.systemUTC();
    private static final TokenService TOKENS=new TokenService("uc-test-only-secret-at-least-32-bytes-long",CLOCK);
    @BeforeAll static void prepareDatabase() {MySqlTestDatabase.user();}

    @Test void registrationSurvivesRestartAndCredentialsStayHashed() throws Exception {
        String jdbc=MySqlTestDatabase.createUrl(),accountId;
        try(Harness server=new Harness(jdbc,UcServer.Settings.defaults())) {
            var registered=server.stub().register(auth("Alice_1","a-test-password"));
            assertEquals(201,registered.getStatus());assertTrue(registered.hasSession());
            var session = registered.getSession();
            accountId = session.getAccountId();
            assertEquals(accountId,TOKENS.verify(session.getToken()).accountId());
            assertError(409,"USERNAME_TAKEN",server.stub().register(auth("alice_1","other-password")));
            assertError(401,"INVALID_CREDENTIALS",server.stub().login(auth("alice_1","wrong-password")));
            var denied = server.stub().listRealms(EmptyRequest.getDefaultInstance());
            assertEquals(401, denied.getStatus());
            assertEquals("UNAUTHORIZED", denied.getError().getCode());
            assertFalse(denied.hasDirectory());
            Metadata metadata=new Metadata();metadata.put(RpcMetadata.AUTHORIZATION,"Bearer "+session.getToken());
            var realms=server.stub().withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)).listRealms(EmptyRequest.getDefaultInstance());
            assertTrue(realms.hasDirectory());
            assertEquals(1, realms.getDirectory().getRealms(0).getRealmId());
            assertEquals(404,server.http("/api/auth/register").statusCode());
            assertEquals(404,server.http("/api/auth/login").statusCode());
            assertEquals(404,server.http("/api/realms").statusCode());
            assertEquals("grpc",server.health().path("transport").asText());
            assertEquals(server.server.rpcPort(),server.health().path("rpcPort").asInt());
        }
        try(Connection connection=connect(jdbc);var query=connection.createStatement();
            var result=query.executeQuery("SELECT password_salt,password_hash,iterations FROM uc_account")) {
            assertTrue(result.next());assertEquals(24,result.getBytes(1).length);assertEquals(32,result.getBytes(2).length);
            assertEquals(PasswordHasher.ITERATIONS,result.getInt(3));assertFalse(result.next());
        }
        try(Harness restarted=new Harness(jdbc,UcServer.Settings.defaults())) {
            var login=restarted.stub().login(auth("alice_1","a-test-password"));
            assertEquals(200,login.getStatus());assertTrue(login.hasSession());
            assertEquals(accountId,login.getSession().getAccountId());
        }
    }

    @Test void invalidOversizedAndExpiredRequestsDoNotCreateAccounts() throws Exception {
        try(Harness server=new Harness(MySqlTestDatabase.createUrl(),UcServer.Settings.defaults())) {
            assertError(400,"INVALID_REQUEST",server.stub().register(auth("x","short")));
            StatusRuntimeException expired=assertThrows(StatusRuntimeException.class,
                    ()->server.stub().withDeadlineAfter(-1,TimeUnit.MILLISECONDS).register(auth("expired_user","valid-password")));
            assertEquals(Status.Code.DEADLINE_EXCEEDED,expired.getStatus().getCode());
            StatusRuntimeException oversized=assertThrows(StatusRuntimeException.class,
                    ()->server.stub().register(auth("large_user","x".repeat(70_000))));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED,oversized.getStatus().getCode());
            assertTrue(server.accounts.find("expired_user").isEmpty());assertTrue(server.accounts.find("large_user").isEmpty());
            assertTrue(server.stub().register(auth("valid_user","valid-password")).hasSession());
        }
    }

    @Test void fullAuthenticationQueueRejectsAndQueuedCancellationOrDeadlineNeverCreatesAnAccount() throws Exception {
        String jdbc=MySqlTestDatabase.createUrl();
        try(Harness server=new Harness(jdbc,new UcServer.Settings(1,1,1,5000));RegistrationLock held=new RegistrationLock(jdbc)) {
            var first=server.future().register(auth("held_user","valid-password"));
            await(()->server.health().path("authentication").path("active").asInt()==1);
            var queued=server.future().register(auth("cancelled_user","valid-password"));
            await(()->server.health().path("authentication").path("queued").asInt()==1);
            assertError(503,"BUSY",server.stub().register(auth("rejected_user","valid-password")));
            assertEquals("UP",server.health().path("status").asText());
            // gRPC GrpcFuture 仅 cancel(true) 才会取消底层调用；服务端仍以 cancel(false) 保留运行中的事务。
            assertTrue(queued.cancel(true));
            await(()->server.health().path("authentication").path("queued").asInt()==0);
            var expired=server.future().withDeadlineAfter(150,TimeUnit.MILLISECONDS).register(auth("expired_user","valid-password"));
            ExecutionException failure=assertThrows(ExecutionException.class,()->expired.get(2,TimeUnit.SECONDS));
            assertEquals(Status.Code.DEADLINE_EXCEEDED,Status.fromThrowable(failure.getCause()).getCode());
            await(()->server.health().path("authentication").path("queued").asInt()==0);
            held.release();assertError(409,"USERNAME_TAKEN",first.get(3,TimeUnit.SECONDS));
            assertTrue(server.accounts.find("cancelled_user").isEmpty());
            assertTrue(server.accounts.find("expired_user").isEmpty());
            assertTrue(server.accounts.find("rejected_user").isEmpty());
        }
    }

    private static AuthRequest auth(String username,String password) {return AuthRequest.newBuilder().setUsername(username).setPassword(password).build();}
    private static void assertError(int status,String code,AuthReply result) {
        assertEquals(status,result.getStatus());assertFalse(result.hasSession());assertEquals(code,result.getError().getCode());
    }
    private static Connection connect(String jdbc) throws SQLException {return DriverManager.getConnection(jdbc,MySqlTestDatabase.user(),MySqlTestDatabase.password());}
    private static void await(Callable<Boolean> condition) throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
        while(System.nanoTime()<end) {if(condition.call()) return;Thread.sleep(10);}
        fail("Expected authentication queue state before timeout");
    }
    private static final class RegistrationLock implements AutoCloseable {
        private final Connection connection;
        RegistrationLock(String jdbc) throws Exception {
            connection=connect(jdbc);connection.setAutoCommit(false);
            try(var insert=connection.prepareStatement(
                    "INSERT INTO uc_account(username,password_salt,password_hash,iterations,created_at) VALUES(?,?,?,?,?)")) {
                insert.setString(1,"held_user");insert.setBytes(2,new byte[24]);insert.setBytes(3,new byte[32]);
                insert.setInt(4,PasswordHasher.ITERATIONS);insert.setLong(5,CLOCK.instant().getEpochSecond());insert.executeUpdate();
            }
        }
        void release() throws SQLException {connection.commit();}
        @Override public void close() throws SQLException {try {connection.rollback();} finally {connection.close();}}
    }
    private static final class Harness implements AutoCloseable {
        final AccountRepository accounts;
        final UcServer server;
        final ManagedChannel channel;
        final HttpClient client=HttpClient.newHttpClient();
        Harness(String jdbc,UcServer.Settings settings) throws Exception {
            accounts=new AccountRepository(jdbc,MySqlTestDatabase.user(),MySqlTestDatabase.password(),CLOCK);
            server=new UcServer(new InetSocketAddress("127.0.0.1",0),new InetSocketAddress("127.0.0.1",0),accounts,TOKENS,settings);
            server.start();channel=RpcTransport.channel("127.0.0.1:"+server.rpcPort());
        }
        AccountRpcGrpc.AccountRpcBlockingStub stub() {return AccountRpcGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS);}
        AccountRpcGrpc.AccountRpcFutureStub future() {return AccountRpcGrpc.newFutureStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS);}
        HttpResponse<String> http(String path) throws Exception {
            return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+server.port()+path))
                    .timeout(Duration.ofSeconds(2)).GET().build(),HttpResponse.BodyHandlers.ofString());
        }
        JsonNode health() throws Exception {return Json.read(http("/health").body(),JsonNode.class);}
        @Override public void close() throws Exception {channel.shutdownNow().awaitTermination(5,TimeUnit.SECONDS);client.close();server.close();}
    }
}

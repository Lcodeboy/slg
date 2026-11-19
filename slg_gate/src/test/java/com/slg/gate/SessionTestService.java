package com.slg.gate;

import com.slg.core.auth.TokenService;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.v1.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.util.*;

/**
 * 网关传输测试的控制协议对端。它实现 Prepare/Open 的 CAS 和精确关闭，用于验证 Gateway
 * 确实等待远端绑定；不替代 GAME 的账号邮箱、到期租约和持久业务集成测试。
 */
final class SessionTestService extends SessionRpcGrpc.SessionRpcImplBase {
    static final SessionKey KEY = SessionKey.newBuilder().setAccountId("101")
            .setGameInstance("5d484e68-7a7c-44f8-ad0d-78dcb1411d00").setGeneration(1)
            .setBindingId("77cf9d23-713c-4fc5-a62a-26fce7b97618").build();
    private final String instance = UUID.randomUUID().toString();
    private final TokenService tokens;
    private final Map<String, SessionKey> sessions = new HashMap<>();
    private long generation;
    SessionTestService(TokenService tokens) { this.tokens = tokens; }

    @Override public synchronized void prepare(SessionPrepareRequest request, StreamObserver<SessionPreparation> output) {
        String account = tokens.verify(RpcMetadata.TOKEN.get()).accountId();
        var key = sessions.get(account);
        reply(output, SessionPreparation.newBuilder().setGameInstance(instance)
                .setObservedGeneration(key == null ? 0 : key.getGeneration()).setTicketId(UUID.randomUUID().toString()).build());
    }
    @Override public synchronized void open(SessionOpenRequest request, StreamObserver<SessionBinding> output) {
        var claims = tokens.verify(RpcMetadata.TOKEN.get());
        var previous = sessions.get(claims.accountId());
        if (!instance.equals(request.getGameInstance())
                || (previous == null ? 0 : previous.getGeneration()) != request.getExpectedGeneration()) {
            output.onError(Status.FAILED_PRECONDITION.asRuntimeException()); return;
        }
        var key = SessionKey.newBuilder().setAccountId(claims.accountId()).setGameInstance(instance)
                .setGeneration(++generation).setBindingId(UUID.randomUUID().toString()).build();
        sessions.put(claims.accountId(), key);
        reply(output, SessionBinding.newBuilder().setKey(key).setExpiresAt(claims.expiresAt()).setLeaseMillis(30_000).build());
    }
    @Override public synchronized void check(SessionCheckRequest request, StreamObserver<SessionCheckReply> output) {
        var result = SessionCheckReply.newBuilder().setLeaseMillis(30_000);
        request.getBindingsList().stream().filter(key -> key.equals(sessions.get(key.getAccountId()))).forEach(result::addCurrent);
        reply(output, result.build());
    }
    @Override public synchronized void close(SessionCloseRequest request, StreamObserver<SessionCloseReply> output) {
        sessions.remove(request.getBinding().getAccountId(), request.getBinding());
        reply(output, SessionCloseReply.getDefaultInstance());
    }
    private static <T> void reply(StreamObserver<T> observer, T value) { observer.onNext(value); observer.onCompleted(); }

    /** 无网络的 ClientCall 适配仍走完整控制消息及真实 metadata，不给生产类添加免绑定路径。 */
    Channel channel() {
        return new Channel() {
            @Override public String authority() { return "session-test"; }
            @Override public <Q, R> ClientCall<Q, R> newCall(MethodDescriptor<Q, R> method, CallOptions options) {
                return new ClientCall<>() {
                    Listener<R> listener;
                    Metadata headers;
                    volatile boolean cancelled;
                    @Override public void start(Listener<R> listener, Metadata headers) { this.listener = listener; this.headers = headers; }
                    @Override public void request(int count) { }
                    @Override public void cancel(String reason, Throwable error) { cancelled = true; }
                    @Override public void halfClose() { }
                    @Override public void sendMessage(Q input) {
                        options.getExecutor().execute(() -> {
                            if (cancelled) return;
                            String authorization = headers.get(RpcMetadata.AUTHORIZATION);
                            Context context = Context.current().withValue(RpcMetadata.TOKEN,
                                    authorization == null ? null : authorization.substring(7));
                            context.run(() -> {
                                StreamObserver<Object> out = new StreamObserver<>() {
                                    @SuppressWarnings("unchecked") public void onNext(Object value) { if (!cancelled) listener.onMessage((R) value); }
                                    public void onError(Throwable error) { if (!cancelled) listener.onClose(Status.fromThrowable(error), new Metadata()); }
                                    public void onCompleted() { if (!cancelled) listener.onClose(Status.OK, new Metadata()); }
                                };
                                try {
                                    if (input instanceof SessionPrepareRequest req) prepare(req, cast(out));
                                    else if (input instanceof SessionOpenRequest req) open(req, cast(out));
                                    else if (input instanceof SessionCheckRequest req) check(req, cast(out));
                                    else if (input instanceof SessionCloseRequest req) close(req, cast(out));
                                    else out.onError(Status.UNIMPLEMENTED.asRuntimeException());
                                } catch (RuntimeException error) { out.onError(error); }
                            });
                        });
                    }
                };
            }
        };
    }
    @SuppressWarnings("unchecked") private static <T> StreamObserver<T> cast(StreamObserver<?> value) {
        return (StreamObserver<T>) value;
    }
}

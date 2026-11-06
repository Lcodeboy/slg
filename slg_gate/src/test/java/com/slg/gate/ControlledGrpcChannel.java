package com.slg.gate;

import com.slg.rpc.RpcMetadata;
import io.grpc.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** 可控制 gRPC ClientCall 的夹具：观察 metadata、原始取消及 deadline，不使用 HTTP 替身。 */
final class ControlledGrpcChannel extends Channel {
    final BlockingQueue<Pending> requests = new LinkedBlockingQueue<>();
    final AtomicInteger count = new AtomicInteger();
    Pending next() throws InterruptedException {
        Pending value = requests.poll(3, TimeUnit.SECONDS);
        if (value == null) throw new AssertionError("Expected upstream gRPC request");
        return value;
    }
    @Override public String authority() { return "controlled-rpc"; }
    @Override public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> method, CallOptions options) {
        return new ClientCall<>() {
            private Listener<RespT> listener;
            private Metadata headers;
            private final CompletableFuture<Void> completion = new CompletableFuture<>();
            @Override public void start(Listener<RespT> listener, Metadata headers) { this.listener = listener; this.headers = headers; }
            @Override public void request(int count) { }
            @Override public void cancel(String message, Throwable cause) { completion.cancel(false); }
            @Override public void halfClose() { }
            @Override public void sendMessage(ReqT request) {
                @SuppressWarnings("unchecked") Listener<Object> typed = (Listener<Object>) listener;
                Pending pending = new Pending(request, headers, options, typed, completion);
                count.incrementAndGet(); requests.add(pending);
            }
        };
    }
    record Pending(Object request, Metadata headers, CallOptions options, ClientCall.Listener<Object> listener,
                   CompletableFuture<Void> future) {
        String authorization() { return headers.get(RpcMetadata.AUTHORIZATION); }
        void reply(Object result) throws Exception {
            dispatch(() -> { listener.onMessage(result); listener.onClose(Status.OK, new Metadata()); future.complete(null); });
        }
        void fail(Status status) throws Exception {
            dispatch(() -> { listener.onClose(status, new Metadata()); future.completeExceptionally(status.asRuntimeException()); });
        }
        private void dispatch(Runnable work) throws Exception {
            CompletableFuture<Void> delivered = new CompletableFuture<>();
            options.getExecutor().execute(() -> {
                try { work.run(); delivered.complete(null); }
                catch (Throwable failure) { delivered.completeExceptionally(failure); }
            });
            delivered.get(3, TimeUnit.SECONDS);
        }
    }
}

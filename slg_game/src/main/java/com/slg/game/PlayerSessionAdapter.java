package com.slg.game;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slg.core.auth.AccountIds;
import com.slg.core.auth.SessionClaims;
import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.execution.OperationValue;
import com.slg.game.application.execution.QualifiedQueryHandler;
import com.slg.game.application.player.PlayerAccess;
import com.slg.rpc.v1.SessionBinding;
import com.slg.rpc.v1.SessionKey;
import com.slg.rpc.v1.SessionOpenRequest;
import io.grpc.Status;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * 会话控制与玩法共享 PlayerApplication 的账号资格。handler 在装配时注册一次；队列只保存
 * 有界不可变 bytes，不保存网络 Context、Runnable 或每请求回调。Open 的完成才解除旧连接围栏，
 * 已开始写事务必须先收敛并发布，不能因网络取消而提前让替换连接进入。
 */
final class PlayerSessionAdapter {
    static final String OPEN = "session.open.v1";
    private static final int MAX_REQUEST_BYTES = 480;
    private static final int MAX_PAYLOAD_BYTES = 1 + 8 * 3 + 4 + MAX_REQUEST_BYTES;
    private static final int MAX_RESULT_BYTES = 512;
    private PlayerSessionAdapter() { }

    static Map<String, QualifiedQueryHandler> controlHandlers(SessionDirectory sessions, BooleanSupplier ready) {
        Objects.requireNonNull(sessions, "sessions"); Objects.requireNonNull(ready, "ready");
        return Map.of(OPEN, new QualifiedQueryHandler() {
            @Override public CompletionStage<OperationValue> execute(OperationCommand command) {
                if (!ready.getAsBoolean()) throw Status.UNAVAILABLE.withDescription("Realm not ready").asRuntimeException();
                var input = decode(command.payload());
                long account = Long.parseLong(input.claims().accountId());
                if (!command.type().equals(OPEN) || !command.scope().accounts().equals(Set.of(account))
                        || !command.scope().request().scope().equals("session:" + account)
                        || !command.scope().request().requestId().equals(input.request().getAttemptId()))
                    throw Status.INVALID_ARGUMENT.withDescription("Session input differs from qualified account").asRuntimeException();
                // Directory.open 使用同一目录时钟再次核验凭证到期、票据和 CAS；入队时验签不替代此围栏。
                return CompletableFuture.completedFuture(new OperationValue(sessions.open(input.claims(), input.request()).toByteArray()));
            }
            @Override public int maxResultBytes() { return MAX_RESULT_BYTES; }
        });
    }

    static Predicate<PlayerAccess> access(SessionDirectory sessions, BooleanSupplier ready) {
        Objects.requireNonNull(sessions, "sessions"); Objects.requireNonNull(ready, "ready");
        return actor -> ready.getAsBoolean() && sessions.isCurrent(
                new SessionClaims(Long.toString(actor.accountId()), actor.issuedAt(), actor.expiresAt()),
                SessionKey.newBuilder().setAccountId(Long.toString(actor.accountId())).setGameInstance(actor.gameInstance())
                        .setGeneration(actor.generation()).setBindingId(actor.bindingId()).build());
    }

    static PlayerAccess access(SessionClaims claims, SessionKey key) {
        return new PlayerAccess(Long.parseLong(claims.accountId()), claims.issuedAt(), claims.expiresAt(),
                key.getGameInstance(), key.getGeneration(), key.getBindingId());
    }

    static byte[] payload(SessionClaims claims, SessionOpenRequest request) {
        if (!AccountIds.isValid(claims.accountId()) || request.getSerializedSize() > MAX_REQUEST_BYTES)
            throw Status.INVALID_ARGUMENT.withDescription("Invalid session control input").asRuntimeException();
        byte[] bytes = request.toByteArray();
        return ByteBuffer.allocate(1 + 8 * 3 + 4 + bytes.length).put((byte) 1).putLong(Long.parseLong(claims.accountId()))
                .putLong(claims.issuedAt()).putLong(claims.expiresAt()).putInt(bytes.length).put(bytes).array();
    }

    static SessionBinding binding(OperationValue value) {
        if (value.size() > MAX_RESULT_BYTES) throw new IllegalArgumentException("Session result exceeds limit");
        try { return SessionBinding.parseFrom(value.bytes()); }
        catch (InvalidProtocolBufferException malformed) { throw new IllegalArgumentException("Invalid session control result"); }
    }

    private static OpenInput decode(byte[] payload) {
        try {
            if (payload.length < 29 || payload.length > MAX_PAYLOAD_BYTES) throw new IllegalArgumentException();
            var input = ByteBuffer.wrap(payload);
            if (input.get() != 1) throw new IllegalArgumentException();
            long account = input.getLong(), issued = input.getLong(), expires = input.getLong();
            int length = input.getInt();
            if (account <= 0 || issued < 0 || expires <= issued || length != input.remaining()) throw new IllegalArgumentException();
            var bytes = new byte[length]; input.get(bytes);
            return new OpenInput(new SessionClaims(Long.toString(account), issued, expires), SessionOpenRequest.parseFrom(bytes));
        } catch (RuntimeException | InvalidProtocolBufferException malformed) {
            throw Status.INVALID_ARGUMENT.withDescription("Invalid session control input").asRuntimeException();
        }
    }
    private record OpenInput(SessionClaims claims, SessionOpenRequest request) { }
}

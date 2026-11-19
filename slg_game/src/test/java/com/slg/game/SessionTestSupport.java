package com.slg.game;

import com.slg.core.auth.TokenService;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.v1.SessionOpenRequest;
import io.grpc.Context;
import java.time.Duration;
import java.util.UUID;

/** 无并发的单元夹具直接登记绑定；网关/恢复验收必须经真实 SessionRpc Prepare/Open。 */
final class SessionTestSupport {
    private SessionTestSupport() { }
    static Context context(SessionDirectory sessions, TokenService tokens, String account) {
        String token = tokens.issue(account, Duration.ofHours(1));
        var claims = tokens.verify(token);
        var prepared = sessions.prepare(claims);
        var binding = sessions.open(claims, SessionOpenRequest.newBuilder().setGameInstance(prepared.getGameInstance())
                .setExpectedGeneration(prepared.getObservedGeneration()).setTicketId(prepared.getTicketId())
                .setAttemptId(UUID.randomUUID().toString()).build());
        return Context.current().withValues(RpcMetadata.TOKEN, token, RpcMetadata.SESSION, binding.getKey());
    }
}

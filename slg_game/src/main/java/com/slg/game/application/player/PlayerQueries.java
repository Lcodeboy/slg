package com.slg.game.application.player;

import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.execution.OperationValue;
import com.slg.game.application.execution.QualifiedQueryHandler;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.persistence.RequestKey;
import com.slg.game.domain.player.PlayerDetails;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/** 玩家纯读用例；共享账号资格，结果在准入前预留上限，不生成持久回执或空业务事务。 */
final class PlayerQueries {
    private PlayerQueries() { }

    static Map<String, QualifiedQueryHandler> handlers(PlayerOperationContext context) {
        var codec = context.codec();
        return Map.of(PlayerApplication.GET, query(command -> {
            var input = input(context, command);
            return context.directory().load(input.access().accountId()).thenApply(snapshot -> {
                context.requireAccess(input.access());
                return new OperationValue(codec.encodeDetails(PlayerDetails.from(snapshot)));
            });
        }, codec.maxDetailsBytes()), PlayerApplication.RECEIPT, query(command -> {
            var input = input(context, command);
            PlayerApplication.requireRequestId(input.value());
            var key = new RequestKey(PlayerApplication.scope(input.access().accountId()), input.value());
            return context.observe(key).thenApply(observed -> {
                context.requireAccess(input.access());
                if (observed instanceof RealmReceiptReader.Observation.Found found)
                    context.validateReceipt(found.receipt(), key, input.access().accountId());
                return new OperationValue(codec.encodeObservation(observed));
            });
        }, codec.maxResultBytes()));
    }

    private static PlayerInvocation input(PlayerOperationContext context, OperationCommand command) {
        var input = context.codec().decodeInvocation(command.payload());
        context.validateInput(command, input.access());
        return input;
    }
    private static QualifiedQueryHandler query(Function<OperationCommand, CompletionStage<OperationValue>> action, int budget) {
        return new QualifiedQueryHandler() {
            @Override public CompletionStage<OperationValue> execute(OperationCommand command) { return action.apply(command); }
            @Override public int maxResultBytes() { return budget; }
        };
    }
}

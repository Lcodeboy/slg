package com.slg.game.application.player;

import com.slg.game.application.execution.OperationCommand;
import com.slg.game.domain.persistence.CommitReceipt;

/**
 * 在线玩家写入口模板：在共同账号生命周期上加入当前会话与玩家回执校验。
 * 保留 decode/access/digest/decide 四个用例扩展点，后台写入直接继承账号模板并提供系统身份协议。
 */
public abstract class AbstractPlayerWriteHandler<I> extends AbstractAccountWriteHandler<I> {
    protected AbstractPlayerWriteHandler(PlayerOperationContext context) { super(context); }
    protected abstract PlayerAccess access(I input);
    @Override protected final long accountId(I input) { return access(input).accountId(); }
    @Override protected final void validateInput(OperationCommand command, I input) { context.validateInput(command, access(input)); }
    @Override protected final void validateReplay(I input, OperationCommand command, CommitReceipt receipt) {
        context.validateReceipt(receipt, command.scope().request(), accountId(input));
    }
}

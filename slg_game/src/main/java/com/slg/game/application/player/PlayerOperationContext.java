package com.slg.game.application.player;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.execution.OperationCommand;
import com.slg.game.application.port.*;
import com.slg.game.application.read.BoundedReadExecutor;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * 玩家用例共享的生命周期依赖，不是可变的请求上下文。
 * 长期持有端口及目录，不保存当前玩家、连接或 SQL Session。缓存读写及决定操作要求调用方
 * 已取得账号资格；无 I/O 的 requireAccess 也供门面在准入前核验会话。
 */
public final class PlayerOperationContext {
    private final RealmIdentity realm;
    private final PlayerDirectory directory;
    private final RealmReceiptReader receipts;
    private final BoundedReadExecutor reads;
    private final ReadBudget budget;
    private final PlayerPayloadCodec codec;
    private final Predicate<PlayerAccess> access;

    public PlayerOperationContext(RealmIdentity realm, PlayerDirectory directory, RealmReceiptReader receipts,
                                  BoundedReadExecutor reads, ReadBudget budget, PlayerPayloadCodec codec,
                                  Predicate<PlayerAccess> access) {
        this.realm = Objects.requireNonNull(realm); this.directory = Objects.requireNonNull(directory);
        this.receipts = Objects.requireNonNull(receipts); this.reads = Objects.requireNonNull(reads);
        this.budget = Objects.requireNonNull(budget); this.codec = Objects.requireNonNull(codec);
        this.access = Objects.requireNonNull(access);
        // 每个写用例从准备线程同时启动玩家加载与回执读取，不在冷读通知内嵌套申请额度。
        if (reads.limits().maxOutstanding() < 2)
            throw new IllegalArgumentException("Player application requires at least two outstanding cold reads");
    }

    public RealmIdentity realm() { return realm; }
    public PlayerDirectory directory() { return directory; }
    public PlayerPayloadCodec codec() { return codec; }

    public void requireAccess(PlayerAccess actor) {
        if (!access.test(actor)) throw new GameException("SESSION_REPLACED", "Authenticate a new game connection");
    }

    /** 输入中的可信账号仍须与实际排队资格相同；不能通过替换 payload 借用另一账号的锁。 */
    public void validateInput(OperationCommand command, PlayerAccess actor) {
        if (!command.scope().request().scope().equals(PlayerApplication.scope(actor.accountId()))
                || !command.scope().accounts().contains(actor.accountId()))
            throw new IllegalArgumentException("Player input differs from qualified account");
        requireAccess(actor);
    }

    public CompletionStage<RealmReceiptReader.Observation> observe(RequestKey request) {
        return reads.readReceipt(receipts, realm, request, budget).result();
    }

    /** 回执内容还须属于所查账号；不能因 JSON 可解码就重放其他玩家资料或行军。 */
    public PlayerOutcome validateReceipt(CommitReceipt receipt, RequestKey expectedKey, long account) {
        if (!expectedKey.equals(receipt.request()) || !expectedKey.scope().equals(PlayerApplication.scope(account)))
            throw new IllegalArgumentException("Player receipt identity differs from requested identity");
        PlayerOutcome outcome = codec.decodeOutcome(receipt.resultJson());
        if (outcome.player() != null) {
            var profile = outcome.player().profile();
            if (profile.accountId() != account || profile.currentRealmId() != realm.realmId())
                throw new IllegalArgumentException("Player receipt outcome differs from requested identity");
        }
        // 行军本身不携带 currentRealmId；realm 由回执读取端口绑定，永久角色 ID 仅证明账号。
        if (outcome.march() != null && PlayerIdentity.accountOf(outcome.march().playerId()) != account)
            throw new IllegalArgumentException("March receipt outcome differs from requested account");
        return outcome;
    }

    public CompletionStage<Void> publish(long account, CommitDecision decision, CommitResult.Committed committed) {
        return directory.load(account).thenAccept(before -> {
            try { directory.publish(PlayerSnapshotPublisher.apply(before, decision, committed.receipt().sequence())); }
            catch (RuntimeException invalid) { directory.invalidate(account); throw invalid; }
        });
    }

    /** 已确认提交后只能修复可见状态；读取失败或水位倒退继续隔离，不改写原提交结果。 */
    public CompletionStage<Void> reload(long account, CommitResult.Committed committed) {
        directory.invalidate(account);
        return directory.load(account).thenAccept(current -> {
            if (current.highSequence() < committed.receipt().sequence()) {
                directory.invalidate(account);
                throw new IllegalStateException("Reload precedes confirmed player receipt");
            }
        });
    }

    public CompletionStage<Void> discard(long account, CommitResult.NotCommitted rejected) {
        if (rejected.reason() == CommitResult.Reason.VERSION_CONFLICT || rejected.reason() == CommitResult.Reason.OWNER_LOST
                || rejected.reason() == CommitResult.Reason.ROLLED_BACK) directory.invalidate(account);
        return CompletableFuture.completedFuture(null);
    }
}

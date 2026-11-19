package com.slg.game.application.march;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.player.*;
import com.slg.game.domain.GameException;
import com.slg.game.domain.march.*;
import com.slg.game.domain.persistence.*;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 行军入口门面：把已认证输入编码为命令，并在准入前声明完整范围。具体读库、纯裁决、提交和
 * 发布分别属于 reader、规则、公共生命周期；本类没有数据库句柄，也不新增每玩家执行器。
 */
public final class MarchApplication {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final PlayerApplication players;
    private final RealmIdentity realm;
    private final MarchPayloadCodec codec;

    public MarchApplication(PlayerApplication players, RealmIdentity realm, MarchPayloadCodec codec) {
        this.players = Objects.requireNonNull(players); this.realm = Objects.requireNonNull(realm);
        this.codec = Objects.requireNonNull(codec);
        if (players.limits().maxOperations() - players.limits().systemReservedOperations() < 2)
            throw new IllegalArgumentException("Recall handoff requires two client operation slots");
    }

    public PlayerCall<CommitResult> startMarch(PlayerAccess actor, String requestId, StartMarchOrder order, Duration wait) {
        String marchId = MarchIdentity.start(realm.realmId(), actor.accountId(), requestId);
        var input = new MarchInvocation(actor, Objects.requireNonNull(order), marchId, null);
        return players.executeWrite(actor, requestId, MarchModule.START, codec.encodeInvocation(input),
                Set.of(new ObjectKey.March(marchId), new ObjectKey.Task(MarchIdentity.arrival(marchId))), wait);
    }

    public PlayerCall<MarchState> getMarch(PlayerAccess actor, String requestId, String marchId, Duration wait) {
        requireMarchId(marchId);
        return players.executeQuery(actor, requestId, MarchModule.GET,
                codec.encodeInvocation(new MarchInvocation(actor, null, marchId, null)),
                Set.of(new ObjectKey.March(marchId)), wait, value -> codec.decodeMarch(value.bytes()));
    }

    public PlayerCall<CommitResult> recallMarch(PlayerAccess actor, String requestId, String marchId, Duration wait) {
        requireMarchId(marchId); Objects.requireNonNull(wait);
        if (wait.isNegative()) throw new IllegalArgumentException("Negative recall wait");
        long began = System.nanoTime();
        var planned = players.executeQuery(actor, requestId, MarchModule.PLAN_RECALL,
                codec.encodeInvocation(new MarchInvocation(actor, null, marchId, null)),
                Set.of(new ObjectKey.March(marchId)), wait, value -> codec.decodePlan(value.bytes()));
        // 规划释放第一阶段资格后再整体准入；等待预算共享，不持账号资格等待另一把矿点锁。
        return new PlannedWriteCall<>(planned, plan -> {
            Duration elapsed = Duration.ofNanos(Math.max(0L, System.nanoTime() - began));
            Duration remaining = elapsed.compareTo(wait) >= 0 ? Duration.ZERO : wait.minus(elapsed);
            return players.executeWrite(actor, requestId, MarchModule.RECALL,
                    codec.encodeInvocation(new MarchInvocation(actor, null, marchId, plan)), recallObjects(plan), remaining);
        });
    }

    /** 门面声明与 handler 入读前复核使用同一范围算法，后续增加依赖时不能只改其中一侧。 */
    static Set<ObjectKey> recallObjects(MarchPlan plan) {
        var keys = new LinkedHashSet<ObjectKey>();
        keys.add(new ObjectKey.March(plan.marchId())); keys.add(new ObjectKey.Battle(plan.marchId()));
        MarchIdentity.taskIds(plan.marchId()).forEach(id -> keys.add(new ObjectKey.Task(id)));
        if (plan.siteX() != null) keys.add(new ObjectKey.GatherSite(plan.siteX(), plan.siteY()));
        return Set.copyOf(keys);
    }

    static void requireMarchId(String id) {
        GameException.require(id != null && ID.matcher(id).matches(), "INVALID_ARGUMENT", "Invalid march ID");
    }
}

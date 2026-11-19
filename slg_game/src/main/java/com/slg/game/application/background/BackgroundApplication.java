package com.slg.game.application.background;

import com.slg.core.battle.BattleResult;
import com.slg.game.application.commit.*;
import com.slg.game.application.player.*;
import com.slg.game.application.port.RealmWorkReader;
import com.slg.game.domain.GameException;
import com.slg.game.domain.march.BattleReport;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerIdentity;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 后台用例门面：持久归属代替在线会话，范围固定后仍进入每区同一个玩家执行器。
 * 发现和远程计算由 pump 编排，本类只接受有界候选/结果，不扫描、不创建线程或数据库连接。
 */
public final class BackgroundApplication implements BackgroundWork {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private final PlayerApplication players;
    private final BackgroundCodec codec;
    private final PlayerPayloadCodec playerCodec;

    public BackgroundApplication(PlayerApplication players, RealmIdentity realm, BackgroundCodec codec, PlayerPayloadCodec playerCodec) {
        this.players = Objects.requireNonNull(players); Objects.requireNonNull(realm);
        this.codec = Objects.requireNonNull(codec); this.playerCodec = Objects.requireNonNull(playerCodec);
        // CLIENT 满载时规划通知与正式提交还需同时持有两个系统槽，否则后台会在交接处永久自拒绝。
        if (players.limits().systemReservedOperations() < 2)
            throw new IllegalArgumentException("Background handoff requires at least two reserved system operation slots");
    }

    @Override public PlayerCall<CommitResult> advance(RealmWorkReader.DueTask candidate, Duration wait) {
        var row = new VersionedState(candidate.version(), candidate.task());
        long account = PlayerIdentity.accountOf(candidate.task().playerId());
        var input = new BackgroundInvocation(account, row, null, null);
        long began = System.nanoTime();
        var plan = players.executeSystemQuery(account, codec.request(row), CommitRequest.Origin.DUE_TASK, BackgroundModule.PLAN,
                codec.encodeInvocation(input), Set.of(new ObjectKey.March(candidate.task().marchId())), wait,
                value -> codec.decodePlan(value.bytes()));
        return new PlannedWriteCall<>(plan, found -> {
            var prepared = new BackgroundInvocation(account, row, found, null);
            var elapsed = Duration.ofNanos(Math.max(0L, System.nanoTime() - began));
            var remaining = elapsed.compareTo(wait) >= 0 ? Duration.ZERO : wait.minus(elapsed);
            return players.executeSystemWrite(account, codec.request(row), CommitRequest.Origin.DUE_TASK, BackgroundModule.ADVANCE,
                    codec.encodeInvocation(prepared), BackgroundScopes.task(prepared), remaining);
        });
    }

    @Override public PlayerCall<Optional<BattleDispatch>> prepareBattle(RealmWorkReader.PendingBattle candidate, Duration wait) {
        var input = battle(candidate, null);
        return players.executeSystemQuery(input.accountId(), codec.request(input.candidate()), CommitRequest.Origin.BATTLE_RESULT,
                BackgroundModule.PREPARE_BATTLE, codec.encodeInvocation(input), BackgroundScopes.battle(input), wait,
                value -> codec.decodeDispatch(value.bytes()));
    }

    @Override public PlayerCall<CommitResult> settle(RealmWorkReader.PendingBattle candidate, BattleResult result, Duration wait) {
        var input = battle(candidate, Objects.requireNonNull(result));
        return players.executeSystemWrite(input.accountId(), codec.request(input.candidate()), CommitRequest.Origin.BATTLE_RESULT,
                BackgroundModule.SETTLE, codec.encodeInvocation(input), BackgroundScopes.battle(input), wait);
    }

    public PlayerCall<BattleReport> getBattle(PlayerAccess actor, String requestId, String battleId, Duration wait) {
        requireBattleId(battleId);
        return players.executeQuery(actor, requestId, BackgroundModule.GET_BATTLE,
                playerCodec.encodeInvocation(new PlayerInvocation(actor, battleId)), Set.of(new ObjectKey.Battle(battleId)), wait,
                value -> codec.decodeReport(value.bytes()));
    }

    private static BackgroundInvocation battle(RealmWorkReader.PendingBattle candidate, BattleResult result) {
        return new BackgroundInvocation(PlayerIdentity.accountOf(candidate.battle().playerId()),
                new VersionedState(candidate.version(), candidate.battle()), null, result);
    }
    static void requireBattleId(String id) {
        GameException.require(id != null && ID.matcher(id).matches(), "INVALID_ARGUMENT", "Invalid battle ID");
    }
}

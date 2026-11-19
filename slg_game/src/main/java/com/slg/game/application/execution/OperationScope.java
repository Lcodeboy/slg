package com.slg.game.application.execution;

import com.slg.game.application.commit.CommitRequest;
import com.slg.game.domain.player.PlayerIdentity;
import com.slg.game.domain.persistence.*;

import java.time.Duration;
import java.util.*;

/**
 * 在读取/裁决之前声明的完整执行范围，不能事后仅从 changes 推导。
 * 玩家资产/占用/行军集合共享稳定账号资格，建角前与迁服后的永久角色 ID 因而使用同一队列。
 * 已有行军或矿点旧属主无法从键推断，用例仍须显式声明这些玩家或账号。
 * 版本依赖不能证明集合没有新增成员；账号资格也不能替代行军/任务/矿点等具体对象资格。
 */
public record OperationScope(RequestKey request, Set<ObjectKey> objects, CommitRequest.Origin origin,
                             Duration waitBudget, Duration commitQueueBudget, Set<Long> accounts) {
    /** 兼容已有调用方式；可解析的角色键会自动加入其账号资格。 */
    public OperationScope(RequestKey request, Set<ObjectKey> objects, CommitRequest.Origin origin,
                          Duration waitBudget, Duration commitQueueBudget) {
        this(request, objects, origin, waitBudget, commitQueueBudget, Set.of());
    }

    public OperationScope {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(origin, "origin");
        Objects.requireNonNull(waitBudget, "waitBudget");
        Objects.requireNonNull(commitQueueBudget, "commitQueueBudget");
        if (waitBudget.isNegative() || commitQueueBudget.isNegative()) throw new IllegalArgumentException("Negative operation budget");
        var ordered = new TreeSet<ObjectKey>(ObjectKey.ORDER);
        ordered.addAll(Objects.requireNonNull(objects, "objects"));
        objects = Collections.unmodifiableSet(new LinkedHashSet<>(ordered));
        var qualifiedAccounts = new TreeSet<Long>();
        for (Long account : Objects.requireNonNull(accounts, "accounts")) {
            if (account == null || account <= 0) throw new IllegalArgumentException("Account qualification must be positive");
            qualifiedAccounts.add(account);
        }
        for (ObjectKey key : objects) {
            Long account = accountOf(owner(key));
            if (account != null) qualifiedAccounts.add(account);
        }
        accounts = Collections.unmodifiableSet(new LinkedHashSet<>(qualifiedAccounts));
        for (ObjectKey key : objects) {
            String player = owner(key);
            if (player != null && !objects.contains(new ObjectKey.Player(player)) && !accounts.contains(accountOf(player)))
                throw new IllegalArgumentException("Player qualification is required for owned objects");
        }
    }

    /** 读取后只校验范围，不扩锁；缺失资格必须在 SQL 前拒绝，不能带着半套锁重新裁决。 */
    void validate(CommitDecision decision) {
        if (!request.equals(decision.request())) throw new IllegalArgumentException("Decision request differs from qualified request");
        for (ObjectKey key : decision.readVersions().keySet()) require(key);
        for (ObjectChange change : decision.changes()) {
            require(change.key());
            String player = switch (change.afterState()) {
                case PlayerState state -> state.playerId();
                case ResourceState state -> state.playerId();
                case TroopState state -> state.playerId();
                case HeroState state -> state.playerId();
                case MarchState state -> state.playerId();
                case GatherSiteState state -> state.occupantPlayerId();
                case TaskState state -> state.playerId();
                case BattleState state -> state.playerId();
            };
            if (player != null) require(new ObjectKey.Player(player));
        }
    }

    void validateReceipt(CommitDecision decision, CommitReceipt receipt) {
        if (!request.equals(receipt.request()) || !decision.digest().equals(receipt.digest()))
            throw new IllegalArgumentException("Receipt does not match qualified request and prepared digest");
    }

    private void require(ObjectKey key) {
        String player = owner(key);
        // 账号覆盖该玩家全部直接成员及集合读；其他对象仍必须预先逐项声明，不能读取后扩锁。
        boolean accountQualified = player != null && accounts.contains(accountOf(player));
        if (!objects.contains(key) && !accountQualified)
            throw new IllegalArgumentException("Decision is outside qualified object scope");
        if (player != null && !objects.contains(new ObjectKey.Player(player)) && !accountQualified)
            throw new IllegalArgumentException("Decision is outside qualified player scope");
    }
    private static Long accountOf(String player) {
        if (player == null) return null;
        try { return PlayerIdentity.accountOf(player); }
        catch (IllegalArgumentException malformedLegacyKey) { return null; }
    }
    private static String owner(ObjectKey key) {
        return switch (key) {
            case ObjectKey.Player value -> value.playerId();
            case ObjectKey.Resource value -> value.playerId();
            case ObjectKey.Troop value -> value.playerId();
            case ObjectKey.Hero value -> value.playerId();
            default -> null;
        };
    }

    /** 对实际字段计量的有界编码预算，不接受调用者自报 retainedBytes，也不声称精确 JVM 堆大小。 */
    long metadataBytes() {
        long bytes = 96L + request.scope().length() + request.requestId().length() + 8L * accounts.size();
        for (ObjectKey key : objects) bytes += 32L + switch (key) {
            case ObjectKey.Player value -> value.playerId().length();
            case ObjectKey.Resource value -> value.playerId().length() + 4;
            case ObjectKey.Troop value -> value.playerId().length() + 4;
            case ObjectKey.Hero value -> value.playerId().length() + value.heroId().length();
            case ObjectKey.March value -> value.marchId().length();
            case ObjectKey.GatherSite value -> 8;
            case ObjectKey.Task value -> value.taskId().length();
            case ObjectKey.Battle value -> value.battleId().length();
        };
        return bytes;
    }
}

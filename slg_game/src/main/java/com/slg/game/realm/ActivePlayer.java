package com.slg.game.realm;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 单个账号在本区的已提交状态。邮箱独占业务访问；失效标志可由调度器在任务间检查。
 * 版本 0 表示已加载但尚未建角，后续建角与读取仍在同一邮箱排队。
 */
public final class ActivePlayer {
    record Hero(String heroId, String assignedMarch) {}

    record Player(String playerId, String accountId, int foundingRealmId, int currentRealmId,
                  String name, Map<String, Long> troops,
                  List<Hero> heroes, List<String> activeMarchIds, Map<String,Long> resources,
                  Map<String, Long> wounded, Map<String, Long> dead) {
        Player {
            // 身份随完整快照落卷；迁服可以独立更新 currentRealmId，历史行军、战报仍引用原 playerId。
            if (currentRealmId <= 0 || !PlayerIdentity.atCreation(foundingRealmId, accountId).equals(playerId))
                throw new IllegalArgumentException("Player identity does not match its founding realm and account");
            troops = Map.copyOf(troops);
            heroes = List.copyOf(heroes);
            activeMarchIds = List.copyOf(activeMarchIds);
            resources = Map.copyOf(resources);
            wounded = Map.copyOf(wounded);
            dead = Map.copyOf(dead);
        }

        /** 派兵、战损和返城只更新玩法数据，统一沿用本次加载的归属、建角区和当前区。 */
        Player withArmy(Map<String,Long> troops, List<Hero> heroes, List<String> marches, Map<String,Long> resources,
                        Map<String,Long> wounded, Map<String,Long> dead) {
            return new Player(playerId, accountId, foundingRealmId, currentRealmId, name, troops, heroes, marches,
                    resources, wounded, dead);
        }
    }

    /** 以一个引用发布数据和版本，读取者不会看到新的兵力配旧版本。 */
    record Snapshot(long version, Player player) {}

    private final String accountId;
    private final String playerId;
    private final int realmId;
    private Snapshot snapshot;
    private long observedSequence;
    private volatile boolean valid = true;

    ActivePlayer(String accountId, int realmId) {
        this(accountId, realmId, null, 0, 0);
    }

    ActivePlayer(String accountId, int realmId, Player player, long version, long observedSequence) {
        this.accountId = Objects.requireNonNull(accountId);
        this.realmId = realmId;
        this.playerId = player == null ? PlayerIdentity.atCreation(realmId, accountId) : player.playerId();
        if (version < 0 || (version == 0) != (player == null)
                || (player != null && !owns(player))) {
            throw new IllegalArgumentException("Player snapshot identity or version mismatch");
        }
        this.snapshot = new Snapshot(version, player);
        this.observedSequence = observedSequence;
    }

    public String accountId() { return accountId; }
    /** 建角前用于首次请求去重；建角后取自账号目录所指向的持久角色，与当前区服号可以不同。 */
    public String playerId() { return playerId; }

    private boolean owns(Player player) {
        return realmId == player.currentRealmId() && accountId.equals(player.accountId()) && playerId.equals(player.playerId());
    }
    public boolean valid() { return valid; }
    public void invalidate() { valid = false; }

    Snapshot snapshot() { return snapshot; }

    /** 重放旧回执不能覆盖较新的快照。发现本对象尚未观察到的提交，交给下次激活重新读取。 */
    void replayed(long sequence) {
        if (sequence > observedSequence) invalidate();
    }

    /** 仅实际写入了本次决定的调用者才可以发布私有草案。 */
    void committed(long sequence, long expectedVersion, Player next) throws IOException {
        if (!valid || snapshot.version() != expectedVersion || sequence <= observedSequence
                || (next != null && !owns(next))) {
            invalidate();
            throw new IOException("Active player requires reload after commit");
        }
        if (next != null) snapshot = new Snapshot(expectedVersion + 1, next);
        observedSequence = sequence;
    }
}

package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/** 首版建角纯规则；不查库、不编码响应、不发布缓存，所有初始行最终必须与同一回执一起提交。 */
public final class PlayerCreation {
    public static final String STARTER_HERO = "sergey";
    static final Set<TroopType> STARTER_TROOPS = Set.of(TroopType.INFANTRY, TroopType.LANCER, TroopType.MARKSMAN);
    private PlayerCreation() {}

    /** 先声明完整资格所需键；不提前验证昵称或裁决业务，非法建角仍可在用例内形成原始业务回执。 */
    public static Set<ObjectKey> initialKeys(RealmIdentity identity, long accountId) {
        String playerId = PlayerIdentity.atCreation(Objects.requireNonNull(identity, "identity").realmId(), accountId);
        return Set.of(new ObjectKey.Player(playerId), new ObjectKey.Resource(playerId, ResourceType.WOOD),
                new ObjectKey.Troop(playerId, TroopType.INFANTRY), new ObjectKey.Troop(playerId, TroopType.LANCER),
                new ObjectKey.Troop(playerId, TroopType.MARKSMAN), new ObjectKey.Hero(playerId, STARTER_HERO));
    }

    /**
     * 与旧玩法保持相同经济默认值：仅 WOOD=0，三个兵种为 3000/1000/1000，伤亡均 0，sergey 空闲。
     * FOOD/GEMS 未获得前不创建行，不创建活动行军。返回的是未提交后像，不带伪造的已提交版本。
     */
    public static List<ObjectState> initialStates(RealmIdentity identity, long accountId, String requestedName) {
        Objects.requireNonNull(identity, "identity");
        String playerId = PlayerIdentity.atCreation(identity.realmId(), accountId);
        String name = normalizedName(requestedName);
        return List.of(new PlayerState(playerId, accountId, identity.realmId(), identity.realmId(), name),
                new ResourceState(playerId, ResourceType.WOOD, 0),
                new TroopState(playerId, TroopType.INFANTRY, 3000, 0, 0),
                new TroopState(playerId, TroopType.LANCER, 1000, 0, 0),
                new TroopState(playerId, TroopType.MARKSMAN, 1000, 0, 0),
                new HeroState(playerId, STARTER_HERO, null));
    }

    /**
     * 调用者须在账号/玩家及初始对象资格内读取 before；纯函数不能证明数据库仍然缺失。
     * 所有根都要求 expectedVersion=0，唯一账号索引与统一事务负责最终拒绝并发创建或已有孤行。
     */
    public static List<ObjectChange> create(PlayerSnapshot before, String requestedName) {
        Objects.requireNonNull(before, "before");
        String name = normalizedName(requestedName);
        GameException.require(before.profile().isEmpty(), "PLAYER_EXISTS", "Character already exists");
        return initialStates(before.identity(), before.accountId(), name).stream()
                .map(state -> new ObjectChange(state.key(), 0, state)).toList();
    }

    public static String normalizedName(String requestedName) {
        GameException.require(requestedName != null, "INVALID_ARGUMENT", "name is invalid");
        String name = requestedName.strip();
        // 保留既有 Java UTF-16 长度语义及 strip 顺序，不在迁入存储时改变昵称业务规则。
        GameException.require(!name.isEmpty() && name.length() <= 24 && name.codePoints().noneMatch(Character::isISOControl),
                "INVALID_ARGUMENT", "name is invalid");
        return name;
    }
}

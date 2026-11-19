package com.slg.game.domain.persistence;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.core.type.IntCodeEnum;

import java.util.Comparator;
import java.util.Objects;

/**
 * 区服内结构化对象键。复合分量分别保存和绑定 SQL 参数，不用分隔符拼接后再解析，
 * 因而玩家/英雄标识中出现标点也不会造成两个不同主键相撞。使用时另绑定 RealmIdentity。
 *
 * <p>ORDER 是事务和内存冲突调度共同采用的稳定全序：先按显式类型序，再按主键分量；
 * ASCII 标识采用区分大小写的二进制顺序，坐标与枚举 code 按数值顺序。枚举比较与数据库
 * 整数主键一致，SQL 适配器不得换成 locale、枚举名称或 ordinal 排序，以免锁序分歧。
 * 按此序锁定全部业务对象后才能获取区服 cursor。</p>
 */
public sealed interface ObjectKey extends Comparable<ObjectKey> permits ObjectKey.Player, ObjectKey.Resource,
        ObjectKey.Troop, ObjectKey.Hero, ObjectKey.March, ObjectKey.GatherSite, ObjectKey.Task, ObjectKey.Battle {
    Comparator<ObjectKey> ORDER = ObjectKey::compareTo;
    ObjectKind kind();

    @Override default int compareTo(ObjectKey other) {
        Objects.requireNonNull(other, "other");
        int kindOrder = Integer.compare(kind().lockOrder(), other.kind().lockOrder());
        if (kindOrder != 0) return kindOrder;
        return switch (this) {
            case Player key -> key.playerId().compareTo(((Player) other).playerId());
            case Resource key -> compare(key.playerId(), key.resourceType(),
                    ((Resource) other).playerId(), ((Resource) other).resourceType());
            case Troop key -> compare(key.playerId(), key.troopType(),
                    ((Troop) other).playerId(), ((Troop) other).troopType());
            case Hero key -> compare(key.playerId(), key.heroId(),
                    ((Hero) other).playerId(), ((Hero) other).heroId());
            case March key -> key.marchId().compareTo(((March) other).marchId());
            case GatherSite key -> {
                var site = (GatherSite) other;
                int xOrder = Integer.compare(key.x(), site.x());
                yield xOrder == 0 ? Integer.compare(key.y(), site.y()) : xOrder;
            }
            case Task key -> key.taskId().compareTo(((Task) other).taskId());
            case Battle key -> key.battleId().compareTo(((Battle) other).battleId());
        };
    }

    private static int compare(String leftFirst, String leftSecond, String rightFirst, String rightSecond) {
        int first = leftFirst.compareTo(rightFirst);
        return first == 0 ? leftSecond.compareTo(rightSecond) : first;
    }

    private static int compare(String leftFirst, IntCodeEnum leftSecond, String rightFirst, IntCodeEnum rightSecond) {
        int first = leftFirst.compareTo(rightFirst);
        return first == 0 ? Integer.compare(leftSecond.code(), rightSecond.code()) : first;
    }

    record Player(String playerId) implements ObjectKey {
        public Player { PersistenceChecks.identifier(playerId, "playerId", 256); }
        @Override public ObjectKind kind() { return ObjectKind.PLAYER; }
    }
    record Resource(String playerId, ResourceType resourceType) implements ObjectKey {
        public Resource {
            PersistenceChecks.identifier(playerId, "playerId", 256);
            Objects.requireNonNull(resourceType, "resourceType");
        }
        @Override public ObjectKind kind() { return ObjectKind.RESOURCE; }
    }
    record Troop(String playerId, TroopType troopType) implements ObjectKey {
        public Troop {
            PersistenceChecks.identifier(playerId, "playerId", 256);
            Objects.requireNonNull(troopType, "troopType");
        }
        @Override public ObjectKind kind() { return ObjectKind.TROOP; }
    }
    record Hero(String playerId, String heroId) implements ObjectKey {
        public Hero {
            PersistenceChecks.identifier(playerId, "playerId", 256);
            PersistenceChecks.identifier(heroId, "heroId", 256);
        }
        @Override public ObjectKind kind() { return ObjectKind.HERO; }
    }
    record March(String marchId) implements ObjectKey {
        public March { PersistenceChecks.identifier(marchId, "marchId", 256); }
        @Override public ObjectKind kind() { return ObjectKind.MARCH; }
    }
    record GatherSite(int x, int y) implements ObjectKey {
        public GatherSite {
            PersistenceChecks.nonNegative(x, "x");
            PersistenceChecks.nonNegative(y, "y");
        }
        @Override public ObjectKind kind() { return ObjectKind.GATHER_SITE; }
    }
    record Task(String taskId) implements ObjectKey {
        public Task { PersistenceChecks.identifier(taskId, "taskId", 256); }
        @Override public ObjectKind kind() { return ObjectKind.TASK; }
    }
    record Battle(String battleId) implements ObjectKey {
        public Battle { PersistenceChecks.identifier(battleId, "battleId", 256); }
        @Override public ObjectKind kind() { return ObjectKind.BATTLE; }
    }
}

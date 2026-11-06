package com.slg.game.realm;

import com.slg.game.domain.GameException;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopGroup;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static com.slg.game.domain.GameException.require;

/** 出征只按兵种选择数量。兵组的发送顺序不影响出征结果或幂等指纹。 */
final class StartMarchOrder {
    private final StartMarchReq request;

    StartMarchOrder(StartMarchReq request) { this.request = request; }

    /** 规范化后仍保留重复项、未知兵种及可选字段的 presence。非法请求也有自己的持久回执，
     * 不能把有重复兵种的错误请求与一个合法请求算成相同指纹。 */
    StartMarchReq canonicalRequest() {
        var groups = request.getTroopsList().stream()
                .sorted(Comparator.comparingInt(TroopGroup::getTypeValue).thenComparingLong(TroopGroup::getCount))
                .toList();
        return request.toBuilder().clearTroops().addAllTroops(groups).build();
    }

    int x() { return request.getTargetX(); }
    int y() { return request.getTargetY(); }

    void validateCoordinatesAndTier() {
        require(request.hasTargetX() && x() >= 0 && x() <= 1199,
                "INVALID_ARGUMENT", "targetX must be in 0..1199");
        require(request.hasTargetY() && y() >= 0 && y() <= 1199,
                "INVALID_ARGUMENT", "targetY must be in 0..1199");
        require(!request.hasTier() || request.getTier() == 1, "INVALID_ARGUMENT", "Only T1 is supported");
    }

    String mission() {
        if (!request.hasMission()) return "GATHER";
        return switch (request.getMission()) {
            case MARCH_MISSION_GATHER -> "GATHER";
            case MARCH_MISSION_ATTACK_NPC -> "ATTACK_NPC";
            default -> throw new GameException("INVALID_ARGUMENT", "Unknown march mission");
        };
    }

    Map<String, Long> troops() {
        var selected = new TreeMap<String, Long>();
        for (var group : request.getTroopsList()) {
            String type = switch (group.getType()) {
                case TROOP_TYPE_INFANTRY -> "INFANTRY";
                case TROOP_TYPE_LANCER -> "LANCER";
                case TROOP_TYPE_MARKSMAN -> "MARKSMAN";
                default -> throw new GameException("INVALID_ARGUMENT", "Unknown troop type");
            };
            require(group.getCount() >= 0 && group.getCount() <= 100_000, "INVALID_ARGUMENT", "Troop count must be in 0..100000");
            require(selected.putIfAbsent(type, group.getCount()) == null, "INVALID_ARGUMENT", "Duplicate troop type");
        }
        selected.values().removeIf(count -> count == 0);
        require(!selected.isEmpty(), "INVALID_ARGUMENT", "Select at least one troop");
        return selected;
    }

    /** 仅在玩家省略英雄选择时携带默认英雄；显式传空表示本队不占用任何英雄。 */
    List<String> heroes(String defaultHero) {
        if (!request.hasHeroes()) return List.of(defaultHero);
        var ids = request.getHeroes().getHeroIdsList();
        require(ids.size() <= 1, "INVALID_ARGUMENT", "Select at most one hero");
        require(ids.isEmpty() || defaultHero.equals(ids.getFirst()), "HERO_NOT_FOUND", "Hero not owned");
        return ids;
    }
}

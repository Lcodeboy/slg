package com.slg.game.domain.march;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * 出征的不可变原始业务输入；不依赖 Protobuf。可选字段的 null 表示未提供，英雄 null 表示
 * 使用默认英雄，空列表表示不带英雄。未知编码和重复兵种保留到领域裁决，不能在解码时
 * 丢弃后把非法请求变成合法请求，也不能让二者产生同一个幂等摘要。
 */
public record StartMarchOrder(Integer mission, Integer targetX, Integer targetY, Integer tier,
                              List<TroopSelection> troops, List<String> heroIds) {
    public StartMarchOrder {
        troops = List.copyOf(Objects.requireNonNull(troops, "troops"));
        heroIds = heroIds == null ? null : List.copyOf(heroIds);
    }

    public record TroopSelection(int type, long count) { }

    /** 发送顺序不影响语义；保留重复、零值和 presence，调用方按此规范输入生成版本化摘要。 */
    public StartMarchOrder canonical() {
        return new StartMarchOrder(mission, targetX, targetY, tier, troops.stream()
                .sorted(Comparator.comparingInt(TroopSelection::type).thenComparingLong(TroopSelection::count)).toList(), heroIds);
    }
}

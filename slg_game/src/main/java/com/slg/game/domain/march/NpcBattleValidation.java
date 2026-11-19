package com.slg.game.domain.march;

import com.slg.core.battle.*;
import com.slg.game.domain.persistence.BattleState;
import java.util.HashMap;

import static com.slg.game.domain.march.MarchRules.verify;

/** 对照固定输入验证 Worker 建议，不能只相信各组构造器的局部守恒或 Worker 自报胜负。 */
final class NpcBattleValidation {
    private NpcBattleValidation() { }

    static void result(BattleState battle, BattleInput input, BattleResult result) {
        verify(result != null && battle.battleId().equals(result.battleId()) && input.seed() == result.seed()
                        && battle.rulesVersion().equals(result.rulesVersion()) && result.rounds() > 0
                        && result.rounds() <= input.maxRounds(), "Invalid Worker report metadata");
        army(input.attacker(), result.attacker()); army(input.defender(), result.defender());
        boolean attackerLives = result.attacker().survivors() > 0, defenderLives = result.defender().survivors() > 0;
        Outcome expected = attackerLives == defenderLives ? Outcome.DRAW
                : attackerLives ? Outcome.ATTACKER_WIN : Outcome.DEFENDER_WIN;
        verify(result.outcome() == expected && (!attackerLives || !defenderLives || result.rounds() == input.maxRounds()),
                "Battle outcome or round limit contradicts survivors");
    }

    private static void army(Army input, ArmyResult result) {
        verify(input.ownerId().equals(result.ownerId()) && input.groups().size() == result.groups().size(),
                "Battle army or group count mismatch");
        var groups = new HashMap<String, TroopGroup>();
        input.groups().forEach(group -> groups.put(group.groupId(), group));
        for (var row : result.groups()) {
            var group = groups.remove(row.groupId());
            verify(group != null && row.type() == group.type() && row.tier() == group.tier()
                    && row.initialCount() == group.count(), "Battle group differs from fixed input");
            // GroupResult 已验证非负与人数守恒，但分伤/死亡的规则仍必须由 GAME 独立复核。
            verify(row.wounded() == ExpeditionBattleEngine.woundedFromLosses(row.losses()),
                    "Wounded count differs from battle rules");
        }
        verify(groups.isEmpty(), "Battle report omitted a group");
    }
}

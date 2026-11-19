package com.slg.game;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.HeroState;
import com.slg.game.domain.persistence.PlayerState;
import com.slg.game.domain.persistence.TroopState;
import com.slg.game.domain.player.PlayerDetails;
import com.slg.game.application.player.PlayerOutcome;
import com.slg.game.domain.persistence.MarchState;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.rpc.v1.TroopGroup;
import com.slg.rpc.v1.HeroSelection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PlayerRpcAdapterTest {
    @Test void battleReportKeepsPendingEmptyAndUsesThePersistedIdentityForSettledResult() {
        var input = new com.slg.core.battle.BattleInput("battle-1",
                new com.slg.core.battle.Army("1_7", List.of(new com.slg.core.battle.TroopGroup("a", TroopType.INFANTRY, 1, 200))),
                new com.slg.core.battle.Army("npc", List.of(new com.slg.core.battle.TroopGroup("d", TroopType.INFANTRY, 1, 20))), 7, 100);
        String hash = "a".repeat(64), rules = com.slg.core.battle.ExpeditionBattleEngine.RULES_VERSION;
        var pending = PlayerRpcAdapter.battle(new com.slg.game.domain.march.BattleReport("battle-1", "battle-1",
                com.slg.game.domain.persistence.BattleState.Status.PENDING, rules, hash, null));
        assertEquals(com.slg.rpc.v1.BattleReportState.REPORT_PENDING, pending.getState()); assertFalse(pending.hasResult());
        var result = new com.slg.core.battle.ExpeditionBattleEngine().simulate(input);
        var settled = PlayerRpcAdapter.battle(new com.slg.game.domain.march.BattleReport("battle-1", "battle-1",
                com.slg.game.domain.persistence.BattleState.Status.SETTLED, rules, hash, result));
        assertEquals(com.slg.rpc.v1.BattleReportState.REPORT_SETTLED, settled.getState());
        assertEquals(hash, settled.getInputHash()); assertEquals(hash, settled.getResult().getInputHash());
        assertEquals(result.attacker().survivors(), settled.getResult().getAttacker().getGroups(0).getSurvivors());
    }

    @Test void marchWireMappingPreservesOriginalOperationAndDoesNotReuseStorageStateCodes() {
        // 存储 RETURNING=4/BATTLE_PENDING=3，wire 正好相反，必须按含义显式转换。
        var returning = new MarchState("m1", "1_7", MarchState.Status.RETURNING, MarchState.Mission.GATHER,
                150, 150, 100, 100, 1, 3001, 1, Map.of(TroopType.INFANTRY, 30L), List.of(), "return-m1", "gather-v1", Map.of());
        assertEquals(com.slg.rpc.v1.MarchState.MARCH_STATE_RETURNING, PlayerRpcAdapter.march(returning).getState());
        var receipt = PlayerRpcAdapter.writeReceipt(PlayerOutcome.recalled(returning));
        assertEquals(GameplayRoutes.RECALL_MARCH, receipt.getMsgId()); assertTrue(receipt.hasRecalled());
        var rejected = PlayerRpcAdapter.writeReceipt(PlayerOutcome.rejected(PlayerOutcome.START, "MARCH_LIMIT", "Limit"));
        assertEquals(GameplayRoutes.START_MARCH, rejected.getMsgId()); assertTrue(rejected.hasError());
        var input = StartMarchReq.newBuilder().setMissionValue(999).setTargetX(0)
                .addTroops(TroopGroup.newBuilder().setTypeValue(998).setCount(5))
                .addTroops(TroopGroup.newBuilder().setTypeValue(998).setCount(5)).setHeroes(HeroSelection.getDefaultInstance());
        var order = PlayerRpcAdapter.startOrder(input.build());
        assertEquals(999, order.mission()); assertEquals(0, order.targetX()); assertNull(order.targetY());
        assertEquals(2, order.troops().size()); assertEquals(998, order.troops().getFirst().type());
        assertEquals(List.of(), order.heroIds());
        assertNull(PlayerRpcAdapter.startOrder(input.clearHeroes().build()).heroIds());
    }

    @Test void projectsTypedAssetsAndNonzeroCasualtiesWithoutChangingPermanentPlayerIdentityOrProfileVersion() {
        var profile = new PlayerState("2_101", 101, 2, 1, "迁入角色");
        var troops = Map.of(TroopType.INFANTRY, new TroopState("2_101", TroopType.INFANTRY, 5, 2, 0),
                TroopType.LANCER, new TroopState("2_101", TroopType.LANCER, 6, 0, 3),
                TroopType.MARKSMAN, new TroopState("2_101", TroopType.MARKSMAN, 7, 0, 0));
        var details = new PlayerDetails(9, profile, Map.of(ResourceType.WOOD, 0L, ResourceType.FOOD, 4L, ResourceType.GEMS, 5L),
                troops, List.of(new HeroState("2_101", "sergey", "march-1")), List.of("march-1"));
        var view = PlayerRpcAdapter.player(details);
        assertEquals("2_101", view.getPlayerId()); assertEquals("101", view.getAccountId());
        assertEquals(2, view.getFoundingRealmId()); assertEquals(1, view.getCurrentRealmId()); assertEquals(9, view.getVersion());
        assertEquals(Map.of("WOOD", 0L, "FOOD", 4L, "GEMS", 5L), view.getResourcesMap());
        assertEquals(Map.of("INFANTRY", 2L), view.getWoundedMap()); assertEquals(Map.of("LANCER", 3L), view.getDeadMap());
        assertEquals("march-1", view.getHeroes(0).getAssignedMarch()); assertEquals(List.of("march-1"), view.getActiveMarchIdsList());
    }
}

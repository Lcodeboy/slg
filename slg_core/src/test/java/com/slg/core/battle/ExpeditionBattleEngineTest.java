package com.slg.core.battle;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExpeditionBattleEngineTest {
    private final ExpeditionBattleEngine engine = new ExpeditionBattleEngine();

    @Test
    void fixedSnapshotAndSeedProduceIdenticalResultsWithoutMutatingInput() {
        Army attackers = army("player", 10_000);
        Army defenders = army("npc", 8_000);
        BattleInput input = new BattleInput("battle", attackers, defenders, 901, 200);
        BattleResult first = engine.simulate(input);
        assertEquals(first, engine.simulate(input));
        assertEquals(30_000, attackers.groups().stream().mapToLong(TroopGroup::count).sum());
        assertEquals("npc", first.defender().ownerId());
        assertEquals(ExpeditionBattleEngine.RULES_VERSION, first.rulesVersion());
        assertConservation(first.attacker());
        assertConservation(first.defender());
    }

    @Test
    void groupListOrderDoesNotChangeDeterministicResult() {
        Army attacker = army("player", 10_000);
        List<TroopGroup> reversed = new ArrayList<>(attacker.groups());
        Collections.reverse(reversed);
        Army defender = army("npc", 10_000);
        BattleResult first = engine.simulate(new BattleInput("battle", attacker, defender, 7, 100));
        BattleResult second = engine.simulate(new BattleInput("battle", new Army("player", reversed), defender, 7, 100));
        assertEquals(first, second);
    }

    @Test
    void billionSoldierGroupsRemainBoundedByGroupAndRoundCount() {
        Army attacker = army("player", TroopGroup.MAX_COUNT);
        Army defender = army("npc", TroopGroup.MAX_COUNT);
        BattleResult result = assertTimeout(Duration.ofSeconds(5),
                () -> engine.simulate(new BattleInput("large", attacker, defender, 19, 1_000)));
        assertEquals(3_000_000_000L, result.attacker().initialCount());
        assertEquals(3, result.attacker().groups().size());
        assertTrue(result.rounds() <= 1_000);
        assertConservation(result.attacker());
        assertConservation(result.defender());
    }

    @Test
    void bothSidesCanBeDestroyedInTheSameRound() {
        Army attacker = new Army("player", List.of(new TroopGroup("a", TroopType.MARKSMAN, 1, 1)), 10, 0.1);
        Army defender = new Army("npc", List.of(new TroopGroup("d", TroopType.MARKSMAN, 1, 1)), 10, 0.1);
        BattleResult result = engine.simulate(new BattleInput("simultaneous", attacker, defender, 1, 1));
        assertEquals(Outcome.DRAW, result.outcome());
        assertEquals(1, result.rounds());
        assertEquals(0, result.attacker().survivors());
        assertEquals(0, result.defender().survivors());
        assertConservation(result.attacker());
        assertConservation(result.defender());
    }

    @Test
    void roundLimitProducesDrawWhenBothArmiesRemainAlive() {
        BattleResult result = engine.simulate(new BattleInput("cap", army("a", 100), army("d", 100), 10, 1));
        assertEquals(1, result.rounds());
        assertEquals(Outcome.DRAW, result.outcome());
        assertTrue(result.attacker().survivors() > 0);
        assertTrue(result.defender().survivors() > 0);
    }

    @Test
    void emptyDefenderSettlesWithoutRunningARound() {
        BattleResult result = engine.simulate(new BattleInput("empty", army("a", 1), new Army("npc", List.of()), 10, 10));
        assertEquals(Outcome.ATTACKER_WIN, result.outcome());
        assertEquals(0, result.rounds());
        assertEquals(0, result.attacker().losses());
        assertEquals(0, result.defender().initialCount());
        assertEquals(Outcome.DRAW, engine.simulate(new BattleInput("both-empty",
                new Army("a", List.of()), new Army("d", List.of()), 0, 1)).outcome());
    }

    @Test
    void immutableSnapshotsAndParameterLimitsPreventInvalidCombat() {
        List<TroopGroup> mutable = new ArrayList<>(List.of(new TroopGroup("group", TroopType.INFANTRY, 1, 10)));
        Army army = new Army("a", mutable);
        mutable.clear();
        assertEquals(1, army.groups().size());
        assertThrows(UnsupportedOperationException.class, () -> army.groups().clear());
        assertThrows(IllegalArgumentException.class, () -> new TroopGroup("x", TroopType.INFANTRY, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new TroopGroup("x", TroopType.INFANTRY, 1, Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> new Army("a", army.groups(), Double.NaN, 1));
        assertThrows(IllegalArgumentException.class, () -> new Army("a", List.of(mutableGroup(), mutableGroup())));
        assertThrows(IllegalArgumentException.class, () -> new BattleInput("x", army, army, 0, 1001));
        assertThrows(IllegalArgumentException.class, () -> new GroupResult("x", TroopType.INFANTRY, 1, 10, 8, 2, 1));
    }

    private static TroopGroup mutableGroup() {
        return new TroopGroup("same", TroopType.INFANTRY, 1, 1);
    }

    @Test void woundedFormulaRoundsDownWithinSingleGroupBounds() {
        assertEquals(0, ExpeditionBattleEngine.woundedFromLosses(0));
        assertEquals(6, ExpeditionBattleEngine.woundedFromLosses(9));
        assertEquals(700_000_000L, ExpeditionBattleEngine.woundedFromLosses(TroopGroup.MAX_COUNT));
        assertThrows(IllegalArgumentException.class, () -> ExpeditionBattleEngine.woundedFromLosses(-1));
        assertThrows(IllegalArgumentException.class, () -> ExpeditionBattleEngine.woundedFromLosses(Long.MAX_VALUE));
    }

    private static Army army(String owner, long count) {
        return new Army(owner, List.of(new TroopGroup("infantry", TroopType.INFANTRY, 3, count),
                new TroopGroup("lancer", TroopType.LANCER, 2, count),
                new TroopGroup("marksman", TroopType.MARKSMAN, 2, count)));
    }

    private static void assertConservation(ArmyResult army) {
        assertEquals(army.initialCount(), army.survivors() + army.wounded() + army.dead());
        assertEquals(army.losses(), army.wounded() + army.dead());
        for (GroupResult group : army.groups()) {
            assertEquals(group.initialCount(), group.survivors() + group.wounded() + group.dead());
            assertTrue(group.survivors() >= 0 && group.wounded() >= 0 && group.dead() >= 0);
        }
    }
}

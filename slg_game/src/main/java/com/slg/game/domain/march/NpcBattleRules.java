package com.slg.game.domain.march;

import com.slg.core.battle.*;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.persistence.*;
import com.slg.game.domain.player.PlayerSnapshot;
import java.util.*;

import static com.slg.game.domain.march.MarchRules.*;

/**
 * NPC 派发前校验与结算纯规则。Worker 只能返回固定输入的战损，资产授权仍属于 GAME；
 * 校验和裁决期间必须持账号、行军、战斗及返城任务资格，远程计算期间不能占用这些资格。
 */
public final class NpcBattleRules {
    private static final Map<ResourceType, Long> WIN_CARGO =
            Map.of(ResourceType.WOOD, 500L, ResourceType.FOOD, 500L, ResourceType.GEMS, 10L);
    private NpcBattleRules() { }

    /**
     * 只证明固定输入仍对应当前预留。JSON schema/原文 SHA 由 codec 校验；此处核对类型化输入与
     * 当前兵力、英雄和规则。该检查不能替代结算时重读，因为 Worker 返回前状态/写权可能变化。
     */
    public static void validateForDispatch(PlayerSnapshot before, VersionedState battleRow,
                                           VersionedState marchRow, BattleInput fixedInput) {
        Objects.requireNonNull(before); Objects.requireNonNull(fixedInput);
        verify(battleRow != null && battleRow.state() instanceof BattleState, "Battle missing");
        verify(marchRow != null && marchRow.state() instanceof MarchState, "Battle march missing");
        var battle = (BattleState) battleRow.state(); var march = (MarchState) marchRow.state();
        verify(before.profile().isPresent() && before.playerId().equals(battle.playerId()), "Battle owner differs from player");
        verify(battle.status() == BattleState.Status.PENDING && battle.result() == null, "Battle is not pending");
        verify(ExpeditionBattleEngine.RULES_VERSION.equals(battle.rulesVersion()), "Unsupported battle rules");
        verify(battle.battleId().equals(battle.marchId()) && battle.marchId().equals(march.marchId())
                        && battle.playerId().equals(march.playerId()) && battle.marchVersion() == marchRow.version(),
                "Battle march identity or generation differs");
        verify(march.status() == MarchState.Status.BATTLE_PENDING && march.mission() == MarchState.Mission.ATTACK_NPC
                        && NPC_MARCH_RULES.equals(march.rulesVersion()) && march.taskId() == null
                        && march.cargo().isEmpty() && march.tier() == 1 && battle.heroIds().equals(march.heroIds()),
                "Battle no longer owns this march");
        verify(marchRow.equals(before.activeMarches().get(march.marchId())), "Player active battle march differs");
        verify(MarchMap.isNpc(march.targetX(), march.targetY()) && coordinate(march.fromX()) && coordinate(march.fromY()),
                "Battle march route is invalid");
        for (String id : march.heroIds()) {
            var row = before.heroes().get(id);
            verify(row != null && row.state() instanceof HeroState hero && march.marchId().equals(hero.assignedMarchId()),
                    "Battle hero reservation differs");
        }
        verify(fixedInput.battleId().equals(battle.battleId()) && fixedInput.attacker().ownerId().equals(battle.playerId())
                        && "npc".equals(fixedInput.defender().ownerId()), "Fixed battle input identity differs");
        var reserved = new EnumMap<TroopType, Long>(TroopType.class);
        for (var group : fixedInput.attacker().groups()) {
            verify(group.tier() == march.tier() && group.count() > 0, "Invalid fixed attacker group");
            reserved.merge(group.type(), group.count(), Math::addExact);
        }
        verify(!reserved.isEmpty() && reserved.equals(march.troops()), "Fixed input differs from reserved march troops");
    }

    /**
     * 首次合法结算只登记伤亡并形成返城。幸存兵、奖励、英雄与队列槽位仍随该行军占用，
     * 必须等 MARCH_RETURN 同事务交付。重复结果应先由应用核对系统回执，不重新执行本纯规则。
     */
    public static BattleTransition settle(PlayerSnapshot before, VersionedState battleRow, VersionedState marchRow,
                                           Optional<VersionedState> returnTask, BattleInput fixedInput,
                                           BattleResult result, long now) {
        Objects.requireNonNull(returnTask);
        if (now < 0) throw new IllegalArgumentException("Negative server time");
        validateForDispatch(before, battleRow, marchRow, fixedInput);
        var battle = (BattleState) battleRow.state(); var march = (MarchState) marchRow.state();
        verify(returnTask.isEmpty(), "Unexpected existing return task before battle settlement");
        NpcBattleValidation.result(battle, fixedInput, result);
        var dependencies = profileDependency(before);
        dependencies.put(battle.key(), battleRow.version()); dependencies.put(march.key(), marchRow.version());
        for (String id : march.heroIds()) {
            var row = before.heroes().get(id); dependencies.put(row.state().key(), row.version());
        }
        var wounded = new EnumMap<TroopType, Long>(TroopType.class);
        var dead = new EnumMap<TroopType, Long>(TroopType.class);
        var survivors = new EnumMap<TroopType, Long>(TroopType.class);
        for (var group : result.attacker().groups()) {
            add(survivors, group.type(), group.survivors());
            add(wounded, group.type(), group.wounded()); add(dead, group.type(), group.dead());
        }
        var changes = new ArrayList<ObjectChange>();
        for (TroopType type : march.troops().keySet()) {
            var row = before.troops().get(type);
            verify(row != null && row.state() instanceof TroopState, "Battle casualty troop row missing");
            var troop = (TroopState) row.state(); dependencies.put(troop.key(), row.version());
            long injured = wounded.getOrDefault(type, 0L), killed = dead.getOrDefault(type, 0L);
            if (injured != 0 || killed != 0) changes.add(new ObjectChange(troop.key(), row.version(),
                    new TroopState(before.playerId(), type, troop.available(), Math.addExact(troop.wounded(), injured),
                            Math.addExact(troop.dead(), killed))));
        }
        var returningArmy = new MarchState(march.marchId(), march.playerId(), march.status(), march.mission(),
                march.fromX(), march.fromY(), march.targetX(), march.targetY(), march.departAt(), march.arriveAt(),
                march.tier(), survivors, march.heroIds(), march.taskId(), march.rulesVersion(), march.cargo());
        var returning = returning(returningArmy, march.targetX(), march.targetY(), Math.max(march.arriveAt(), now),
                result.outcome() == Outcome.ATTACKER_WIN ? WIN_CARGO : Map.of());
        var task = task(returning, TaskState.Type.MARCH_RETURN, Math.incrementExact(marchRow.version()));
        dependencies.put(task.key(), 0L);
        changes.add(new ObjectChange(march.key(), marchRow.version(), returning));
        changes.add(new ObjectChange(task.key(), 0, task));
        return new BattleTransition(changes, dependencies, returning, result);
    }

    private static void add(Map<TroopType, Long> amounts, TroopType type, long count) {
        if (count > 0) amounts.merge(type, count, Math::addExact);
    }
}

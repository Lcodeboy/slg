package com.slg.core.battle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * 自研简化的远征战斗引擎，用于演示服务端战斗流程，不代表《无尽冬日》的真实公式。
 * 每个兵组使用汇总的整数生命值；双方所有存活兵组先按回合初状态出手，再同步扣除生命。
 * 目标优先级为步兵、枪兵、射手；循环克制倍率为 1.2，逆向为 0.9，同兵种为 1.0。
 * 固定种子生成 [0.9, 1.1) 的伤害波动；损失人数的 70% 向下取整为伤兵，其余为死亡。
 * 医院容量、资源和持久化兵力库存均由外部结算处理。计算量由兵组数与回合数决定，不逐个模拟士兵。
 * 引擎无跨调用可变状态，同一版本、输入和种子可确定性重算。
 */
public final class ExpeditionBattleEngine {
    /** 演示规则版本；调整公式、排序、随机数消耗顺序或伤亡划分时应同步升级。 */
    public static final String RULES_VERSION = "expedition-demo-v1";
    /** 固定兵种、等级、标识排序，使输入列表顺序不影响目标选择和随机数分配。 */
    private static final Comparator<TroopGroup> GROUP_ORDER = Comparator
            .comparing(TroopGroup::type).thenComparingInt(TroopGroup::tier).thenComparing(TroopGroup::groupId);

    /**
     * 在独立的内存状态中计算战斗，不修改输入、不访问数据库或系统时间。
     * 达到回合上限或任一方全灭时结束；双方同回合全灭也判为平局。
     *
     * @param input 包含双方快照、固定随机种子和回合上限的完整输入
     * @return 带规则版本、原始种子及守恒战损的不可变结果
     */
    public BattleResult simulate(BattleInput input) {
        Objects.requireNonNull(input, "input");
        List<GroupState> attackers = states(input.attacker());
        List<GroupState> defenders = states(input.defender());
        Random random = new Random(input.seed());
        int rounds = 0;
        while (rounds < input.maxRounds() && anyAlive(attackers) && anyAlive(defenders)) {
            // 必须先算完双方伤害再应用，保证本回合被击败的兵组仍能完成本回合攻击。
            long[] damageToDefenders = attacks(attackers, defenders, input.attacker(), input.defender(), random);
            long[] damageToAttackers = attacks(defenders, attackers, input.defender(), input.attacker(), random);
            applyDamage(attackers, damageToAttackers);
            applyDamage(defenders, damageToDefenders);
            rounds++;
        }
        boolean attackerAlive = anyAlive(attackers);
        boolean defenderAlive = anyAlive(defenders);
        Outcome outcome = attackerAlive == defenderAlive ? Outcome.DRAW
                : attackerAlive ? Outcome.ATTACKER_WIN : Outcome.DEFENDER_WIN;
        return new BattleResult(input.battleId(), outcome, rounds,
                result(input.attacker(), attackers), result(input.defender(), defenders), RULES_VERSION, input.seed());
    }

    /** 按规则规定的稳定顺序复制兵组状态，确保计算不受调用方列表顺序或后续复用影响。 */
    private static List<GroupState> states(Army army) {
        List<GroupState> states = new ArrayList<>();
        army.groups().stream().sorted(GROUP_ORDER).forEach(group -> states.add(new GroupState(group)));
        return states;
    }

    /** 判断是否至少有一个兵组仍有正生命值，用于回合终止与胜负判断。 */
    private static boolean anyAlive(List<GroupState> groups) {
        return groups.stream().anyMatch(group -> group.health > 0);
    }

    /**
     * 根据回合初状态生成目标侧伤害数组，不在此阶段修改任一方生命值。
     * 本演示版本中所有存活兵组集中攻击排序最前的存活目标，过量伤害不向下一兵组溢出。
     *
     * @param sources 按稳定顺序排列的攻击兵组
     * @param targets 按承伤优先级排列的目标兵组
     * @param sourceArmy 提供攻击倍率的攻击方快照
     * @param targetArmy 提供防御倍率的目标方快照
     * @param random 本次战斗共用的种子随机序列，每个存活攻击兵组消耗一次随机数
     * @return 与目标列表逐项对应的非负整数伤害
     */
    private static long[] attacks(List<GroupState> sources, List<GroupState> targets,
                                  Army sourceArmy, Army targetArmy, Random random) {
        long[] damage = new long[targets.size()];
        int targetIndex = 0;
        while (targetIndex < targets.size() && targets.get(targetIndex).health == 0) targetIndex++;
        if (targetIndex == targets.size()) return damage;
        GroupState target = targets.get(targetIndex);
        // 防御转为大于 1 的除数，避免出现负伤害；所有系数仅属于本演示规则。
        double armor = 1 + (15 + target.group.tier() * 3) * targetArmy.defenseMultiplier() / 100;
        for (GroupState source : sources) {
            if (source.health == 0) continue;
            double raw = source.survivors() * (double) unitAttack(source.group)
                    * sourceArmy.attackMultiplier() * typeMultiplier(source.group.type(), target.group.type())
                    * (0.9 + random.nextDouble() * 0.2) / armor;
            // 每个存活兵组至少造成 1 点整数伤害；精确加法使越界显式失败而非绕回负数。
            damage[targetIndex] = Math.addExact(damage[targetIndex], Math.max(1, Math.round(raw)));
        }
        return damage;
    }

    /** 统一应用一方已计算的回合伤害，将剩余生命下限钳制为 0。 */
    private static void applyDamage(List<GroupState> groups, long[] damage) {
        for (int i = 0; i < groups.size(); i++) {
            GroupState group = groups.get(i);
            group.health -= Math.min(group.health, damage[i]);
        }
    }

    /**
     * 将汇总生命还原为存活人数，再按演示比例划分伤兵与死亡。
     * 先计算战损、再把战损拆为两部分，确保每组初始人数始终等于三类结果之和。
     */
    private static ArmyResult result(Army army, List<GroupState> states) {
        List<GroupResult> results = new ArrayList<>();
        for (GroupState state : states) {
            long survivors = state.survivors();
            long losses = state.group.count() - survivors;
            long wounded = woundedFromLosses(losses);
            results.add(new GroupResult(state.group.groupId(), state.group.type(), state.group.tier(),
                    state.group.count(), survivors, wounded, losses - wounded));
        }
        return new ArmyResult(army.ownerId(), results);
    }

    /** Worker 战报和 GAME 结算共同使用的单组伤兵公式。余数归入死亡，三项数量始终守恒。 */
    public static long woundedFromLosses(long losses) {
        if (losses < 0 || losses > TroopGroup.MAX_COUNT) throw new IllegalArgumentException("Invalid group losses");
        return losses * 7 / 10;
    }

    /** 返回本演示版本按兵种、等级定义的单兵生命点数。 */
    private static int unitHealth(TroopGroup group) {
        return switch (group.type()) {
            case INFANTRY -> 120 + 20 * group.tier();
            case LANCER -> 100 + 15 * group.tier();
            case MARKSMAN -> 80 + 10 * group.tier();
        };
    }

    /** 返回本演示版本的单兵基础攻击点数，实际伤害还需叠加倍率、克制、随机波动与防御。 */
    private static int unitAttack(TroopGroup group) {
        return switch (group.type()) {
            case INFANTRY -> 10 + 2 * group.tier();
            case LANCER -> 13 + 2 * group.tier();
            case MARKSMAN -> 17 + 3 * group.tier();
        };
    }

    /** 返回演示循环克制倍率：步兵克枪兵、枪兵克射手、射手克步兵，同类中性。 */
    private static double typeMultiplier(TroopType source, TroopType target) {
        if (source == target) return 1.0;
        boolean advantage = (source == TroopType.INFANTRY && target == TroopType.LANCER)
                || (source == TroopType.LANCER && target == TroopType.MARKSMAN)
                || (source == TroopType.MARKSMAN && target == TroopType.INFANTRY);
        return advantage ? 1.2 : 0.9;
    }

    /** 单次模拟独占的可变兵组状态，仅维护总生命，不分配单兵对象。 */
    private static final class GroupState {
        /** 不可变参战快照，用于保留初始人数和结果标识。 */
        private final TroopGroup group;
        /** 单兵满生命点数，由规则版本和兵种等级确定。 */
        private final int unitHealth;
        /** 当前汇总生命点数，范围为 0 到初始人数乘以单兵生命。 */
        private long health;

        /** 将初始人数转换为总生命，乘法溢出时显式失败。 */
        private GroupState(TroopGroup group) {
            this.group = group;
            this.unitHealth = unitHealth(group);
            this.health = Math.multiplyExact(group.count(), unitHealth);
        }

        /**
         * 对总生命除以单兵生命向上取整，最后一名尚有部分生命的士兵仍计为存活。
         * 使用 1 + (health - 1) / unitHealth，避免常见向上取整写法的加法溢出。
         */
        private long survivors() {
            return health == 0 ? 0 : 1 + (health - 1) / unitHealth;
        }
    }
}

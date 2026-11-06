package com.slg.game.realm;

import com.slg.rpc.BattleWire;
import com.slg.rpc.v1.BattleReportState;
import com.slg.rpc.v1.GetBattleRsp;
import com.slg.rpc.v1.HeroView;
import com.slg.rpc.v1.MarchMission;
import com.slg.rpc.v1.MarchState;
import com.slg.rpc.v1.MarchView;
import com.slg.rpc.v1.PlayerView;
import com.slg.rpc.v1.TroopGroup;
import com.slg.rpc.v1.TroopType;
import java.io.IOException;
import java.util.TreeMap;

/** 明确客户端可以看到的字段。写回执和查询复用同一转换，区别在于传入的是提交当时还是当前的快照。 */
final class GameplayViews {
    private GameplayViews() {}

    static PlayerView player(ActivePlayer.Player player, long version) {
        var view = PlayerView.newBuilder().setPlayerId(player.playerId()).setAccountId(player.accountId())
                .setFoundingRealmId(player.foundingRealmId()).setCurrentRealmId(player.currentRealmId())
                .setVersion(version).setName(player.name()).putAllTroops(player.troops()).putAllResources(player.resources())
                .putAllWounded(player.wounded()).putAllDead(player.dead()).addAllActiveMarchIds(player.activeMarchIds());
        for (var hero : player.heroes()) {
            var out = HeroView.newBuilder().setHeroId(hero.heroId());
            if (hero.assignedMarch() != null) out.setAssignedMarch(hero.assignedMarch());
            view.addHeroes(out);
        }
        return view.build();
    }

    static MarchView march(MarchTasks.March march) throws IOException {
        var mission = switch (march.mission()) {
            case "GATHER" -> MarchMission.MARCH_MISSION_GATHER;
            case "ATTACK_NPC" -> MarchMission.MARCH_MISSION_ATTACK_NPC;
            default -> throw new IOException("Invalid stored march mission");
        };
        var state = switch (march.state()) {
            case "OUTBOUND" -> MarchState.MARCH_STATE_OUTBOUND;
            case "GATHERING" -> MarchState.MARCH_STATE_GATHERING;
            case "RETURNING" -> MarchState.MARCH_STATE_RETURNING;
            case "BATTLE_PENDING" -> MarchState.MARCH_STATE_BATTLE_PENDING;
            case "COMPLETED" -> MarchState.MARCH_STATE_COMPLETED;
            default -> throw new IOException("Invalid stored march state");
        };
        var view = MarchView.newBuilder().setMarchId(march.marchId()).setPlayerId(march.playerId())
                .setState(state).setMission(mission).setFromX(march.fromX()).setFromY(march.fromY())
                .setTargetX(march.targetX()).setTargetY(march.targetY()).setDepartAt(march.departAt())
                .setArriveAt(march.arriveAt()).setTier(march.tier()).addAllHeroIds(march.heroIds())
                .setRulesVersion(march.rulesVersion()).putAllCargo(march.cargo());
        if (march.taskId() != null) view.setTaskId(march.taskId());
        for (var group : new TreeMap<>(march.troops()).entrySet()) {
            var type = switch (group.getKey()) {
                case "INFANTRY" -> TroopType.TROOP_TYPE_INFANTRY;
                case "LANCER" -> TroopType.TROOP_TYPE_LANCER;
                case "MARKSMAN" -> TroopType.TROOP_TYPE_MARKSMAN;
                default -> throw new IOException("Invalid stored troop type");
            };
            view.addTroops(TroopGroup.newBuilder().setType(type).setCount(group.getValue()));
        }
        return view.build();
    }

    static GetBattleRsp battle(NpcBattles.Report report) throws IOException {
        var state = switch (report.state()) {
            case "PENDING" -> BattleReportState.REPORT_PENDING;
            case "SETTLED" -> BattleReportState.REPORT_SETTLED;
            default -> throw new IOException("Invalid stored battle state");
        };
        if ((report.result() != null) != (state == BattleReportState.REPORT_SETTLED))
            throw new IOException("Battle result disagrees with its state");
        var view = GetBattleRsp.newBuilder().setBattleId(report.battleId()).setMarchId(report.marchId())
                .setState(state).setRulesVersion(report.rulesVersion()).setInputHash(report.inputHash());
        if (report.result() != null) view.setResult(BattleWire.reply(report.result(), report.inputHash()));
        return view.build();
    }
}

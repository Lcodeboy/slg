package com.slg.game;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.player.PlayerApplication;
import com.slg.game.application.player.PlayerOutcome;
import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.persistence.CommitReceipt;
import com.slg.game.domain.march.StartMarchOrder;
import com.slg.game.domain.march.BattleReport;
import com.slg.rpc.BattleWire;
import com.slg.game.domain.player.PlayerDetails;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;
import java.util.Comparator;

/** 玩家应用结果的协议投影；不重算业务、不读缓存，也不把当前玩家详情冒充原提交响应。 */
final class PlayerRpcAdapter {
    private PlayerRpcAdapter() { }

    /** 战报只转换已验证的权威结果，不调用 Worker 或把 PENDING 伪装成零伤亡战果。 */
    static GetBattleRsp battle(BattleReport report) {
        var response = GetBattleRsp.newBuilder().setBattleId(report.battleId()).setMarchId(report.marchId())
                .setState(switch (report.status()) {
                    case PENDING -> BattleReportState.REPORT_PENDING;
                    case SETTLED -> BattleReportState.REPORT_SETTLED;
                }).setRulesVersion(report.rulesVersion()).setInputHash(report.inputHash());
        // 公开战报携带持久摘要；Worker 传输摘要仅用于内部计算请求的校验，不替换这份身份。
        if (report.result() != null) response.setResult(BattleWire.reply(report.result(), report.inputHash()));
        return response.build();
    }

    static PlayerView player(PlayerDetails details) {
        var profile = details.profile();
        var view = PlayerView.newBuilder().setPlayerId(profile.playerId()).setAccountId(Long.toString(profile.accountId()))
                .setFoundingRealmId(profile.foundingRealmId()).setCurrentRealmId(profile.currentRealmId())
                .setName(profile.name()).setVersion(details.version()).addAllActiveMarchIds(details.activeMarchIds());
        details.resources().forEach((type, amount) -> view.putResources(resource(type), amount));
        details.troops().forEach((type, troop) -> {
            String key = troop(type);
            view.putTroops(key, troop.available());
            // 旧客户端用缺键表达零伤亡；数据库仍保存明确的零，不能丢掉非零伤亡。
            if (troop.wounded() != 0) view.putWounded(key, troop.wounded());
            if (troop.dead() != 0) view.putDead(key, troop.dead());
        });
        details.heroes().forEach(hero -> {
            var result = HeroView.newBuilder().setHeroId(hero.heroId());
            if (hero.assignedMarchId() != null) result.setAssignedMarch(hero.assignedMarchId());
            view.addHeroes(result);
        });
        return view.build();
    }

    static PlayReply created(PlayRequest request, PlayerApplication application, CommitReceipt receipt) {
        return written(request, application, receipt);
    }

    static PlayReply written(PlayRequest request, PlayerApplication application, CommitReceipt receipt) {
        var outcome = application.outcome(receipt);
        var original = writeReceipt(outcome);
        if (original.getMsgId() != request.getMsgId())
            throw new IllegalArgumentException("Stored operation differs from requested operation");
        var reply = PlayReply.newBuilder().setMsgId(request.getMsgId()).setRequestId(request.getRequestId()).setStatus(200);
        if (original.hasError()) reply.setError(original.getError());
        else reply.setBody(switch (outcome.operation()) {
            case PlayerOutcome.CREATE -> original.getCreated().toByteString();
            case PlayerOutcome.START -> original.getStarted().toByteString();
            case PlayerOutcome.RECALL -> original.getRecalled().toByteString();
            default -> throw new IllegalArgumentException("Unknown player receipt operation");
        });
        return reply.build();
    }

    static GetReceiptRsp receipt(String target, PlayerApplication application, RealmReceiptReader.Observation observed) {
        var response = GetReceiptRsp.newBuilder().setTargetRequestId(target);
        if (observed instanceof RealmReceiptReader.Observation.Found found) {
            var original = application.outcome(found.receipt());
            // 查询必须恢复原操作及原响应；即使当前行军已返城，出征回执仍保持出征时的 OUTBOUND。
            response.setJournalSeq(found.receipt().sequence()).setReceipt(writeReceipt(original));
        }
        // 未观察到回执不是“确定未提交”，保持既有无 receipt 的观察语义。
        return response.build();
    }

    static WriteReceipt writeReceipt(PlayerOutcome outcome) {
        var receipt = WriteReceipt.newBuilder().setMsgId(switch (outcome.operation()) {
            case PlayerOutcome.CREATE -> GameplayRoutes.CREATE_PLAYER;
            case PlayerOutcome.START -> GameplayRoutes.START_MARCH;
            case PlayerOutcome.RECALL -> GameplayRoutes.RECALL_MARCH;
            default -> throw new IllegalArgumentException("Unknown player receipt operation");
        });
        if (outcome.errorCode() != null) return receipt.setError(error(outcome)).build();
        switch (outcome.operation()) {
            case PlayerOutcome.CREATE -> receipt.setCreated(CreatePlayerRsp.newBuilder().setPlayer(player(outcome.player())));
            case PlayerOutcome.START -> receipt.setStarted(StartMarchRsp.newBuilder().setMarch(march(outcome.march())));
            case PlayerOutcome.RECALL -> receipt.setRecalled(RecallMarchRsp.newBuilder().setMarch(march(outcome.march())));
            default -> throw new IllegalArgumentException("Unknown player receipt operation");
        }
        return receipt.build();
    }

    static StartMarchOrder startOrder(StartMarchReq request) {
        // 原始枚举整数、optional presence 与重复项留给领域规则；适配阶段不吞非法输入或套默认值。
        return new StartMarchOrder(request.hasMission() ? request.getMissionValue() : null,
                request.hasTargetX() ? request.getTargetX() : null, request.hasTargetY() ? request.getTargetY() : null,
                request.hasTier() ? request.getTier() : null,
                request.getTroopsList().stream().map(group -> new StartMarchOrder.TroopSelection(group.getTypeValue(), group.getCount())).toList(),
                request.hasHeroes() ? request.getHeroes().getHeroIdsList() : null);
    }

    static MarchView march(com.slg.game.domain.persistence.MarchState value) {
        var view = MarchView.newBuilder().setMarchId(value.marchId()).setPlayerId(value.playerId())
                .setState(switch (value.status()) {
                    case OUTBOUND -> MarchState.MARCH_STATE_OUTBOUND;
                    case GATHERING -> MarchState.MARCH_STATE_GATHERING;
                    case BATTLE_PENDING -> MarchState.MARCH_STATE_BATTLE_PENDING;
                    case RETURNING -> MarchState.MARCH_STATE_RETURNING;
                    case COMPLETED -> MarchState.MARCH_STATE_COMPLETED;
                }).setMission(switch (value.mission()) {
                    case GATHER -> MarchMission.MARCH_MISSION_GATHER;
                    case ATTACK_NPC -> MarchMission.MARCH_MISSION_ATTACK_NPC;
                }).setFromX(value.fromX()).setFromY(value.fromY()).setTargetX(value.targetX()).setTargetY(value.targetY())
                .setDepartAt(value.departAt()).setArriveAt(value.arriveAt()).setTier(value.tier())
                .addAllHeroIds(value.heroIds()).setRulesVersion(value.rulesVersion());
        if (value.taskId() != null) view.setTaskId(value.taskId());
        value.troops().entrySet().stream().sorted(Comparator.comparingInt(entry -> entry.getKey().code())).forEach(group ->
                view.addTroops(TroopGroup.newBuilder().setType(switch (group.getKey()) {
                    case INFANTRY -> com.slg.rpc.v1.TroopType.TROOP_TYPE_INFANTRY;
                    case LANCER -> com.slg.rpc.v1.TroopType.TROOP_TYPE_LANCER;
                    case MARKSMAN -> com.slg.rpc.v1.TroopType.TROOP_TYPE_MARKSMAN;
                }).setCount(group.getValue())));
        value.cargo().forEach((type, count) -> view.putCargo(resource(type), count));
        return view.build();
    }

    private static BusinessError error(PlayerOutcome outcome) {
        return BusinessError.newBuilder().setCode(outcome.errorCode()).setMessage(outcome.errorMessage()).build();
    }
    private static String resource(ResourceType type) {
        return switch (type) { case WOOD -> "WOOD"; case FOOD -> "FOOD"; case GEMS -> "GEMS"; };
    }
    private static String troop(TroopType type) {
        return switch (type) { case INFANTRY -> "INFANTRY"; case LANCER -> "LANCER"; case MARKSMAN -> "MARKSMAN"; };
    }
}

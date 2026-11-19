package com.slg.game.infrastructure.mysql.codec;

import com.slg.core.battle.*;
import com.slg.game.application.background.*;
import com.slg.game.application.march.MarchPlan;
import com.slg.game.application.port.RealmWorkReader;
import com.slg.game.domain.march.PendingBattle;
import com.slg.game.domain.persistence.*;
import com.slg.rpc.BattleWire;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BackgroundJsonCodecTest {
    private final BackgroundJsonCodec codec = new BackgroundJsonCodec();

    @Test void systemIdentityBindsCandidateButExcludesPlanAndWorkerResult() {
        var task = new TaskState("arrive-m1", "1_7", "m1", TaskState.Type.MARCH_ARRIVE, TaskState.Status.PENDING, 1, 4000, null);
        var row = new VersionedState(1, task);
        var initial = new BackgroundInvocation(7, row, null, null);
        var planned = new BackgroundInvocation(7, row, new MarchPlan("m1", 1, 150, 150), null);
        assertEquals("due:7", codec.request(row).scope());
        assertEquals(codec.digest(initial), codec.digest(planned));
        assertNotEquals(codec.request(row), codec.request(new VersionedState(2, task)));
        assertEquals(planned, codec.decodeInvocation(codec.encodeInvocation(planned)));
        assertEquals(planned.plan(), codec.decodePlan(codec.encodePlan(planned.plan())));
        var outcome = new BackgroundOutcome("DUE_TASK", 7, task.taskId(), 1, "BLOCKED");
        assertEquals(outcome, codec.decodeOutcome(codec.encodeOutcome(outcome)));
        String json = new String(codec.encodeInvocation(initial), StandardCharsets.UTF_8);
        assertThrows(CodecException.class, () -> codec.decodeInvocation(json.replaceFirst("\\{", "{\"schema\":1,").getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> codec.decodeInvocation(json.replace("\"accountId\":7", "\"accountId\":8").getBytes(StandardCharsets.UTF_8)));
    }

    @Test void fixedBattleDispatchAndOriginalReportValidatePersistentHashSeparatelyFromWireHash() {
        var fixed = new BattleInput("battle-1", new Army("1_7", List.of(new TroopGroup("infantry", TroopType.INFANTRY, 1, 200))),
                new Army("npc", List.of(new TroopGroup("npc", TroopType.INFANTRY, 1, 20))), 7, 100);
        var battle = new MarchPayloadJsonCodec().prepareBattle(new PendingBattle("battle-1", "1_7", List.of(), 2,
                ExpeditionBattleEngine.RULES_VERSION, fixed));
        assertNotEquals(BattleWire.inputHash(fixed), battle.inputHash(), "Storage and wire use different canonical formats");
        var dispatch = new BattleDispatch(new RealmWorkReader.PendingBattle(1, battle), fixed);
        assertEquals(Optional.of(dispatch), codec.decodeDispatch(codec.encodeDispatch(Optional.of(dispatch))));
        assertEquals(Optional.empty(), codec.decodeDispatch(codec.encodeDispatch(Optional.empty())));
        var result = new ExpeditionBattleEngine().simulate(fixed);
        var row = new VersionedState(1, battle);
        var invocation = new BackgroundInvocation(7, row, null, result);
        assertEquals(codec.digest(invocation), codec.digest(new BackgroundInvocation(7, row, null, null)));
        assertEquals(invocation, codec.decodeInvocation(codec.encodeInvocation(invocation)));
        var settled = new BattleState(battle.battleId(), battle.marchId(), battle.playerId(), BattleState.Status.SETTLED,
                battle.heroIds(), battle.inputHash(), battle.rulesVersion(), battle.marchVersion(), battle.input(), codec.result(result));
        var report = codec.report(settled);
        assertEquals(report, codec.decodeReport(codec.encodeReport(report)));
        assertEquals(result, report.result()); assertEquals(battle.inputHash(), report.inputHash());
        var corrupt = new BattleState(battle.battleId(), battle.marchId(), battle.playerId(), BattleState.Status.PENDING,
                battle.heroIds(), "0".repeat(64), battle.rulesVersion(), battle.marchVersion(), battle.input(), null);
        assertThrows(CodecException.class, () -> codec.input(corrupt));
    }
}

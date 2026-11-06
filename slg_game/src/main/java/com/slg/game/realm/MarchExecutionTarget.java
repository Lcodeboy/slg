package com.slg.game.realm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal;
import java.io.IOException;
import java.util.regex.Pattern;

/** 为已知行军找出本次可能修改的第二个对象；真正的归属、代次和业务资格仍在执行时重验。 */
public final class MarchExecutionTarget {
    private static final Pattern MARCH_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private MarchExecutionTarget() {}

    public static String forMarch(RealmJournal journal, String playerId, String marchId) throws IOException {
        if (marchId == null || !MARCH_ID.matcher(marchId).matches()) return null;
        var value = journal.read("march:" + marchId);
        if (value == null) return null;
        MarchTasks.March march;
        try { march = Json.MAPPER.readValue(value.json(), MarchTasks.March.class); }
        catch (JsonProcessingException malformed) {
            // 领域处理器会隔离这条无法解码的任务；它无法据此修改任何矿点。原卷读取失败则向上抛出。
            return null;
        }
        if (march == null || !playerId.equals(march.playerId()) || !"GATHER".equals(march.mission())) return null;
        // 返程目标已变成主城。到达或采集结束都会在离开当前阶段的同批次完成占/放矿。
        return "OUTBOUND".equals(march.state()) || "GATHERING".equals(march.state())
                ? GatherSite.key(march.targetX(), march.targetY()) : null;
    }
}

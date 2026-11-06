package com.slg.game.realm;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import com.slg.core.protocol.Json;
import com.slg.game.realm.MarchTasks.BrokenTask;
import com.slg.game.realm.MarchTasks.March;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.Change;
import java.io.IOException;

/** 每个采集点共享一份有版本的占用记录。地形定义哪些点可采；第一次实际占用时才创建记录。 */
record GatherSite(int x, int y, String occupantMarchId, String occupantPlayerId, int generation) {
    static final String RULES_VERSION = "realm-gather-v2";
    private static final ObjectReader READER = Json.MAPPER.readerFor(GatherSite.class)
            .with(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

    /** 启服前分页检查所有待完成的采集，包括尚未到期的队伍。每个任务必须引用对应代次的行军，
     * 并持有同一矿点。任一对不上就停止接流，避免新到达者占走尚未核实的矿，或对错误队伍发奖。 */
    static void verifyActiveReservations(RealmJournal journal) throws IOException {
        RealmJournal.DueTask cursor = null;
        while (true) {
            var page = journal.dueTasks(Long.MAX_VALUE, cursor, 128);
            for (var due : page) {
                if (!"GATHER_DONE".equals(due.type())) continue;
                var value = journal.read("march:" + due.marchId());
                if (value == null || value.version() != due.generation())
                    throw new IOException("Gather task generation disagrees with march " + due.marchId());
                March march = Json.MAPPER.readValue(value.json(), March.class);
                if (march == null || !due.marchId().equals(march.marchId()) || !"GATHER".equals(march.mission())
                        || !"GATHERING".equals(march.state()) || !RULES_VERSION.equals(march.rulesVersion())
                        || !due.playerId().equals(march.playerId()) || !due.taskId().equals(march.taskId())
                        || due.dueAt() != march.arriveAt())
                    throw new IOException("Gather task disagrees with march or rules: " + due.taskId());
                try {
                    if (!read(journal, march.targetX(), march.targetY()).heldBy(march, due.generation()))
                        throw new IOException("Gather reservation disagrees with task " + due.taskId());
                } catch (BrokenTask invalid) {
                    throw new IOException("Invalid reservation for gather task " + due.taskId(), invalid);
                }
            }
            if (page.size() < 128) return;
            cursor = page.getLast();
        }
    }

    static boolean exists(int x, int y) {
        return x == y && (x == 150 || x == 650 || x == 1150);
    }

    static String key(int x, int y) { return "resource:" + x + "-" + y; }

    record Stored(GatherSite site, long version) {
        boolean occupied() { return site.occupantMarchId() != null; }

        Change claim(March march, int generation) {
            if (occupied()) throw new BrokenTask("Gather site already occupied");
            return change(new GatherSite(site.x(), site.y(), march.marchId(), march.playerId(), generation));
        }

        Change release(March march, int generation) {
            // taskId/generation 校验保护行军；这里再核对矿上的属主与代次，旧任务不能释放新占用。
            if (!heldBy(march, generation))
                throw new BrokenTask("Gather site reservation disagrees with the current march and generation");
            return change(empty(site.x(), site.y()));
        }

        boolean heldBy(March march, int generation) {
            return march.marchId().equals(site.occupantMarchId()) && march.playerId().equals(site.occupantPlayerId())
                    && generation == site.generation();
        }

        private Change change(GatherSite next) {
            // 空闲也保留记录及其版本，避免重新占领后旧草案误以为矿点仍处于原来的空闲版本。
            return new Change(key(next.x(), next.y()), version, Json.write(next));
        }
    }

    static Stored read(RealmJournal journal, int x, int y) throws IOException {
        if (!exists(x, y)) throw new BrokenTask("Unknown gather site");
        var value = journal.read(key(x, y));
        if (value == null) return new Stored(empty(x, y), 0);
        GatherSite site;
        try { site = READER.readValue(value.json()); }
        catch (IOException invalid) { throw new BrokenTask("Malformed gather site"); }
        if (site == null || site.x() != x || site.y() != y)
            throw new BrokenTask("Gather site identity mismatch");
        boolean free = site.occupantMarchId() == null && site.occupantPlayerId() == null && site.generation() == 0;
        boolean held = site.occupantMarchId() != null && !site.occupantMarchId().isBlank()
                && site.occupantPlayerId() != null && !site.occupantPlayerId().isBlank() && site.generation() > 0;
        if (!free && !held) throw new BrokenTask("Malformed gather site reservation");
        return new Stored(site, value.version());
    }

    private static GatherSite empty(int x, int y) { return new GatherSite(x, y, null, null, 0); }
}

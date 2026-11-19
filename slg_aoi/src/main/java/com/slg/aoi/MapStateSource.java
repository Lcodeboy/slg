package com.slg.aoi;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * AOI 唯一的权威数据入口。接口只交付不可变的已提交状态，不暴露 Player、World、数据库句柄
 * 或传输协议对象。同进程使用本地适配器；以后拆进程时可以在此处接入远程协议。
 */
public interface MapStateSource {
    Identity identity();

    /**
     * 所有回调必须来自同一个固定水位快照。正常返回的 H 才是“完整结束标记”；异常退出时
     * 接收方必须丢弃全部半成品。读取适配器应定期调用 check，以释放取消或超时的读取快照。
     */
    long snapshot(Check check, Consumer<Route> receiver, Consumer<WorldObject> objects) throws Exception;

    /** 连续扫描日志，包括没有地图变化的决定。throughSeq 表示完整处理边界，不能按最后一条路线推断。 */
    Page changes(long afterSeq, int limit) throws Exception;

    /**
     * 通知仅用于唤醒，允许合并和遗漏；接收方仍按游标补读并周期检查。实现不能在提交线程
     * 等待 AOI 消费，也不能把通知异常传播为业务提交失败。关闭句柄后停止注册的通知。
     */
    default AutoCloseable onAdvance(Runnable wakeup) throws Exception { return () -> {}; }

    @FunctionalInterface interface Check { void run() throws Exception; }

    /**
     * 地图游标所属的权威数据集；重建或更换数据集后必须丢弃旧游标和视图。
     * datasetId 来自权威数据集身份；重建 AOI 索引只改变 generation，不能更换该身份。
     */
    record Identity(int realmId, String datasetId) {
        public Identity {
            if (realmId <= 0 || datasetId == null || datasetId.isBlank())
                throw new IllegalArgumentException("Realm and dataset identity required");
        }
    }

    enum Direction { OUTBOUND, RETURNING }

    /** 公共地图路线不携带兵力、货物、账户令牌或玩家内部对象引用。 */
    record Route(String marchId, long version, Direction direction, double fromX, double fromY,
                 int targetX, int targetY, long departAt, long arriveAt) {
        public Route {
            if (marchId == null || !marchId.matches("[A-Za-z0-9_-]{1,64}") || version < 1
                    || !Double.isFinite(fromX) || !Double.isFinite(fromY) || fromX < 0 || fromY < 0
                    || targetX < 0 || targetY < 0 || departAt < 0 || arriveAt <= departAt)
                throw new IllegalArgumentException("Invalid map route");
            Objects.requireNonNull(direction, "direction");
        }
    }

    /** route 为 null 表示删除；删除仍保留版本，防止迟到事件把已经消失的路线重新插入。 */
    record Change(long sequence, String marchId, long version, Route route) {
        public Change {
            if (sequence < 1 || marchId == null || !marchId.matches("[A-Za-z0-9_-]{1,64}") || version < 1)
                throw new IllegalArgumentException("Invalid map change");
            if (route != null && (!route.marchId().equals(marchId) || route.version() != version))
                throw new IllegalArgumentException("Change differs from its route");
        }
    }

    /**
     * 公共点对象与路线分开表达：城市/矿点按坐标落入视野，不伪装成带时间的零长行军。
     * 所有字段均不可变；角色公开标识不是登录账号，不能由地图对象反向取得玩家可变实体。
     */
    sealed interface WorldObject permits City, ResourceSite {
        String objectId();
        long version();
        int x();
        int y();
    }

    record City(String playerId, long version, int x, int y, String name) implements WorldObject {
        public City {
            if (!validPlayerId(playerId) || version < 1 || x < 0 || y < 0 || name == null || name.isBlank())
                throw new IllegalArgumentException("Invalid public city");
        }
        @Override public String objectId() { return "city:" + playerId; }
    }

    /**
     * 未首次占用的静态矿点没有持久变更，版本必须为 0，不能伪造版本 1 与首次占矿冲突。
     * 空 owner 表示空闲；资源与占用字段是公开地图信息，不包含采集货物、兵力或任务代次。
     */
    record ResourceSite(int x, int y, long version, String resourceType, String occupantPlayerId) implements WorldObject {
        public ResourceSite {
            if (x < 0 || y < 0 || version < 0 || !"WOOD".equals(resourceType) || occupantPlayerId == null
                    || (!occupantPlayerId.isEmpty() && (!validPlayerId(occupantPlayerId) || version == 0)))
                throw new IllegalArgumentException("Invalid public resource site");
        }
        @Override public String objectId() { return "resource:" + x + "-" + y; }
    }

    /** 删除同样保留对象类型前缀和持久版本；其后迟到的旧占用不能复活已移除的对象。 */
    record ObjectChange(long sequence, String objectId, long version, WorldObject object) {
        public ObjectChange {
            if (sequence < 1 || version < 1 || !validObjectId(objectId))
                throw new IllegalArgumentException("Invalid map object change");
            if (object != null && (!objectId.equals(object.objectId()) || version != object.version()))
                throw new IllegalArgumentException("Change differs from its map object");
        }
    }

    private static boolean validPlayerId(String value) {
        return value != null && value.length() <= 64 && value.matches("[1-9][0-9]*_[1-9][0-9]*");
    }

    private static boolean validObjectId(String value) {
        return value != null && ((value.startsWith("city:") && validPlayerId(value.substring(5)))
                || (value.length() <= 64 && value.matches("resource:(0|[1-9][0-9]*)-(0|[1-9][0-9]*)")));
    }

    /** afterSeq 绑定本次读取起点，防止异步完成的旧页被错误接到新索引。 */
    record Page(Identity identity, long afterSeq, long prunedThrough, long highSeq,
                long throughSeq, List<Change> changes, List<ObjectChange> objectChanges) {
        public Page {
            Objects.requireNonNull(identity, "identity");
            changes = List.copyOf(changes);
            objectChanges = List.copyOf(objectChanges);
        }
    }

    /** 保留窗口已经越过消费位置，允许重建；不是可以跳过缺口继续消费的普通错误。 */
    final class CursorExpired extends IOException {
        private final long prunedThrough;
        public CursorExpired(long prunedThrough) {
            super("Map cursor pruned through " + prunedThrough);
            this.prunedThrough = prunedThrough;
        }
        public long prunedThrough() { return prunedThrough; }
    }

    /** 游标超出同一读取视图的真实 H；AOI 必须停止，RPC 可明确报告调用方前置条件不成立。 */
    final class CursorAhead extends BadData {
        private final long high;
        public CursorAhead(long high) {
            super("Map cursor is ahead of high watermark " + high);
            if (high < 0) throw new IllegalArgumentException("High watermark must be nonnegative");
            this.high = high;
        }
        public long high() { return high; }
    }

    /** 已提交数据不能解释或连续性被破坏。必须停在原游标，不能按网络重试策略跨过坏页。 */
    class BadData extends IOException {
        public BadData(String message) { super(message); }
        public BadData(String message, Throwable cause) { super(message, cause); }
    }
}

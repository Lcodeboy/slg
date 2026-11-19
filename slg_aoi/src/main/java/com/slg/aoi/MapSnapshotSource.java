package com.slg.aoi;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 可流式封包的地图供数扩展。begin 只声明当前读取视图的固定 H，不证明快照完整；
 * 接收方只能在 snapshot 正常返回且返回值仍等于 H 后发布索引或发送完整结束标记。
 * 失败、取消或容量超限必须丢弃已接收的全部半成品，不能把已发送块当作可见的地图。
 */
public interface MapSnapshotSource extends MapStateSource {
    /** begin 必须恰好一次并先于任何 DTO，所有回调与正常返回均来自同一读取视图。 */
    long snapshot(Check check, SnapshotReceiver receiver) throws Exception;

    interface SnapshotReceiver {
        void begin(long highSequence) throws Exception;
        void route(Route route) throws Exception;
        void object(WorldObject object) throws Exception;
    }

    /** 本地 AOI 继续沿用原契约；不额外保存整区列表，也不为取得 H 再开第二份快照。 */
    @Override default long snapshot(Check check, Consumer<Route> routes, Consumer<WorldObject> objects) throws Exception {
        Objects.requireNonNull(check); Objects.requireNonNull(routes); Objects.requireNonNull(objects);
        class Receiver implements SnapshotReceiver {
            private long high = -1;
            @Override public void begin(long value) {
                if (high != -1 || value < 0) throw new IllegalStateException("Invalid snapshot watermark declaration");
                high = value;
            }
            @Override public void route(Route route) { requireStarted(); routes.accept(route); }
            @Override public void object(WorldObject object) { requireStarted(); objects.accept(object); }
            private void requireStarted() {
                if (high < 0) throw new IllegalStateException("Snapshot item precedes its watermark");
            }
        }
        var receiver = new Receiver();
        long complete = snapshot(check, receiver);
        if (complete < 0 || receiver.high != complete)
            throw new BadData("Snapshot completion differs from its declared watermark");
        return complete;
    }
}

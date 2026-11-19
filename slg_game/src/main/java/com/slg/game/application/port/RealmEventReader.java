package com.slg.game.application.port;

import com.slg.game.domain.persistence.CommittedEvent;
import com.slg.game.domain.persistence.RealmIdentity;
import java.util.List;
import java.util.Objects;

/** 读取原子业务决定的连续提交前缀；地图可在收到整页后筛选自身关注的对象。 */
public interface RealmEventReader {
    /**
     * 从 after 的下一条开始，读取不超过 limit（正数）和 budget 的完整决定。
     * 同一页的清理边界、水位与事件必须来自同一一致视图；不可按事件类型过滤后
     * 仍宣称序号连续。after 已落入清理前缀时抛 CURSOR_EXPIRED；日志空洞、顺序错误
     * 或游标超过真实水位时抛 INVALID_DATA，不得跳过缺口。返回页不得拆开一个决定。
     */
    EventPage readAfter(RealmIdentity identity, long after, int limit, ReadBudget budget)
            throws StorageAccessException;

    /** 已清理边界来自失败读取的同一视图；地图消费者不能凭 after+1 猜测真实 F。 */
    final class CursorExpired extends StorageAccessException {
        private static final long serialVersionUID = 1L;
        private final long prunedThrough;
        public CursorExpired(long prunedThrough) {
            super(Code.CURSOR_EXPIRED, "Event cursor is pruned");
            if (prunedThrough < 1) throw new IllegalArgumentException("Pruned prefix must be positive");
            this.prunedThrough = prunedThrough;
        }
        public long prunedThrough() { return prunedThrough; }
    }

    /** 游标大于同一权威读取视图中的 H；保留真实水位，不能由调用方游标推断或自动倒退。 */
    final class CursorAhead extends StorageAccessException {
        private static final long serialVersionUID = 1L;
        private final long high;
        public CursorAhead(long high) {
            super(Code.INVALID_DATA, "Event cursor is ahead of authority");
            if (high < 0) throw new IllegalArgumentException("High watermark must be nonnegative");
            this.high = high;
        }
        public long high() { return high; }
    }

    /**
     * prunedThrough 是已清理前缀的末尾；events 精确覆盖 (after, through]，high 为本页
     * 视图的已提交水位。构造时拒绝缺号、重复、倒序和虚假进度；构造成功不能代替底层
     * 的数据集核验及实际提交证明。集合被复制，返回后不跟随调用方修改。
     */
    record EventPage(RealmIdentity identity, long after, long prunedThrough, long high, long through,
                     List<CommittedEvent> events) {
        public EventPage {
            Objects.requireNonNull(identity, "identity");
            events = List.copyOf(Objects.requireNonNull(events, "events"));
            if (prunedThrough < 0 || after < prunedThrough || high < after || through < after || through > high)
                throw new IllegalArgumentException("Event page watermarks do not form a valid retained prefix");
            long expected = after;
            for (CommittedEvent event : events) {
                if (expected == Long.MAX_VALUE || event.sequence() != expected + 1)
                    throw new IllegalArgumentException("Event page must contain consecutive complete decisions");
                expected++;
            }
            if (expected != through)
                throw new IllegalArgumentException("Event page progress must equal its last event");
            if (events.isEmpty() && after < high)
                throw new IllegalArgumentException("An event page cannot report an empty prefix before high watermark");
        }
    }
}

package com.slg.game.application.background;

import java.util.LinkedHashMap;

/** 仅供协调器持有的近期退避记录；逐项淘汰不删除权威待办，下轮仍可从数据库发现。 */
final class BackgroundRetries {
    private final BackgroundPumpLimits limits;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();
    private record Entry(int failures, long after) { }

    BackgroundRetries(BackgroundPumpLimits limits) { this.limits = limits; }
    boolean eligible(String key, long now) {
        var entry = entries.get(key);
        return entry == null || now - entry.after() >= 0;
    }
    void success(String key) { entries.remove(key); }
    void fail(String key, long now) {
        var previous = entries.remove(key);
        int count = previous == null ? 1 : Math.min(31, previous.failures() + 1);
        long delay = limits.retryBase().toNanos();
        long maximum = limits.retryMax().toNanos();
        for (int i = 1; i < count && delay < maximum; i++)
            delay = delay > maximum / 2 ? maximum : Math.min(maximum, delay * 2);
        if (entries.size() >= limits.maxRetryEntries()) entries.pollFirstEntry();
        entries.put(key, new Entry(count, now + delay));
    }
    int size() { return entries.size(); }
}

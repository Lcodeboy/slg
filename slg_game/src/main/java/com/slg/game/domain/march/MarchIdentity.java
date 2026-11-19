package com.slg.game.domain.march;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 新行军 ID 在加载角色前即可计算，便于入队前完整声明行军与任务资格。v2 使用已认证账号、
 * 当前区与原请求 ID；不依赖可能来自其他建角区的永久 playerId，也不随时钟或会话变化。
 * 该算法属于权威库契约；摘要版本和输入字段不可随调用入口改变，否则原请求无法幂等重放。
 */
public final class MarchIdentity {
    private static final Pattern REQUEST = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private MarchIdentity() { }

    public static String start(int realmId, long accountId, String requestId) {
        if (realmId <= 0 || accountId <= 0 || requestId == null || !REQUEST.matcher(requestId).matches())
            throw new IllegalArgumentException("Invalid march request identity");
        String source = "march-v2\n" + realmId + "\n" + accountId + "\n" + requestId;
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(source.getBytes(StandardCharsets.US_ASCII))).substring(0, 32); }
        catch (NoSuchAlgorithmException unavailable) { throw new IllegalStateException("SHA-256 unavailable", unavailable); }
    }

    public static String arrival(String marchId) { return "arrive-" + marchId; }
    public static String gather(String marchId) { return "gather-" + marchId; }
    public static String returning(String marchId) { return "return-" + marchId; }
    public static List<String> taskIds(String marchId) { return List.of(arrival(marchId), gather(marchId), returning(marchId)); }
}

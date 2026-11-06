package com.slg.core.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.protocol.Json;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;

/**
 * 签发和校验固定版本 slg1、固定算法 HmacSHA256 的账号会话，不接受令牌自行指定算法。
 * 令牌内容只有签名保护，没有加密；当前采用无状态校验，不提供单个会话的撤销列表。
 * 每次签名使用独立 Mac，避免并发请求共享有状态的加密实例。
 */
public final class TokenService {
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private final SecretKeySpec key;
    private final Clock clock;

    /**
     * 使用显式密钥和时钟创建会话服务；密钥按 UTF-8 编码后至少 32 字节，不提供默认生产密钥。
     * 同一会话的签发方和校验方须使用一致密钥及同步时钟，clock 不能为空。
     */
    public TokenService(String secret, Clock clock) {
        if (secret == null || secret.getBytes(StandardCharsets.UTF_8).length < 32) {
            throw new IllegalArgumentException("SLG_TOKEN_SECRET must be explicitly set to at least 32 UTF-8 bytes");
        }
        this.key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * 为可信账号标识签发令牌，时间精度为秒，Duration 中不足一秒的部分会被截断。
     * 账号只接受 UC 自增正整数的规范十进制文本；有效期至少一秒，按整天截断后的值不得大于 30。
     * 此方法不查询账号是否存在，调用方必须先完成账号创建或凭证校验。
     *
     * @throws IllegalArgumentException 账号或有效期不满足签发约束
     */
    public String issue(String accountId, Duration lifetime) {
        if (!AccountIds.isValid(accountId) || lifetime == null || lifetime.isNegative()
                || lifetime.isZero() || lifetime.toSeconds() < 1 || lifetime.toDays() > 30) {
            throw new IllegalArgumentException("Invalid session parameters");
        }
        long now = clock.instant().getEpochSecond();
        SessionClaims claims = new SessionClaims(accountId, now, Math.addExact(now, lifetime.toSeconds()));
        String signed = "slg1." + ENCODER.encodeToString(Json.write(claims).getBytes(StandardCharsets.UTF_8));
        return signed + "." + ENCODER.encodeToString(sign(signed));
    }

    /**
     * 校验格式、规范 Base64 编码、签名和秒级有效期，再返回账号声明。
     * 不接受未来签发时刻，也没有过期宽限；成功只证明会话有效，不证明具体角色或资源的访问权限。
     *
     * @throws IllegalArgumentException 任意格式、签名或时效错误；统一错误不包含令牌原文和底层原因
     */
    public SessionClaims verify(String token) {
        try {
            if (token == null || token.length() > 2048) throw invalid();
            String[] parts = token.split("\\.", -1);
            if (parts.length != 3 || !parts[0].equals("slg1")) throw invalid();
            byte[] signature = DECODER.decode(parts[2]);
            // 先校验签名再解析声明，签名字节使用恒定时间比较，避免普通字符串比较泄露匹配进度。
            if (!ENCODER.encodeToString(signature).equals(parts[2])
                    || !MessageDigest.isEqual(sign(parts[0] + "." + parts[1]), signature)) throw invalid();
            byte[] payload = DECODER.decode(parts[1]);
            if (!ENCODER.encodeToString(payload).equals(parts[1])) throw invalid();
            // 校验原始 JSON 类型，不能让解码器把数值型账号转换为字符串：签名正确也须符合 UC 协议。
            JsonNode raw = Json.read(new String(payload, StandardCharsets.UTF_8), JsonNode.class);
            if (!raw.isObject() || raw.size() != 3 || !raw.path("accountId").isTextual()
                    || !integerClaim(raw.path("issuedAt")) || !integerClaim(raw.path("expiresAt"))) throw invalid();
            SessionClaims claims = new SessionClaims(raw.get("accountId").textValue(),
                    raw.get("issuedAt").longValue(), raw.get("expiresAt").longValue());
            if (!AccountIds.isValid(claims.accountId()) || claims.issuedAt() < 0 || !isCurrent(claims)) throw invalid();
            return claims;
        } catch (RuntimeException ignored) {
            throw invalid();
        }
    }

    /**
     * 仅检查已由 verify 返回的声明在本服务时钟上是否仍处于有效期内。
     * 供绑定了固定令牌的长连接在命令收发及空闲期间检查到期，不必反复解析和验签相同的字节。
     * 调用方不可传入来自客户端的未验签声明作为授权依据。
     */
    public boolean isCurrent(SessionClaims verified) {
        long now = clock.instant().getEpochSecond();
        return verified.issuedAt() <= now && now < verified.expiresAt();
    }

    /** Mac 不是可共享的请求状态，每次调用新建实例；加密提供者异常只暴露固定错误。 */
    private byte[] sign(String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(key);
            return mac.doFinal(value.getBytes(StandardCharsets.US_ASCII));
        } catch (Exception e) {
            throw new IllegalStateException("Session signing unavailable");
        }
    }

    private static boolean integerClaim(JsonNode value) {
        return value.isIntegralNumber() && value.canConvertToLong();
    }

    /** 统一所有无效会话的外部错误，既不回显输入，也不暴露验证失败发生在哪一步。 */
    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid or expired session");
    }
}

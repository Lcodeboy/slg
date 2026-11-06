package com.slg.uc;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.SecureRandom;

/**
 * 账号服务内部的密码派生工具，使用随机盐和 PBKDF2WithHmacSHA256 产生 256 位校验值。
 * 本类只负责密码派生，不做输入长度验证或并发限流，计算资源由 HTTP 边界控制。
 */
final class PasswordHasher {
    /** 新账号使用的派生次数；随账号一同持久化，旧账号登录沿用其记录中的次数。 */
    static final int ITERATIONS = 600_000;
    private static final SecureRandom RANDOM = new SecureRandom();

    /** 为每次注册生成独立的 24 字节随机盐，盐用于防止相同密码产生相同派生值。 */
    static byte[] salt() {
        byte[] bytes = new byte[24];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * 使用指定盐和迭代次数派生 32 字节结果；注册与登录共用该过程。
     * 尽量清理临时字符数组和 PBEKeySpec 中的密码，但无法擦除调用方传入的不可变 String。
     */
    static byte[] hash(String password, byte[] salt, int iterations) {
        char[] chars = password.toCharArray();
        PBEKeySpec spec = new PBEKeySpec(chars, salt, iterations, 256);
        // PBEKeySpec 已复制字符内容，先清空本地数组；其内部副本在 finally 中清理。
        java.util.Arrays.fill(chars, '\0');
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException("Password hashing unavailable");
        } finally {
            spec.clearPassword();
        }
    }
}

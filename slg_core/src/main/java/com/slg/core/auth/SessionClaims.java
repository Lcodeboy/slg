package com.slg.core.auth;

/**
 * 会话中的账号声明；记录本身不执行校验，只有通过 TokenService.verify 得到的值才可用于鉴权。
 * 不包含角色或区服授权，游戏服务仍需根据 accountId 查询角色归属。
 *
 * @param accountId 账号标识，不应直接当作客户端可指定的角色标识
 * @param issuedAt 签发时刻，自 Unix 纪元起的秒数
 * @param expiresAt 失效时刻，自 Unix 纪元起的秒数；达到该时刻即不可继续使用
 */
public record SessionClaims(String accountId, long issuedAt, long expiresAt) {}

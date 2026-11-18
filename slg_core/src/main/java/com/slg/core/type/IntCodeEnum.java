package com.slg.core.type;

/**
 * 具有稳定外部整数编码的领域枚举。code 是持久化契约，必须为正且在各自枚举内唯一；
 * 已使用的编码不更改、不复用。声明顺序、展示名称、战斗优先级都不能代替此编码。
 * 本接口不决定 JSON 或协议的现有序列化方式，具体边界由对应适配器显式映射。
 */
public interface IntCodeEnum {
    int code();
}

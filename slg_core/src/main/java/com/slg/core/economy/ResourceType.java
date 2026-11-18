package com.slg.core.economy;

import com.slg.core.type.EnumCodes;
import com.slg.core.type.IntCodeEnum;
import java.util.Map;

/**
 * 当前玩法支持的资源种类。固定 code 用于新权威库和新事件编码，禁止使用 ordinal；
 * 扩展种类分配新编码，不能复用旧编码。这里不自动改变现有运行链路的字符串载荷。
 */
public enum ResourceType implements IntCodeEnum {
    WOOD(1), FOOD(2), GEMS(3);

    private static final Map<Integer, ResourceType> BY_CODE = EnumCodes.index(values());
    private final int code;

    ResourceType(int code) { this.code = code; }

    @Override public int code() { return code; }

    /** 未识别的编码必须明确拒绝，不能默认当成木材或零资源。 */
    public static ResourceType fromCode(int code) { return EnumCodes.require(BY_CODE, code, "ResourceType"); }
}

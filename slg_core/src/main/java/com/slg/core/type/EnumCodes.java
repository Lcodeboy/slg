package com.slg.core.type;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** 纯值转换工具；在枚举初始化时发现编码冲突，避免到实际资产读取时才发生错误映射。 */
public final class EnumCodes {
    private EnumCodes() {}

    /**
     * 创建不可变索引。零/负数和重复编码均明确失败，不能靠后声明的常量覆盖旧定义。
     * 每个枚举只在自身静态初始化时调用，修改传入数组不会改变已构建的映射。
     */
    public static <E extends Enum<E> & IntCodeEnum> Map<Integer, E> index(E[] values) {
        Objects.requireNonNull(values, "values");
        var result = new HashMap<Integer, E>();
        for (E value : values) {
            Objects.requireNonNull(value, "enum value");
            if (value.code() <= 0)
                throw new IllegalArgumentException("Enum code must be positive: " + value.name());
            if (result.putIfAbsent(value.code(), value) != null)
                throw new IllegalArgumentException("Duplicate enum code: " + value.code());
        }
        return Map.copyOf(result);
    }

    /**
     * 严格解码；不把未知编号猜成默认资源、兵种或状态。后续数据库/事件适配器须把此异常
     * 分类为不支持的编码或坏数据并停止该次读取，不能返回成功的缺省资产。
     */
    public static <E extends Enum<E> & IntCodeEnum> E require(Map<Integer, E> index, int code, String typeName) {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(typeName, "typeName");
        E value = index.get(code);
        if (value == null) throw new IllegalArgumentException("Unknown " + typeName + " code: " + code);
        return value;
    }
}

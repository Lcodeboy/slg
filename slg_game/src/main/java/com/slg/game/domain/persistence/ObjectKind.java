package com.slg.game.domain.persistence;

import com.slg.core.type.EnumCodes;
import com.slg.core.type.IntCodeEnum;

import java.util.Map;

/**
 * 首批支持的结构化权威对象。code 是持久化标识，发布后不改值、不复用，不能取 ordinal；
 * 未知编码必须拒绝，不能默认映射到另一类对象。锁序是独立的调度契约，不拿 code 代替。
 * 新增类型时须同时定义表映射、编码与锁序；此枚举不表示 SQL 表名。
 */
public enum ObjectKind implements IntCodeEnum {
    PLAYER(1, 10), RESOURCE(2, 20), TROOP(3, 30), HERO(4, 40), MARCH(5, 50), GATHER_SITE(6, 60),
    TASK(7, 70), BATTLE(8, 80);

    private static final Map<Integer, ObjectKind> BY_CODE = EnumCodes.index(values());
    private final int code;
    private final int lockOrder;
    ObjectKind(int code, int lockOrder) {
        this.code = code;
        this.lockOrder = lockOrder;
    }
    @Override public int code() { return code; }
    public int lockOrder() { return lockOrder; }
    public static ObjectKind fromCode(int code) { return EnumCodes.require(BY_CODE, code, "ObjectKind"); }
}

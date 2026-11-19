package com.slg.game.domain.persistence;

import com.slg.core.type.IntCodeEnum;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 仅校验值边界，不引入编码、存储或业务规则。 */
final class PersistenceChecks {
    private PersistenceChecks() {}

    static void identifier(String value, String name, int maximumLength) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty() || value.length() > maximumLength)
            throw new IllegalArgumentException(name + " has an invalid length");
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character < 33 || character > 126)
                throw new IllegalArgumentException(name + " must contain printable ASCII without spaces");
        }
    }

    static void jsonText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }

    static void positive(long value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
    }

    static void nonNegative(long value, String name) {
        if (value < 0) throw new IllegalArgumentException(name + " must not be negative");
    }

    static void coordinate(double value, String name) {
        if (!Double.isFinite(value) || value < 0)
            throw new IllegalArgumentException(name + " must be finite and nonnegative");
    }

    static void optionalIdentifier(String value, String name, int maximumLength) {
        if (value != null) identifier(value, name, maximumLength);
    }

    /** 只接受带固定编码的枚举键；复制后不再跟随生产者修改，负数量在进入提交边界前拒绝。 */
    static <E extends Enum<E> & IntCodeEnum> Map<E, Long> amounts(Map<E, Long> values, String name) {
        var copy = Map.copyOf(Objects.requireNonNull(values, name));
        copy.forEach((type, amount) -> nonNegative(amount, name + " amount"));
        return copy;
    }

    static List<String> identifiers(List<String> values, String name) {
        var copy = List.copyOf(Objects.requireNonNull(values, name));
        var unique = new HashSet<String>();
        for (String value : copy) {
            identifier(value, name + " entry", 256);
            if (!unique.add(value)) throw new IllegalArgumentException(name + " contains duplicate IDs");
        }
        return copy;
    }
}

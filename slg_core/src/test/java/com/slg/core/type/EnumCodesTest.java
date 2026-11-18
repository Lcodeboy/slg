package com.slg.core.type;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EnumCodesTest {
    @Test void indexUsesExplicitCodesEvenWhenDeclarationOrderDiffers() {
        var index = EnumCodes.index(Reordered.values());
        assertEquals(Map.of(20, Reordered.FIRST, 10, Reordered.SECOND), index);
        assertSame(Reordered.SECOND, EnumCodes.require(index, 10, "Reordered"));
        assertThrows(IllegalArgumentException.class, () -> EnumCodes.require(index, 0, "Reordered"));
        assertThrows(IllegalArgumentException.class, () -> EnumCodes.require(index, 1, "Reordered"));
    }

    @Test void indexIsImmutableAndIndependentOfInputArray() {
        Reordered[] values = Reordered.values();
        var index = EnumCodes.index(values);
        values[0] = null;
        assertSame(Reordered.FIRST, index.get(20));
        assertThrows(UnsupportedOperationException.class, () -> index.put(30, Reordered.FIRST));
    }

    @Test void duplicateCodesFailInsteadOfSilentlyOverwriting() {
        assertThrows(IllegalArgumentException.class, () -> EnumCodes.index(Duplicate.values()));
    }

    @Test void zeroAndNegativeCodesCannotBecomePersistenceMappings() {
        assertThrows(IllegalArgumentException.class, () -> EnumCodes.index(new Invalid[]{Invalid.ZERO}));
        assertThrows(IllegalArgumentException.class, () -> EnumCodes.index(new Invalid[]{Invalid.NEGATIVE}));
    }

    @Test void missingEnumDefinitionsAreRejected() {
        assertThrows(NullPointerException.class, () -> EnumCodes.index((Reordered[]) null));
        assertThrows(NullPointerException.class, () -> EnumCodes.index(new Reordered[]{null}));
    }

    private enum Reordered implements IntCodeEnum {
        FIRST(20), SECOND(10);
        private final int code;
        Reordered(int code) { this.code = code; }
        @Override public int code() { return code; }
    }

    private enum Duplicate implements IntCodeEnum {
        FIRST, SECOND;
        @Override public int code() { return 1; }
    }

    private enum Invalid implements IntCodeEnum {
        ZERO(0), NEGATIVE(-1);
        private final int code;
        Invalid(int code) { this.code = code; }
        @Override public int code() { return code; }
    }
}

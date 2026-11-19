package com.slg.game.domain.player;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PlayerIdentityTest {
    @Test void canonicalIdentityPreservesLargeAccountAndFoundingRealmExactly() {
        String id = PlayerIdentity.atCreation(17, PlayerTestData.ACCOUNT);
        assertEquals("17_9007199254740993", id);
        assertEquals(new PlayerIdentity.Components(17, PlayerTestData.ACCOUNT), PlayerIdentity.parse(id));
        assertEquals(PlayerTestData.ACCOUNT, PlayerIdentity.accountOf(id));
        assertEquals(new PlayerIdentity.Components(Integer.MAX_VALUE, Long.MAX_VALUE),
                PlayerIdentity.parse(PlayerIdentity.atCreation(Integer.MAX_VALUE, Long.MAX_VALUE)));
    }

    @Test void noncanonicalTextAndOverflowCannotCreateAnotherQualificationForTheSameAccount() {
        for (String invalid : List.of("", "1", "_1", "1_", "1_1_1", "01_1", "1_01", "+1_1", "1_+1",
                "-1_1", "1_-1", "0_1", "1_0", "1_1.0", "1_1e2", "1_ 1", "1_1\n", "１_1", "1_１",
                "2147483648_1", "1_9223372036854775808", "12345678901_1"))
            assertThrows(IllegalArgumentException.class, () -> PlayerIdentity.parse(invalid), invalid);
    }

    @Test void missingOrNonpositiveIdentityComponentsAreRejected() {
        assertThrows(NullPointerException.class, () -> PlayerIdentity.parse(null));
        assertThrows(IllegalArgumentException.class, () -> PlayerIdentity.atCreation(0, 1));
        assertThrows(IllegalArgumentException.class, () -> PlayerIdentity.atCreation(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new PlayerIdentity.Components(-1, 1));
    }
}

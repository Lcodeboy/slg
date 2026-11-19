package com.slg.game.domain.player;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.domain.GameException;
import com.slg.game.domain.persistence.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.slg.game.domain.player.PlayerTestData.*;
import static org.junit.jupiter.api.Assertions.*;

class PlayerCreationTest {
    @Test void initialSixRowsPreserveExistingGameDefaultsWithoutInventingResourcesOrMarches() {
        var rows = PlayerCreation.initialStates(REALM, ACCOUNT, " Alice ");
        assertEquals(6, rows.size());
        var profile = assertInstanceOf(PlayerState.class, rows.getFirst());
        assertEquals(PLAYER, profile.playerId()); assertEquals(ACCOUNT, profile.accountId());
        assertEquals(17, profile.foundingRealmId()); assertEquals(17, profile.currentRealmId());
        assertEquals("Alice", profile.name());
        assertEquals(List.of(new ResourceState(PLAYER, ResourceType.WOOD, 0)), rows.stream().filter(ResourceState.class::isInstance).toList());
        var troops = rows.stream().filter(TroopState.class::isInstance).map(TroopState.class::cast).toList();
        assertEquals(List.of(new TroopState(PLAYER, TroopType.INFANTRY, 3000, 0, 0),
                new TroopState(PLAYER, TroopType.LANCER, 1000, 0, 0), new TroopState(PLAYER, TroopType.MARKSMAN, 1000, 0, 0)), troops);
        assertEquals(new HeroState(PLAYER, "sergey", null), rows.getLast());
        assertFalse(rows.stream().anyMatch(MarchState.class::isInstance));
        assertEquals(PlayerCreation.initialKeys(REALM, ACCOUNT), new HashSet<>(rows.stream().map(ObjectState::key).toList()));
        assertThrows(UnsupportedOperationException.class, rows::clear);
    }

    @Test void creationProposesOnlyZeroExpectedVersionsAndDoesNotPublishCommittedState() {
        var before = PlayerSnapshot.absent(REALM, ACCOUNT, 50);
        var changes = PlayerCreation.create(before, "Alice");
        assertEquals(6, changes.size());
        assertTrue(changes.stream().allMatch(change -> change.expectedVersion() == 0));
        assertEquals(PlayerCreation.initialStates(REALM, ACCOUNT, "Alice"), changes.stream().map(ObjectChange::afterState).toList());
        assertTrue(before.profile().isEmpty());
        assertEquals(50, before.highSequence());
        assertThrows(UnsupportedOperationException.class, changes::clear);
    }

    @Test void existingPlayerProducesTypedBusinessRejectionAndDoesNotRegrantInitialAssets() {
        var existing = snapshot();
        var failure = assertThrows(GameException.class, () -> PlayerCreation.create(existing, "Replacement"));
        assertEquals("PLAYER_EXISTS", failure.code());
        assertEquals("Alice", existing.profileState().orElseThrow().name());
        assertEquals(7, existing.profile().orElseThrow().version());
    }

    @Test void nicknameRulesKeepStripUtf16LengthAndControlCharacterBehavior() {
        assertEquals("Alice", PlayerCreation.normalizedName(" \nAlice\t "));
        assertEquals("x".repeat(24), PlayerCreation.normalizedName("x".repeat(24)));
        assertEquals("😀".repeat(12), PlayerCreation.normalizedName("😀".repeat(12)));
        for (String invalid : List.of("", " \t\n", "x".repeat(25), "😀".repeat(13), "A\nB", "A\u007fB"))
            assertEquals("INVALID_ARGUMENT", assertThrows(GameException.class,
                    () -> PlayerCreation.normalizedName(invalid)).code());
        assertEquals("INVALID_ARGUMENT", assertThrows(GameException.class, () -> PlayerCreation.normalizedName(null)).code());
        // 旧规则先验证昵称再判断已建角；改变这一顺序会改变同一个非法请求的业务响应。
        assertEquals("INVALID_ARGUMENT", assertThrows(GameException.class, () -> PlayerCreation.create(snapshot(), " ")).code());
    }
}

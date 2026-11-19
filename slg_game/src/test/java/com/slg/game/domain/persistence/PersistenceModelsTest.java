package com.slg.game.domain.persistence;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.slg.core.economy.ResourceType.WOOD;
import static org.junit.jupiter.api.Assertions.*;

class PersistenceModelsTest {
    private static final ObjectKey PLAYER = new ObjectKey.Player("17_100086");
    private static final ObjectKey MINE = new ObjectKey.GatherSite(150, 150);
    private static final RequestKey REQUEST = new RequestKey("timer:17_100086", "march.arrive.1");

    @Test void capturesCollectionsBeforeTheyCanBeChangedByTheProducer() {
        var change = new ObjectChange(PLAYER, 3, player());
        var changes = new ArrayList<>(List.of(change));
        var reads = new HashMap<>(Map.of(PLAYER, 3L, MINE, 2L));
        var decision = decision(changes, reads);

        changes.clear();
        reads.put(PLAYER, 4L);
        reads.remove(MINE);

        assertEquals(List.of(change), decision.changes());
        assertEquals(Map.of(PLAYER, 3L, MINE, 2L), decision.readVersions());
        assertThrows(UnsupportedOperationException.class, () -> decision.changes().clear());
        assertThrows(UnsupportedOperationException.class, () -> decision.readVersions().put(MINE, 5L));
    }

    @Test void rejectsRepeatedChangesEvenWhenTheyUseSeparateEqualKeyInstances() {
        var samePlayer = new ObjectKey.Player("17_100086");
        assertThrows(IllegalArgumentException.class, () -> decision(List.of(
                new ObjectChange(PLAYER, 3, player()), new ObjectChange(samePlayer, 4, player())), Map.of()));
    }

    @Test void requiresReadAndWriteExpectationsToAgreeIncludingCreation() {
        assertDoesNotThrow(() -> decision(List.of(new ObjectChange(PLAYER, 3, player())), Map.of(PLAYER, 3L)));
        assertDoesNotThrow(() -> decision(List.of(new ObjectChange(MINE, 0, site())), Map.of(MINE, 0L)));
        assertThrows(IllegalArgumentException.class, () -> decision(
                List.of(new ObjectChange(PLAYER, 3, player())), Map.of(PLAYER, 2L)));
        assertThrows(IllegalArgumentException.class, () -> decision(
                List.of(new ObjectChange(MINE, 0, site())), Map.of(MINE, 1L)));
    }

    @Test void preservesReadOnlyDependenciesForDecisionsWithoutStateChanges() {
        var decision = decision(List.of(), Map.of(PLAYER, Long.MAX_VALUE, MINE, 0L));
        assertTrue(decision.changes().isEmpty());
        assertEquals(Map.of(PLAYER, Long.MAX_VALUE, MINE, 0L), decision.readVersions());
        assertThrows(IllegalArgumentException.class, () -> decision(List.of(), Map.of(PLAYER, -1L)));
    }

    @Test void distinguishesAbsentExpectedVersionsFromExistingStatesAndChecksOverflow() {
        assertEquals(0, new ObjectChange(PLAYER, 0, player()).expectedVersion());
        assertEquals(Long.MAX_VALUE - 1, new ObjectChange(PLAYER, Long.MAX_VALUE - 1, player()).expectedVersion());
        assertEquals(Long.MAX_VALUE, new VersionedState(Long.MAX_VALUE, player()).version());
        assertThrows(IllegalArgumentException.class, () -> new ObjectChange(PLAYER, -1, player()));
        assertThrows(IllegalArgumentException.class, () -> new ObjectChange(PLAYER, Long.MAX_VALUE, player()));
        assertThrows(IllegalArgumentException.class, () -> new VersionedState(0, player()));
        assertThrows(IllegalArgumentException.class, () -> new VersionedState(-1, player()));
    }

    @Test void rejectsMissingCollectionElementsAndReadVersions() {
        var changes = new ArrayList<ObjectChange>();
        changes.add(null);
        assertThrows(NullPointerException.class, () -> decision(changes, Map.of()));
        var reads = new HashMap<ObjectKey, Long>();
        reads.put(PLAYER, null);
        assertThrows(NullPointerException.class, () -> decision(List.of(), reads));
        reads.clear();
        reads.put(null, 1L);
        assertThrows(NullPointerException.class, () -> decision(List.of(), reads));
        assertThrows(NullPointerException.class, () -> decision(null, Map.of()));
        assertThrows(NullPointerException.class, () -> decision(List.of(), null));
    }

    @Test void keepsDatasetAndWriterIdentitySeparateAndRequiresPositiveEpochs() {
        UUID dataset = UUID.randomUUID(), instance = UUID.randomUUID();
        var realm = new RealmIdentity(17, dataset);
        var token = new WriterToken(realm, instance, 1);
        assertEquals(realm, token.realm());
        assertEquals(instance, token.instanceId());
        assertNotEquals(realm, new RealmIdentity(17, UUID.randomUUID()));
        assertNotEquals(token, new WriterToken(realm, instance, 2));
        assertThrows(IllegalArgumentException.class, () -> new RealmIdentity(0, dataset));
        assertThrows(NullPointerException.class, () -> new RealmIdentity(17, null));
        assertThrows(IllegalArgumentException.class, () -> new WriterToken(realm, instance, 0));
        assertThrows(NullPointerException.class, () -> new WriterToken(null, instance, 1));
        assertThrows(NullPointerException.class, () -> new WriterToken(realm, null, 1));
    }

    @Test void preservesSystemRequestIdsAndEnforcesIdentifierBoundaries() {
        assertEquals("march.arrive.1", REQUEST.requestId());
        assertDoesNotThrow(() -> new RequestKey("s".repeat(256), "r".repeat(128)));
        assertDoesNotThrow(() -> new ObjectKey.Resource("p".repeat(256), WOOD));
        assertDoesNotThrow(() -> new ObjectKey.Hero("p".repeat(256), "h".repeat(256)));
        assertThrows(IllegalArgumentException.class, () -> new RequestKey("s".repeat(257), "request"));
        assertThrows(IllegalArgumentException.class, () -> new RequestKey("scope", "r".repeat(129)));
        assertThrows(IllegalArgumentException.class, () -> new ObjectKey.Resource("p".repeat(257), WOOD));
        assertThrows(IllegalArgumentException.class, () -> new ObjectKey.Hero("player", "h".repeat(257)));
        assertThrows(IllegalArgumentException.class, () -> new ObjectKey.Player("i".repeat(257)));
        for (String invalid : List.of("", "has space", "line\nbreak", "角色", "\u007f")) {
            assertThrows(IllegalArgumentException.class, () -> new RequestKey(invalid, "request"));
            assertThrows(IllegalArgumentException.class, () -> new ObjectKey.Player(invalid));
        }
        assertNotEquals(new RequestKey("scope", "task"), new RequestKey("scope", "TASK"));
    }

    @Test void keepsJsonOnlyInAuxiliaryPayloadsWithoutPretendingToValidateSyntax() {
        String text = "  codec-must-check-this  ";
        var payload = new JsonPayload(1, text);
        assertEquals(text, payload.json());
        var decision = new CommitDecision(REQUEST, "digest", "FACT", text, text, List.of(), Map.of());
        assertEquals(text, decision.factJson());
        assertEquals(text, decision.resultJson());
        assertThrows(IllegalArgumentException.class, () -> new JsonPayload(0, "{}"));
        assertThrows(IllegalArgumentException.class, () -> new JsonPayload(1, " "));
        assertThrows(NullPointerException.class, () -> new VersionedState(1, null));
        assertThrows(NullPointerException.class, () -> new ObjectChange(PLAYER, 0, null));
    }

    @Test void boundsReceiptAndEventSequencesWithoutChangingTheOriginalDecision() {
        var decision = decision(List.of(new ObjectChange(PLAYER, 0, player())), Map.of());
        var event = new CommittedEvent(7, decision);
        var receipt = new CommitReceipt(REQUEST, 7, decision.digest(), decision.resultJson());
        assertSame(decision, event.decision());
        assertEquals(receipt.request(), event.decision().request());
        assertEquals(7, event.sequence());
        assertEquals(7, receipt.sequence());
        assertDoesNotThrow(() -> new CommittedEvent(Long.MAX_VALUE, decision));
        assertThrows(IllegalArgumentException.class, () -> new CommittedEvent(0, decision));
        assertThrows(IllegalArgumentException.class, () -> new CommitReceipt(REQUEST, -1, "digest", "{}"));
        assertThrows(NullPointerException.class, () -> new CommittedEvent(1, null));
    }

    @Test void validatesDecisionAndReceiptMetadataWithoutRestrictingDigestAlgorithms() {
        assertDoesNotThrow(() -> new CommitDecision(REQUEST, "d".repeat(128), "f".repeat(128),
                "{}", "{}", List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new CommitDecision(REQUEST, "d".repeat(129),
                "FACT", "{}", "{}", List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new CommitDecision(REQUEST, "digest",
                "f".repeat(129), "{}", "{}", List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new CommitDecision(REQUEST, "digest",
                "FACT", " ", "{}", List.of(), Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new CommitReceipt(REQUEST, 1, "digest", " "));
        assertThrows(NullPointerException.class, () -> new CommitReceipt(null, 1, "digest", "{}"));
    }

    private static PlayerState player() {
        return new PlayerState("17_100086", 100086, 17, 17, "玩家");
    }

    private static GatherSiteState site() { return new GatherSiteState(150, 150, null, null, 0); }

    private static CommitDecision decision(List<ObjectChange> changes, Map<ObjectKey, Long> reads) {
        return new CommitDecision(REQUEST, "input-digest", "MARCH_ARRIVE", "{}", "{}", changes, reads);
    }
}

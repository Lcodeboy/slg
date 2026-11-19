package com.slg.game.application.execution;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.commit.CommitRequest;
import com.slg.game.domain.persistence.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class OperationScopeTest {
    private static final RequestKey REQUEST = new RequestKey("client", "request");

    @Test void everyCanonicalOwnedAssetAutomaticallyQualifiesItsStableAccount() {
        for (ObjectKey asset : List.of(new ObjectKey.Resource("1_7", ResourceType.WOOD),
                new ObjectKey.Troop("1_7", TroopType.INFANTRY), new ObjectKey.Hero("1_7", "hero"))) {
            assertEquals(Set.of(7L), scope(Set.of(asset)).accounts());
            assertDoesNotThrow(() -> scope(Set.of(asset, new ObjectKey.Player("1_7"))));
        }
    }

    @Test void explicitAccountCoversAllDirectMembersButNeverAddsUnannouncedWorldObjects() {
        var scope = new OperationScope(REQUEST, Set.of(), CommitRequest.Origin.CLIENT,
                Duration.ofSeconds(1), Duration.ofSeconds(1), Set.of(7L));
        assertDoesNotThrow(() -> scope.validate(decision(List.of(), Map.of(
                new ObjectKey.Player("2_7"), 0L,
                new ObjectKey.Resource("2_7", ResourceType.WOOD), 0L,
                new ObjectKey.Troop("2_7", TroopType.INFANTRY), 0L,
                new ObjectKey.Hero("2_7", "sergey"), 0L))));
        assertThrows(IllegalArgumentException.class, () -> scope.validate(
                decision(List.of(), Map.of(new ObjectKey.Player("2_8"), 0L))));
        assertThrows(IllegalArgumentException.class, () -> scope.validate(
                decision(List.of(), Map.of(new ObjectKey.March("march"), 0L))));
        assertThrows(IllegalArgumentException.class, () -> scope.validate(
                decision(List.of(), Map.of(new ObjectKey.Player("02_7"), 0L))));
    }

    @Test void accountQualificationIsImmutablePositiveAndCountedBeforeAdmission() {
        var accounts = new HashSet<Long>(Set.of(7L));
        var scope = new OperationScope(REQUEST, Set.of(), CommitRequest.Origin.CLIENT,
                Duration.ofSeconds(1), Duration.ofSeconds(1), accounts);
        accounts.add(8L);
        assertEquals(Set.of(7L), scope.accounts());
        assertThrows(UnsupportedOperationException.class, () -> scope.accounts().clear());
        assertEquals(8L, scope.metadataBytes() - scope(Set.of()).metadataBytes());
        assertThrows(IllegalArgumentException.class, () -> new OperationScope(REQUEST, Set.of(), CommitRequest.Origin.CLIENT,
                Duration.ZERO, Duration.ZERO, Set.of(0L)));
    }

    @Test void postPreparationValidationCoversReadOnlyDependenciesAndInferredOwners() {
        var march = new MarchState("march", "1_7", MarchState.Status.COMPLETED, MarchState.Mission.GATHER,
                0, 0, 1, 1, 0, 1, 1, Map.of(), List.of(), null, "rules", Map.of());
        var decision = decision(List.of(new ObjectChange(march.key(), 1, march)), Map.of());
        assertThrows(IllegalArgumentException.class, () -> scope(Set.of(march.key())).validate(decision));
        assertDoesNotThrow(() -> scope(Set.of(march.key(), new ObjectKey.Player("1_7"))).validate(decision));
        assertThrows(IllegalArgumentException.class, () -> scope(Set.of()).validate(
                decision(List.of(), Map.of(new ObjectKey.GatherSite(1, 2), 0L))));
    }

    @Test void requestIdentityCannotChangeAfterAcquiringQualification() {
        var other = new CommitDecision(new RequestKey("other", "request"), "digest", "FACT", "{}", "{}", List.of(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> scope(Set.of()).validate(other));
    }

    @Test void committedReceiptMustMatchBothQualifiedRequestAndPreparedDigest() {
        var scope = scope(Set.of());
        var decision = decision(List.of(), Map.of());
        assertDoesNotThrow(() -> scope.validateReceipt(decision, new CommitReceipt(REQUEST, 1, "digest", "{}")));
        assertThrows(IllegalArgumentException.class,
                () -> scope.validateReceipt(decision, new CommitReceipt(REQUEST, 1, "other", "{}")));
        assertThrows(IllegalArgumentException.class, () -> scope.validateReceipt(decision,
                new CommitReceipt(new RequestKey("scope", "other"), 1, "digest", "{}")));
    }

    @Test void scopeAndPayloadTakeDefensiveCopiesBeforeQueueing() {
        var keys = new HashSet<ObjectKey>(Set.of(new ObjectKey.Player("1_7")));
        var scope = scope(keys);
        keys.clear();
        assertEquals(Set.of(new ObjectKey.Player("1_7")), scope.objects());
        assertThrows(UnsupportedOperationException.class, scope.objects()::clear);
        byte[] bytes = {1, 2, 3};
        var command = new OperationCommand(scope, "TYPE", bytes);
        long charged = command.retainedBytes();
        bytes[0] = 9;
        command.payload()[1] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, command.payload());
        assertEquals(charged, command.retainedBytes());
        assertEquals(100, new OperationCommand(scope, "TYPE", new byte[103]).retainedBytes() - charged);
    }

    private static OperationScope scope(Set<ObjectKey> keys) {
        return new OperationScope(REQUEST, keys, CommitRequest.Origin.CLIENT, Duration.ofSeconds(1), Duration.ofSeconds(1));
    }
    private static CommitDecision decision(List<ObjectChange> changes, Map<ObjectKey, Long> reads) {
        return new CommitDecision(REQUEST, "digest", "FACT", "{}", "{}", changes, reads);
    }
}

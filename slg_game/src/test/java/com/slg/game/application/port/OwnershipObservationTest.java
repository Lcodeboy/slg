package com.slg.game.application.port;

import com.slg.game.domain.persistence.RealmIdentity;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OwnershipObservationTest {
    private final RealmIdentity realm = new RealmIdentity(1, UUID.randomUUID());

    @Test void initializationReleasedAndExpiredOwnersRemainDistinct() {
        assertEquals(0, new RealmOwnership.OwnershipObservation(realm, 0, null, false, false).epoch());
        assertEquals(7, new RealmOwnership.OwnershipObservation(realm, 7, null, false, false).epoch());
        UUID instance = UUID.randomUUID();
        var expired = new RealmOwnership.OwnershipObservation(realm, 7, instance, true, false);
        assertTrue(expired.active()); assertFalse(expired.leaseValid()); assertEquals(instance, expired.instanceId());
        assertTrue(new RealmOwnership.OwnershipObservation(realm, 7, instance, true, true).leaseValid());
    }

    @Test void incompleteOrContradictoryObservationsCannotPretendToDescribeAWriter() {
        UUID instance = UUID.randomUUID();
        assertThrows(NullPointerException.class, () -> new RealmOwnership.OwnershipObservation(null, 0, null, false, false));
        assertThrows(IllegalArgumentException.class, () -> new RealmOwnership.OwnershipObservation(realm, -1, null, false, false));
        assertThrows(IllegalArgumentException.class, () -> new RealmOwnership.OwnershipObservation(realm, 0, instance, true, true));
        assertThrows(IllegalArgumentException.class, () -> new RealmOwnership.OwnershipObservation(realm, 1, null, true, false));
        assertThrows(IllegalArgumentException.class, () -> new RealmOwnership.OwnershipObservation(realm, 1, instance, false, false));
        assertThrows(IllegalArgumentException.class, () -> new RealmOwnership.OwnershipObservation(realm, 1, null, false, true));
    }
}

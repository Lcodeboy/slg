package com.slg.game.infrastructure.mysql;

import com.slg.game.application.port.RealmOwnership;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.infrastructure.mysql.mapper.RealmMetadataMapper;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** 观察只读当前控制状态，不替代数据库屏障，也不把丢失确认后猜到的任期变成 token。 */
@Timeout(30)
class MySqlOwnershipObservationIT extends AuthoritySchemaFixture {
    private MySqlRealmOwnership ownership() { return new MySqlRealmOwnership(sessions, Duration.ofSeconds(30)); }

    @Test void initializedRealmHasAnExplicitEmptyObservationWithoutAcquiringOwnership() throws Exception {
        assertEquals(new RealmOwnership.OwnershipObservation(identity, 0, null, false, false), ownership().observe(identity));
        assertEquals(0, scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        assertEquals(0, scalar("SELECT expires_at_millis FROM realm_owner_lease WHERE realm_id=1"));
    }

    @Test void acquiredAndFencedEpochsAreObservedWithoutRenewingEitherLease() throws Exception {
        var ownership = ownership(); var writer = ownership.acquire(identity, UUID.randomUUID());
        long expiry = scalar("SELECT expires_at_millis FROM realm_owner_lease WHERE realm_id=1");
        assertEquals(new RealmOwnership.OwnershipObservation(identity, writer.epoch(), writer.instanceId(), true, true),
                ownership.observe(identity));
        assertEquals(expiry, scalar("SELECT expires_at_millis FROM realm_owner_lease WHERE realm_id=1"));
        var replacement = ownership.fence(writer);
        assertEquals(new RealmOwnership.OwnershipObservation(identity, replacement.epoch(), writer.instanceId(), true, true),
                ownership.observe(identity));
        assertFalse(ownership.renew(writer));
    }

    @Test void expiredLeaseRemainsActiveMetadataAndIsNotMistakenForAReleasedWriter() throws Exception {
        var ownership = ownership(); var writer = ownership.acquire(identity, UUID.randomUUID());
        expireLease();
        var observed = ownership.observe(identity);
        assertTrue(observed.active()); assertFalse(observed.leaseValid());
        assertEquals(writer.epoch(), observed.epoch()); assertEquals(writer.instanceId(), observed.instanceId());
        assertEquals(writer.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
    }

    @Test void releaseKeepsEpochButClearsInstanceAndLeaseValidity() throws Exception {
        var ownership = ownership(); var writer = ownership.acquire(identity, UUID.randomUUID());
        ownership.release(writer);
        assertEquals(new RealmOwnership.OwnershipObservation(identity, writer.epoch(), null, false, false), ownership.observe(identity));
    }

    @Test void foreignTakeoverIsVisibleWithoutTryingToAcquireItBack() throws Exception {
        var ownership = ownership(); var old = ownership.acquire(identity, UUID.randomUUID()); expireLease();
        var foreign = ownership.acquire(identity, UUID.randomUUID());
        var observed = ownership.observe(identity);
        assertTrue(observed.epoch() > old.epoch()); assertNotEquals(old.instanceId(), observed.instanceId());
        assertEquals(foreign.instanceId(), observed.instanceId()); assertTrue(observed.leaseValid());
    }

    @Test void wrongDatasetAndMissingOrMismatchedLeaseFailInsteadOfReturningAnEmptyOwner() throws Exception {
        var ownership = ownership();
        assertEquals(StorageAccessException.Code.IDENTITY_MISMATCH, assertThrows(StorageAccessException.class,
                () -> ownership.observe(new RealmIdentity(identity.realmId(), UUID.randomUUID()))).code());
        ownership.acquire(identity, UUID.randomUUID());
        execute("UPDATE realm_owner_lease SET writer_epoch=writer_epoch+1 WHERE realm_id=1");
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> ownership.observe(identity)).code());
        execute("DELETE FROM realm_owner_lease WHERE realm_id=1");
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> ownership.observe(identity)).code());
    }

    @Test void observationWaitsForOwnerChangeAndThenReadsItsMatchingCurrentLease() throws Exception {
        var ownership = ownership(); var writer = ownership.acquire(identity, UUID.randomUUID());
        UUID replacement = UUID.randomUUID();
        try (var changing = sessions.openWrite(); var executor = Executors.newSingleThreadExecutor()) {
            var mapper = changing.getMapper(RealmMetadataMapper.class);
            mapper.lockOwnerExclusive(identity.realmId()); mapper.lockLease(identity.realmId());
            assertEquals(1, mapper.updateOwner(identity.realmId(), writer.epoch(), writer.instanceId().toString(), true,
                    writer.epoch() + 1, replacement.toString(), true));
            assertEquals(1, mapper.updateLease(identity.realmId(), writer.epoch() + 1, replacement.toString(), 30_000));
            var started = new CountDownLatch(1);
            var observation = executor.submit(() -> { started.countDown(); return ownership.observe(identity); });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> observation.get(150, TimeUnit.MILLISECONDS));
            changing.commit(true);
            assertEquals(new RealmOwnership.OwnershipObservation(identity, writer.epoch() + 1, replacement, true, true),
                    observation.get(3, TimeUnit.SECONDS));
        }
    }

    @Test void matchingExpiredOwnerCanRecoverThroughAConditionalExclusiveBarrier() throws Exception {
        var ownership = ownership(); var old = ownership.acquire(identity, UUID.randomUUID());
        expireLease();
        var observed = ownership.observe(identity);
        assertTrue(observed.active()); assertFalse(observed.leaseValid());
        var recovered = ownership.reacquire(observed);
        assertEquals(old.instanceId(), recovered.instanceId()); assertEquals(old.epoch() + 1, recovered.epoch());
        assertFalse(ownership.renew(old));
        assertEquals(new RealmOwnership.OwnershipObservation(identity, recovered.epoch(), recovered.instanceId(), true, true),
                ownership.observe(identity));
    }

    @Test void recoveryDoesNotTrustAnObservationClaimThatTheCurrentLeaseIsExpired() throws Exception {
        var ownership = ownership(); var writer = ownership.acquire(identity, UUID.randomUUID());
        var live = ownership.observe(identity);
        assertThrows(IllegalArgumentException.class, () -> ownership.reacquire(live));
        // record不是授权证据：即使调用者构造leaseValid=false，也必须由数据库当前读重新判定。
        var claimedExpired = new RealmOwnership.OwnershipObservation(identity, writer.epoch(), writer.instanceId(), true, false);
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.reacquire(claimedExpired)).code());
        assertEquals(writer.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
        ownership.release(writer);
        assertThrows(IllegalArgumentException.class, () -> ownership.reacquire(ownership.observe(identity)));
    }

    @Test void foreignAcquireAndReleaseBetweenObservationAndRecoveryCannotBeOverwritten() throws Exception {
        var ownership = ownership(); var old = ownership.acquire(identity, UUID.randomUUID());
        expireLease(); var observed = ownership.observe(identity);
        var foreign = ownership.acquire(identity, UUID.randomUUID()); ownership.release(foreign);
        // 观察后的foreign已经完成一次接管并清空身份。普通acquire此时可成功，恢复专用CAS必须拒绝。
        assertEquals(StorageAccessException.Code.OWNERSHIP_CONFLICT,
                assertThrows(StorageAccessException.class, () -> ownership.reacquire(observed)).code());
        assertEquals(new RealmOwnership.OwnershipObservation(identity, foreign.epoch(), null, false, false),
                ownership.observe(identity));
        assertEquals(foreign.epoch(), scalar("SELECT writer_epoch FROM realm_owner WHERE realm_id=1"));
    }

    private void expireLease() throws Exception {
        execute("UPDATE realm_owner_lease SET expires_at_millis="
                + "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)-1 WHERE realm_id=1");
    }
}

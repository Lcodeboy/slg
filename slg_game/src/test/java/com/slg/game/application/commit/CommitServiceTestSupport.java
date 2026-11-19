package com.slg.game.application.commit;

import com.slg.game.application.port.RealmOwnership;
import com.slg.game.application.port.RealmTransaction;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** 可控端口只用于生命周期竞争测试；真实锁、提交与断链另由 MySqlAsyncCommitIT 验证。 */
final class CommitServiceTestSupport {
    static final WriterToken WRITER = new WriterToken(new RealmIdentity(1, UUID.randomUUID()), UUID.randomUUID(), 1);
    private CommitServiceTestSupport() {}

    static CommitQueueLimits limits(int tasks, long bytes, int reservedTasks, long reservedBytes, int databases, int notifications) {
        return new CommitQueueLimits(tasks, bytes, reservedTasks, reservedBytes, databases, notifications,
                Duration.ofSeconds(10), Duration.ofMillis(10), Duration.ofMinutes(10));
    }
    static CommitRequest request(String id) { return request(id, CommitRequest.Origin.CLIENT, Duration.ofSeconds(5)); }
    static CommitRequest request(String id, CommitRequest.Origin origin, Duration wait) {
        return new CommitRequest(WRITER, new CommitDecision(new RequestKey("test", id), "digest-" + id,
                "TEST", "{}", "{\"ok\":true}", List.of(), Map.of()), origin, wait);
    }
    static CommitResult result(CommitHandle handle) throws Exception { return handle.settlement().toCompletableFuture().get(5, TimeUnit.SECONDS); }
    static CommitResult.Reason reason(CommitHandle handle) throws Exception {
        return assertInstanceOf(CommitResult.NotCommitted.class, result(handle)).reason();
    }
    static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "Timed out waiting for lifecycle transition");
    }
    static void gate(CountDownLatch latch) throws StorageAccessException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw unavailable();
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw unavailable(); }
    }
    static StorageAccessException unavailable() {
        return new StorageAccessException(StorageAccessException.Code.UNAVAILABLE, "Injected storage failure");
    }
    static void drain(BoundedRealmCommitService service) throws Exception {
        service.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals(CommitQueueSnapshot.Mode.CLOSED, service.snapshot().mode());
    }

    @FunctionalInterface interface CommitAction {
        RealmTransaction.CommitAttempt run(WriterToken writer, CommitDecision decision) throws StorageAccessException;
    }
    @FunctionalInterface interface ResolveAction {
        RealmTransaction.CommitAttempt run(WriterToken previous, WriterToken next, RequestKey key, String digest) throws StorageAccessException;
    }
    record Resolution(WriterToken previous, WriterToken replacement, RequestKey request) {}

    static final class Transactions implements RealmTransaction {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final List<Resolution> resolutions = new CopyOnWriteArrayList<>();
        final AtomicLong sequence = new AtomicLong();
        volatile CommitAction commitAction = this::committed;
        volatile ResolveAction resolveAction = (previous, next, key, digest) ->
                new CommitAttempt.Settled(new CommitResult.NotCommitted(CommitResult.Reason.ROLLED_BACK));
        @Override public CommitAttempt commit(WriterToken writer, CommitDecision decision) throws StorageAccessException {
            calls.add(decision.request().requestId()); return commitAction.run(writer, decision);
        }
        @Override public CommitAttempt resolveAfterFence(WriterToken previous, WriterToken next, RequestKey key, String digest)
                throws StorageAccessException {
            resolutions.add(new Resolution(previous, next, key)); return resolveAction.run(previous, next, key, digest);
        }
        CommitAttempt committed(WriterToken writer, CommitDecision decision) {
            return new CommitAttempt.Settled(new CommitResult.Committed(new CommitReceipt(decision.request(),
                    sequence.incrementAndGet(), decision.digest(), decision.resultJson()), true));
        }
    }

    static final class Ownership implements RealmOwnership {
        final AtomicReference<WriterToken> current = new AtomicReference<>(WRITER);
        final AtomicBoolean leaseValid = new AtomicBoolean(true);
        final AtomicInteger fences = new AtomicInteger(), observations = new AtomicInteger(), acquires = new AtomicInteger(),
                reacquires = new AtomicInteger(), renewals = new AtomicInteger();
        final CountDownLatch fenceGate = new CountDownLatch(1);
        volatile boolean pauseFence, loseFenceAcknowledgement;
        volatile UUID foreignAfterFence;
        volatile OwnershipObservation observationOverride;
        @Override public OwnershipObservation observe(RealmIdentity identity) {
            observations.incrementAndGet(); var value = current.get();
            if (observationOverride != null) return observationOverride;
            return new OwnershipObservation(identity, value.epoch(), value.instanceId(), true, leaseValid.get());
        }
        @Override public WriterToken acquire(RealmIdentity identity, UUID instance) throws StorageAccessException {
            acquires.incrementAndGet();
            if (leaseValid.get()) throw new StorageAccessException(StorageAccessException.Code.OWNERSHIP_CONFLICT, "Lease is still valid");
            var value = new WriterToken(identity, instance, current.get().epoch() + 1);
            current.set(value); leaseValid.set(true); return value;
        }
        @Override public WriterToken reacquire(OwnershipObservation expected) throws StorageAccessException {
            reacquires.incrementAndGet(); var old = current.get();
            if (!expected.active() || expected.leaseValid() || leaseValid.get()
                    || !old.realm().equals(expected.realm()) || old.epoch() != expected.epoch()
                    || !old.instanceId().equals(expected.instanceId()))
                throw new StorageAccessException(StorageAccessException.Code.OWNERSHIP_CONFLICT, "Recovery observation changed");
            var value = new WriterToken(old.realm(), old.instanceId(), old.epoch() + 1);
            current.set(value); leaseValid.set(true); return value;
        }
        @Override public WriterToken fence(WriterToken old) throws StorageAccessException {
            fences.incrementAndGet(); if (pauseFence) gate(fenceGate);
            var value = new WriterToken(old.realm(), foreignAfterFence == null ? old.instanceId() : foreignAfterFence, old.epoch() + 1);
            current.set(value);
            if (loseFenceAcknowledgement || foreignAfterFence != null) throw unavailable();
            return value;
        }
        @Override public boolean renew(WriterToken value) {
            renewals.incrementAndGet(); return current.get().equals(value) && leaseValid.get();
        }
        @Override public void release(WriterToken token) { }
    }
}

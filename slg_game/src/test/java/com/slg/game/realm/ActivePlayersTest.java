package com.slg.game.realm;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ActivePlayersTest {
    private static final Duration TTL = Duration.ofMinutes(1);

    @Test void oneColdLoadServesTheWholeMailboxWithoutOccupyingTheOnlyBusinessWorker() throws Exception {
        var loading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var loadsOfA = new AtomicInteger();
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        try (var players = new ActivePlayers(id -> {
            if (id.equals("101")) {
                loadsOfA.incrementAndGet();
                loading.countDown();
                await(release);
            }
            return new ActivePlayer(id, 1);
        }, 1, 1, 8, 4, 4, TTL)) {
            var hot = get(players.submit("102", deadline(), p -> p));
            try {
                var first = players.submit("101", deadline(), p -> { order.add(1); return p; });
                await(loading);
                var cancelled = players.submit("101", deadline(), p -> { order.add(99); return p; });
                assertTrue(cancelled.cancel(false));
                var second = players.submit("101", deadline(), p -> { order.add(2); return p; });
                assertSame(hot, get(players.submit("102", deadline(), p -> p)));
                assertFalse(second.isDone());
                release.countDown();
                assertSame(get(first), get(second));
                assertEquals(List.of(1, 2), order);
                assertEquals(1, loadsOfA.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test void independentLoadsCanOverlapAndOverloadNeverStartsExtraWork() throws Exception {
        var loading = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try (var players = new ActivePlayers(id -> {
            calls.incrementAndGet();
            loading.countDown();
            await(release);
            return new ActivePlayer(id, 1);
        }, 1, 2, 3, 2, 4, TTL)) {
            try {
                var a = players.submit("101", deadline(), ActivePlayer::accountId);
                var a2 = players.submit("101", deadline(), ActivePlayer::accountId);
                failed(RejectedExecutionException.class, players.submit("101", deadline(), p -> fail("per-player bound")));
                var b = players.submit("102", deadline(), ActivePlayer::accountId);
                await(loading);
                failed(RejectedExecutionException.class, players.submit("103", deadline(), p -> fail("global bound")));
                release.countDown();
                assertEquals("101", get(a));
                assertEquals("101", get(a2));
                assertEquals("102", get(b));
                assertEquals(2, calls.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test void sameTargetKeepsAdmissionOrderWhileOtherTargetsCanUseTheOnlyWorker() throws Exception {
        var loadingA = new CountDownLatch(1);
        var releaseA = new CountDownLatch(1);
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        try (var players = new ActivePlayers(id -> {
            if (id.equals("101")) { loadingA.countDown(); await(releaseA); }
            return new ActivePlayer(id, 1);
        }, 1, 2, 8, 2, 6, TTL)) {
            get(players.submit("102", deadline(), ActivePlayer::accountId));
            var first = players.submitForTarget("101", deadline(), "mine1", p -> order.add("101"));
            try {
                await(loadingA);
                var second = players.submitForTarget("102", deadline(), "mine1", p -> order.add("102"));
                failed(RejectedExecutionException.class,
                        players.submitForTarget("104", deadline(), "mine1", p -> fail("target queue full")));
                assertEquals("103", get(players.submitForTarget("103", deadline(), "mine2", ActivePlayer::accountId)));
                assertFalse(second.isDone(), "B must not overtake A while A is cold");
                releaseA.countDown();
                get(first); get(second);
                assertEquals(List.of("101", "102"), order);
                assertEquals(0, players.metrics().get("targets"));
            } finally { releaseA.countDown(); }
        }
    }

    @Test void resolvingRecallCannotDeadlockBehindItsOwnLaterArrival() throws Exception {
        var resolving = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        try (var players = new ActivePlayers(id -> new ActivePlayer(id, 1), 2, 2, 8, 4, 4, TTL)) {
            var recall = players.submitResolvingTarget("101", deadline(), p -> {
                resolving.countDown(); await(release); return "mine";
            }, p -> order.add("recall"));
            try {
                await(resolving);
                var later = players.submitForTarget("101", deadline(), "mine", p -> order.add("A due"));
                var other = players.submitForTarget("102", deadline(), "mine", p -> order.add("B due"));
                release.countDown();
                get(recall); get(later); get(other);
                assertEquals(List.of("recall", "A due", "B due"), order);
            } finally { release.countDown(); }
        }
    }

    @Test void removingAnUnstartedTargetHeadWakesOtherOwnersEvenIfItsLoaderIsStillBusy() throws Exception {
        var loading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var players = new ActivePlayers(id -> {
            if (id.equals("101")) { loading.countDown(); await(release); }
            return new ActivePlayer(id, 1);
        }, 1, 2, 8, 4, 6, TTL)) {
            get(players.submit("102", deadline(), ActivePlayer::accountId));
            var cancelled = players.submitForTarget("101", deadline(), "mine", p -> fail("cancelled"));
            try {
                await(loading);
                var waiting = players.submitForTarget("102", deadline(), "mine", ActivePlayer::accountId);
                assertTrue(cancelled.cancel(false));
                assertEquals("102", get(waiting));
                long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(150);
                var expired = players.submitForTarget("101", limit, "mine", p -> fail("expired"));
                var next = players.submitForTarget("102", deadline(), "mine", ActivePlayer::accountId);
                failed(QueueDeadlineException.class, expired);
                assertEquals("102", get(next));
                assertEquals(0, players.metrics().get("targets"));
            } finally { release.countDown(); }
        }
    }

    @Test void aFailedTargetCommandReleasesTheMinerQueueAfterTheWorkHasStopped() throws Exception {
        var working = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var players = new ActivePlayers(id -> new ActivePlayer(id, 1), 2, 2, 8, 4, 4, TTL)) {
            var failed = players.submitForTarget("101", deadline(), "mine", p -> {
                working.countDown(); await(release); throw new IOException("commit result unavailable");
            });
            try {
                await(working);
                var next = players.submitForTarget("102", deadline(), "mine", ActivePlayer::accountId);
                assertEquals("103", get(players.submitForTarget("103", deadline(), "other", ActivePlayer::accountId)));
                assertFalse(next.isDone());
                release.countDown();
                failed(IOException.class, failed);
                assertEquals("102", get(next));
                failed(ActivePlayers.TargetLoadFailed.class, players.submitResolvingTarget("101", deadline(),
                        p -> { throw new IOException("cannot read march target"); }, p -> fail("must never start")));
                assertEquals("101", get(players.submit("101", deadline(), ActivePlayer::accountId)));
                assertEquals(0, players.metrics().get("targets"));
            } finally { release.countDown(); }
        }
    }

    @Test void failedLoadReleasesEveryWaiterAndANewRequestCanRetry() throws Exception {
        var loading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var attempts = new AtomicInteger();
        try (var players = new ActivePlayers(id -> {
            if (attempts.incrementAndGet() == 1) {
                loading.countDown();
                await(release);
                throw new IOException("unavailable");
            }
            return new ActivePlayer(id, 1);
        }, 1, 1, 4, 4, 2, TTL)) {
            try {
                var first = players.submit("101", deadline(), p -> fail("unloaded player"));
                await(loading);
                var second = players.submit("101", deadline(), p -> fail("unloaded player"));
                release.countDown();
                failed(ActivePlayers.LoadFailed.class, first);
                failed(ActivePlayers.LoadFailed.class, second);
                assertEquals("101", get(players.submit("101", deadline(), ActivePlayer::accountId)));
                assertEquals(2, attempts.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test void commandsThatExpireDuringAColdLoadNeverRun() throws Exception {
        long second = TimeUnit.SECONDS.toNanos(1);
        var now = new AtomicLong(100 * second);
        var loading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var players = new ActivePlayers(id -> {
            loading.countDown();
            await(release);
            return new ActivePlayer(id, 1);
        }, 1, 1, 4, 4, 2, TTL, now::get)) {
            try {
                var expired = players.submit("101", 120 * second, p -> fail("deadline must cover cold load"));
                await(loading);
                var remaining = players.submit("101", 200 * second, ActivePlayer::accountId);
                now.set(150 * second);
                release.countDown();
                failed(QueueDeadlineException.class, expired);
                assertEquals("101", get(remaining));
            } finally {
                release.countDown();
            }
        }
    }

    @Test void evictionOnlyReleasesIdlePlayersAndAnEvictedAccountLoadsAgain() throws Exception {
        var now = new AtomicLong(1);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var loads = new AtomicInteger();
        try (var players = new ActivePlayers(id -> {
            loads.incrementAndGet();
            return new ActivePlayer(id, 1);
        }, 1, 1, 4, 3, 1, TTL, now::get)) {
            try {
                var first = players.submit("101", Long.MAX_VALUE, p -> { started.countDown(); await(release); return p; });
                await(started);
                var waiting = players.submit("101", Long.MAX_VALUE, p -> p);
                now.addAndGet(2 * TTL.toNanos());
                assertEquals(0, players.evictIdle());
                failed(RejectedExecutionException.class, players.submit("102", Long.MAX_VALUE, p -> fail("all entries in use")));
                release.countDown();
                ActivePlayer old = get(first);
                assertSame(old, get(waiting));
                now.addAndGet(2 * TTL.toNanos());
                players.evictIdle();
                assertNotSame(old, get(players.submit("101", Long.MAX_VALUE, p -> p)));
                assertEquals(2, loads.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test void shutdownRejectsPendingWorkAndWaitsForTheRunningCommand() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var players = new ActivePlayers(id -> new ActivePlayer(id, 1), 1, 1, 4, 3, 4, TTL);
        try {
            var first = players.submit("101", deadline(), p -> { started.countDown(); await(release); return p.accountId(); });
            await(started);
            var queued = players.submit("101", deadline(), p -> fail("queue must be discarded"));
            var closing = CompletableFuture.runAsync(players::close);
            failed(RejectedExecutionException.class, queued);
            assertFalse(closing.isDone(), "the volume still has a command in flight");
            failed(RejectedExecutionException.class, players.submit("102", deadline(), p -> fail("closing")));
            release.countDown();
            assertEquals("101", get(first));
            get(closing);
        } finally {
            release.countDown();
            players.close();
        }
    }

    @Test void shutdownCannotReleaseTheVolumeDuringAColdRead() throws Exception {
        var loading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var players = new ActivePlayers(id -> {
            loading.countDown();
            await(release);
            return new ActivePlayer(id, 1);
        }, 1, 1, 3, 2, 3, TTL);
        try {
            var command = players.submit("101", deadline(), p -> fail("closed before activation completed"));
            await(loading);
            var closing = CompletableFuture.runAsync(players::close);
            failed(RejectedExecutionException.class, command);
            assertFalse(closing.isDone(), "the loader is still accessing volume data");
            release.countDown();
            get(closing);
        } finally {
            release.countDown();
            players.close();
        }
    }

    private static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(10); }
    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(5, TimeUnit.SECONDS), "latch timed out");
    }
    private static <T> T get(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }
    private static void failed(Class<? extends Throwable> expected, CompletableFuture<?> future) {
        assertInstanceOf(expected, assertThrows(ExecutionException.class, () -> get(future)).getCause());
    }
}

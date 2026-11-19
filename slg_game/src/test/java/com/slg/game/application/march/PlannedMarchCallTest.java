package com.slg.game.application.march;

import com.slg.game.application.commit.CommitResult;
import com.slg.game.application.player.PlayerCall;
import com.slg.game.application.player.PlannedWriteCall;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PlannedMarchCallTest {
    @Test void cancellingAnAlreadyStartedPlanningReadPreventsAnyWriteAdmission() {
        var planning = new Call<MarchPlan>(false);
        var submissions = new AtomicInteger();
        var call = new PlannedWriteCall<>(planning, ignored -> { submissions.incrementAndGet(); return new Call<>(false); });
        assertTrue(call.cancelBeforeStart());
        assertFalse(call.result().toCompletableFuture().isDone(), "Internal read still owns its resources");
        planning.completion.complete(new MarchPlan("march", 1, 150, 150));
        assertEquals(0, submissions.get());
        assertEquals(CommitResult.Reason.CANCELLED,
                assertInstanceOf(CommitResult.NotCommitted.class, call.result().toCompletableFuture().join()).reason());
    }

    @Test void cancellationAfterWriteStartedCannotCancelItsInternalOutcome() {
        var planning = new Call<MarchPlan>(false); var writing = new Call<CommitResult>(false);
        var call = new PlannedWriteCall<>(planning, ignored -> writing);
        planning.completion.complete(new MarchPlan("march", 1, 150, 150));
        assertFalse(call.cancelBeforeStart());
        assertTrue(call.result().toCompletableFuture().cancel(false));
        var outcome = new CommitResult.NotCommitted(CommitResult.Reason.ROLLED_BACK);
        writing.completion.complete(outcome);
        assertEquals(outcome, call.result().toCompletableFuture().join());
    }

    private static final class Call<T> implements PlayerCall<T> {
        final CompletableFuture<T> completion = new CompletableFuture<>();
        final boolean cancellable;
        Call(boolean cancellable) { this.cancellable = cancellable; }
        @Override public CompletionStage<T> result() { return completion.minimalCompletionStage(); }
        @Override public boolean cancelBeforeStart() { return cancellable; }
    }
}

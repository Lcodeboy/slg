package com.slg.game.realm;

import com.slg.rpc.v1.CreatePlayerRsp;
import com.slg.rpc.v1.GetReceiptReq;
import com.slg.rpc.v1.GetReceiptRsp;
import com.slg.rpc.v1.WriteReceipt;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.slg.rpc.generated.GameplayRoutes;
import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class CommandReceiptQueryTest {
    private static final String ALICE = "101", BOB = "102";
    @TempDir Path directory;
    private RealmVolume volume;
    private RealmMarchService game;
    private final AtomicBoolean allowProjectionWrite = new AtomicBoolean(true);
    private final AtomicBoolean allowDiskWrite = new AtomicBoolean(true);

    @BeforeEach void openRealm() throws IOException {
        var id = UUID.randomUUID();
        RealmVolume.initialize(directory, 17, id);
        volume = RealmVolume.open(directory, 17, id);
        game = new RealmMarchService(volume.journal(), Clock.systemUTC(),
                high -> allowProjectionWrite.get(), allowDiskWrite::get);
    }

    @AfterEach void closeRealm() {
        if (volume != null) volume.close();
    }

    @Test void sameRequestIdInTwoRoleScopesReturnsOnlyTheAuthenticatedRolesOriginalResponse() throws IOException {
        String id = "same-id";
        var alice = create(ALICE, id, "Alice");
        assertMissing(BOB, id);
        var bob = create(BOB, id, "Bob");
        assertNotEquals(alice, bob);
        assertEquals(alice, recorded(ALICE, id, 1).getCreated());
        assertEquals(bob, recorded(BOB, id, 2).getCreated());
        // 查询本身不占原卷序号。它的关联 ID 随后仍可用于发起一次真正的写请求。
        assertMissing(ALICE, "lookup");
        assertEquals(2, volume.journal().highSequence());
        assertEquals("PLAYER_EXISTS", rejection(() -> create(ALICE, "lookup", "Again")));
        var denied = recorded(ALICE, "lookup", 3);
        assertEquals(GameplayRoutes.CREATE_PLAYER, denied.getMsgId());
        assertEquals("PLAYER_EXISTS", denied.getError().getCode());
    }

    @Test void durablePreCreationRejectionRemainsVisibleAfterCreationAndPayloadConflict() throws IOException {
        var active = game.loadPlayer(ALICE);
        assertEquals("PLAYER_NOT_FOUND", rejection(() -> game.startMarch(active, "depart", gather(100))));
        var original = recorded(ALICE, "depart", 1);
        assertEquals(GameplayRoutes.START_MARCH, original.getMsgId());
        assertEquals("PLAYER_NOT_FOUND", original.getError().getCode());
        assertNull(volume.journal().read("account-role:" + ALICE));
        assertNull(volume.journal().read("player:17_" + ALICE));
        create(ALICE, "create", "Alice");
        assertEquals("REQUEST_CONFLICT", rejection(() -> game.startMarch(game.loadPlayer(ALICE), "depart", gather(99))));
        assertEquals(original, recorded(ALICE, "depart", 1));
        assertEquals(2, volume.journal().highSequence());
    }

    @Test void changingTheOperationCannotReplayAnUnrelatedSuccessfulReceipt() throws IOException {
        var original = create(ALICE, "intent", "Alice");
        assertEquals("REQUEST_CONFLICT", rejection(() -> game.startMarch(game.loadPlayer(ALICE), "intent", gather(1))));
        var saved = recorded(ALICE, "intent", 1);
        assertEquals(GameplayRoutes.CREATE_PLAYER, saved.getMsgId());
        assertEquals(original, saved.getCreated());
        assertEquals(1L, volume.journal().highSequence());
    }

    @Test void transientAdmissionRefusalsCanBeRetriedAndCommittedReceiptsStayReadableUnderPressure() throws IOException {
        allowProjectionWrite.set(false);
        assertEquals("PROJECTION_BACKLOG", rejection(() -> create(ALICE, "create", "Alice")));
        assertMissing(ALICE, "create");
        allowProjectionWrite.set(true);
        allowDiskWrite.set(false);
        assertEquals("DISK_PRESSURE", rejection(() -> create(ALICE, "create", "Alice")));
        assertMissing(ALICE, "create");
        assertEquals(0, volume.journal().highSequence());
        allowDiskWrite.set(true);
        var original = create(ALICE, "create", "Alice");
        allowProjectionWrite.set(false);
        allowDiskWrite.set(false);
        var receipt = recorded(ALICE, "create", 1);
        assertEquals(GameplayRoutes.CREATE_PLAYER, receipt.getMsgId());
        assertEquals(original, receipt.getCreated());
        assertEquals(1, volume.journal().highSequence());
    }

    @Test void onlyTheSessionCanSelectTheScopeAndInvalidIdsAreRejected() throws IOException {
        create(ALICE, "sent", "Alice");
        for (String invalid : List.of("", " sent", "sent ", "\tsent", "ｓent", "timer:1", "a".repeat(65)))
            assertEquals("INVALID_ARGUMENT", rejection(() -> query(ALICE, invalid)), invalid);
        // 一个合法目标 ID 在另一个账号的邮箱查询只能得到缺失。协议中不能提交角色或区服来改变查询范围。
        assertEquals(1, GetReceiptReq.getDescriptor().getFields().size());
        assertMissing(BOB, "sent");
        assertMissing(BOB, "A_-" + "1".repeat(61));
        assertEquals(1, volume.journal().highSequence());
    }

    private WriteReceipt recorded(String account, String target, long sequence) throws IOException {
        var response = query(account, target);
        assertTrue(response.hasReceipt());
        assertEquals(sequence, response.getJournalSeq());
        return response.getReceipt();
    }

    private void assertMissing(String account, String target) throws IOException {
        var result = query(account, target);
        assertFalse(result.hasReceipt());
        assertEquals(0L, result.getJournalSeq());
    }

    private GetReceiptRsp query(String account, String target) throws IOException {
        var response = game.getReceipt(game.loadPlayer(account), "lookup", receiptFor(target));
        assertEquals(target, response.getTargetRequestId());
        return response;
    }

    private CreatePlayerRsp create(String account, String id, String name) throws IOException {
        return game.createPlayer(game.loadPlayer(account), id, named(name));
    }
}

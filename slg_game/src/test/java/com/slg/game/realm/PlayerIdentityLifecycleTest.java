package com.slg.game.realm;

import com.slg.rpc.v1.CreatePlayerReq;
import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.MarchView;
import com.slg.rpc.v1.PlayerView;
import com.slg.rpc.v1.StartMarchReq;
import com.slg.core.protocol.Json;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

class PlayerIdentityLifecycleTest {
    private static final String ACCOUNT = "9007199254740993";
    private static final String OTHER = "42";
    private static final Clock START = Clock.fixed(Instant.parse("2026-11-01T00:00:00Z"), ZoneOffset.UTC);
    private static final CreatePlayerReq CREATE = named("Alice");
    private static final StartMarchReq MARCH = gather(120);
    @TempDir Path directory;

    @Test void oneAccountCreatesDistinctRolesInTwoRealmsAndReplaysItsOwnReceiptAfterRestart() throws Exception {
        UUID firstId = UUID.randomUUID(), secondId = UUID.randomUUID();
        Path first = initialize("first", 17, firstId), second = initialize("second", 18, secondId);
        PlayerView originalCreate;
        MarchView firstMarch;
        try (var volume = RealmVolume.open(first, 17, firstId)) {
            var game = new RealmMarchService(volume.journal(), START);
            originalCreate = create(game, ACCOUNT);
            identity(originalCreate, 17, 17);
            var created = volume.journal().entry(1);
            assertEquals("17_" + ACCOUNT, created.scope());
            assertEquals(2, created.changes().size());
            assertNotNull(volume.journal().read("player:17_" + ACCOUNT));
            assertNotNull(volume.journal().read("account-role:" + ACCOUNT));
            firstMarch = march(game, ACCOUNT);
        }
        try (var volume = RealmVolume.open(second, 18, secondId)) {
            var game = new RealmMarchService(volume.journal(), START);
            identity(create(game, ACCOUNT), 18, 18);
            var secondMarch = march(game, ACCOUNT);
            assertEquals("18_" + ACCOUNT, secondMarch.getPlayerId());
            assertNotEquals(firstMarch.getMarchId(), secondMarch.getMarchId());
        }
        try (var volume = RealmVolume.open(first, 17, firstId)) {
            var game = new RealmMarchService(volume.journal(), START);
            assertEquals(originalCreate, create(game, ACCOUNT));
            assertEquals(firstMarch, march(game, ACCOUNT));
            assertEquals(2, volume.journal().highSequence());
            assertEquals("PLAYER_EXISTS", rejection(() -> game.createPlayer(game.loadPlayer(ACCOUNT),
                    "new-create", named("Replacement"))));
            identity(player(game, ACCOUNT), 17, 17);
            assertEquals("Alice", player(game, ACCOUNT).getName());
        }
    }

    @Test void commandPayloadCannotChooseIdentityOrReadAnotherAccountsMarch() throws Exception {
        UUID id = UUID.randomUUID();
        Path path = initialize("forgery", 17, id);
        try (var volume = RealmVolume.open(path, 17, id)) {
            var game = new RealmMarchService(volume.journal(), START);
            create(game, ACCOUNT);
            var march = march(game, ACCOUNT);
            // 由会话提供账号，原卷提供区服。四个身份字段即使填入合法值也不能成为客户端输入。
            for (String field : List.of("account_id", "player_id", "founding_realm_id", "current_realm_id"))
                assertNull(CreatePlayerReq.getDescriptor().findFieldByName(field));
            assertEquals("PLAYER_NOT_FOUND", rejection(() -> player(game, OTHER)));
            create(game, OTHER);
            var readMarch = marchById(march.getMarchId());
            assertEquals("MARCH_NOT_FOUND", rejection(() -> game.getMarch(game.loadPlayer(OTHER), "peek", readMarch)));
            assertEquals(2880, player(game, ACCOUNT).getTroopsOrThrow("INFANTRY"));
            assertEquals(3000, player(game, OTHER).getTroopsOrThrow("INFANTRY"));
        }
    }

    @Test void coldLoadingAndOfflineTimerReturnUseFoundingRoleWhenCurrentRealmDiffers() throws Exception {
        UUID id = UUID.randomUUID();
        Path path = initialize("destination", 20, id);
        String role = "17_" + ACCOUNT;
        MarchView departure;
        try (var volume = RealmVolume.open(path, 20, id)) {
            var journal = volume.journal();
            // 构造迁移完成后的合法状态，直接测试 GAME 如何消费该状态。迁服本身不属于本用例。
            fixture(journal, binding(ACCOUNT, role), snapshot(17, 20));
            var game = new RealmMarchService(journal, START);
            identity(player(game, ACCOUNT), 17, 20);
            departure = march(game, ACCOUNT);
            assertEquals(role, departure.getPlayerId());
            assertEquals(role, journal.entry(2).scope());
            assertEquals(role, journal.dueTasks(Long.MAX_VALUE, null, 2).getFirst().playerId());
        }
        // 重开目的区原卷，先只运行生产定时器。此时没有客户端为角色预热邮箱。
        try (var volume = RealmVolume.open(path, 20, id)) {
            var journal = volume.journal();
            var late = Clock.offset(START, Duration.ofHours(1));
            var game = new RealmMarchService(journal, late);
            try (var players = new ActivePlayers(game::loadPlayer, 1, 1, 16, 8, 2, Duration.ofMinutes(1));
                 var pump = new RealmTaskPump(journal, players, game, late, 4)) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (journal.highSequence() < 5 && System.nanoTime() < deadline) {
                    pump.poll();
                    TimeUnit.MILLISECONDS.sleep(10);
                }
                assertEquals(5, journal.highSequence(), "arrival, gather, and return should all find the existing role");
            }
            assertTrue(journal.dueTasks(Long.MAX_VALUE, null, 2).isEmpty());
            var returned = player(game, ACCOUNT);
            identity(returned, 17, 20);
            assertEquals(3000, returned.getTroopsOrThrow("INFANTRY"));
            assertEquals(600, returned.getResourcesOrThrow("WOOD"));
            assertNull(journal.read("player:20_" + ACCOUNT));
            assertEquals(departure, march(game, ACCOUNT));
            assertEquals(5, journal.highSequence());
        }
    }

    @Test void brokenDirectoryOrSnapshotStopsActivationBeforeAReplacementCanBeCreated() throws Exception {
        String role = "17_" + ACCOUNT;
        rejectsBrokenIdentity("missing-player", List.of(binding(ACCOUNT, role)));
        rejectsBrokenIdentity("missing-directory", List.of(snapshot(20, 20)));
        rejectsBrokenIdentity("other-owner", List.of(binding(ACCOUNT, "17_" + OTHER)));
        rejectsBrokenIdentity("wrong-current-realm", List.of(binding(ACCOUNT, role), snapshot(17, 18)));
        rejectsBrokenIdentity("noncanonical-role", List.of(binding(ACCOUNT, "017_" + ACCOUNT)));
        var stored = snapshot(17, 20);
        // 两个数值在 Java 中可被默认 JSON 转型“纠正”，但这些字节已背离投影要求的持久化契约。
        for (var field : List.of("accountId", "currentRealmId")) {
            var malformed = (com.fasterxml.jackson.databind.node.ObjectNode) Json.MAPPER.readTree(stored.json());
            if (field.equals("accountId")) malformed.put(field, Long.parseLong(ACCOUNT));
            else malformed.put(field, 20.5);
            rejectsBrokenIdentity("wrong-type-" + field, List.of(binding(ACCOUNT, role),
                    new Change(stored.key(), 0, Json.write(malformed))));
        }
        rejectsBrokenIdentity("wrong-target", List.of(binding(ACCOUNT, role),
                new Change("player:" + role, 0, snapshot(18, 20).json())));
    }

    private void rejectsBrokenIdentity(String label, List<Change> changes) throws Exception {
        UUID id = UUID.randomUUID();
        Path path = initialize(label, 20, id);
        try (var volume = RealmVolume.open(path, 20, id)) {
            var journal = volume.journal();
            fixture(journal, changes.toArray(Change[]::new));
            var game = new RealmMarchService(journal, START);
            assertThrows(IOException.class, () -> create(game, ACCOUNT), label);
            assertEquals(1, journal.highSequence(), label);
        }
    }

    private Path initialize(String label, int realm, UUID id) throws IOException {
        var path = Files.createDirectory(directory.resolve(label));
        RealmVolume.initialize(path, realm, id);
        return path;
    }

    private static Change binding(String account, String role) {
        return new Change("account-role:" + account, 0, Json.write(Map.of("playerId", role)));
    }

    private static Change snapshot(int founding, int current) {
        String role = founding + "_" + ACCOUNT;
        var player = new ActivePlayer.Player(role, ACCOUNT, founding, current, "Alice", Map.of("INFANTRY", 3000L),
                List.of(new ActivePlayer.Hero("sergey", null)), List.of(), Map.of("WOOD", 0L), Map.of(), Map.of());
        return new Change("player:" + role, 0, Json.write(player));
    }

    private static void fixture(RealmJournal journal, Change... changes) throws IOException {
        journal.commit(new RealmJournal.Decision("fixture", "seed", "digest", "FIXTURE", "{}", "{}", List.of(changes)));
    }

    private static PlayerView create(RealmMarchService game, String account) throws IOException {
        return game.createPlayer(game.loadPlayer(account), "create", CREATE).getPlayer();
    }

    private static PlayerView player(RealmMarchService game, String account) throws IOException {
        return game.getPlayer(game.loadPlayer(account), "read", GetPlayerReq.getDefaultInstance()).getPlayer();
    }

    private static MarchView march(RealmMarchService game, String account) throws IOException {
        return game.startMarch(game.loadPlayer(account), "march", MARCH).getMarch();
    }

    private static void identity(PlayerView player, int founding, int current) {
        assertEquals(founding + "_" + ACCOUNT, player.getPlayerId());
        assertEquals(ACCOUNT, player.getAccountId());
        assertEquals(founding, player.getFoundingRealmId());
        assertEquals(current, player.getCurrentRealmId());
    }
}

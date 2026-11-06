package com.slg.game.storage;

import com.slg.rpc.v1.GameRpcGrpc;
import com.slg.rpc.v1.GetMarchRsp;
import com.slg.rpc.v1.GetPlayerReq;
import com.slg.rpc.v1.GetPlayerRsp;
import com.slg.rpc.v1.GetReceiptRsp;
import com.slg.rpc.v1.MarchState;
import com.slg.rpc.v1.MarchView;
import com.slg.rpc.v1.PlayRequest;
import com.slg.rpc.v1.PlayerView;
import com.slg.rpc.v1.StartMarchRsp;
import com.slg.rpc.v1.WriteReceipt;
import com.fasterxml.jackson.databind.JsonNode;
import com.slg.core.auth.TokenService;
import com.slg.core.protocol.Json;
import com.slg.game.GameServer;
import com.slg.game.projection.MySqlProjectionStore;
import com.slg.game.realm.RealmMarchService;
import com.slg.rpc.generated.GameplayRoutes;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.slg.rpc.RpcMetadata;
import com.slg.rpc.RpcTransport;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;

import static com.slg.game.realm.GameRequests.*;
import static org.junit.jupiter.api.Assertions.*;

/** 仅供 Failsafe 的独立区服试验。父进程掌握外部强杀点，子进程始终是该卷的唯一写者。 */
public final class CleanupCrashScenario {
    private static final int REALM = 1;
    private static final String ACCOUNT = "103";
    private static final String PLAYER = REALM + "_" + ACCOUNT;
    private static final String REQUEST = "original-departure";
    private static final List<String> EVENTS = List.of("PLAYER_CREATED", "MARCH_STARTED", "COMMAND_REJECTED",
            "COMMAND_REJECTED", "MARCH_GATHER_STARTED", "MARCH_GATHER_FINISHED", "MARCH_COMPLETED");

    private CleanupCrashScenario() {}

    /** 在每次 IT 单独建的 schema 和 JUnit 临时目录中执行。凭证仅经子进程环境传递。 */
    public static void run(Path temp, DataSource mysql, String jdbcUrl, String user, String password, UUID volume)
            throws Exception {
        Path testRoot = temp.toRealPath();
        Path signal = testRoot.resolve("cleanup-committed.json");
        String secret = UUID.randomUUID().toString() + UUID.randomUUID();
        int healthPort, rpcPort, workerPort;
        try (var h = port(); var g = port(); var w = port()) {
            healthPort = h.getLocalPort(); rpcPort = g.getLocalPort(); workerPort = w.getLocalPort();
        }
        Map<String, String> env = Map.of("SLG_PROJECTION_ENABLED", "true", "SLG_PROJECTION_JDBC_URL", jdbcUrl,
                "SLG_PROJECTION_DB_USER", user, "SLG_PROJECTION_DB_PASSWORD", password, "SLG_TOKEN_SECRET", secret,
                "SLG_GAME_PORT", String.valueOf(healthPort), "SLG_GAME_RPC_PORT", String.valueOf(rpcPort),
                "SLG_BATTLE_RPC_TARGET", "127.0.0.1:" + workerPort, "SLG_BIND", "127.0.0.1");

        try (var writer = launch(testRoot, env, "isolated-writer", Writer.class, testRoot.toString(), volume.toString())) {
            // 信号仅在清理批次已返回、尚未 close/flush/WAL 同步时发布。强杀绝不执行 Java shutdown hook。
            writer.await(() -> Files.isRegularFile(signal), "fixture did not reach the committed cleanup");
            writer.kill();
        }
        JsonNode evidence = Json.MAPPER.readTree(Files.readString(signal));
        var sent = StartMarchRsp.parseFrom(evidence.required("departure").binaryValue());
        String marchId = sent.getMarch().getMarchId();
        assertFalse(marchId.isEmpty());
        assertEquals(List.of(3L, 4L), Json.MAPPER.convertValue(evidence.path("retained"),
                new com.fasterxml.jackson.core.type.TypeReference<List<Long>>() {}));
        assertEquals(3, new MySqlProjectionStore(mysql, REALM, volume).checkpoint());
        System.out.printf("PASS cleanup before crash: H=4, S=4, MySQL=3, deleted=J1..J2, retained=J3..J4, march=%s%n", marchId);

        Path directory = testRoot.resolve("realm");
        String token = new TokenService(secret, Clock.systemUTC()).issue(ACCOUNT, Duration.ofHours(1));
        long arrivalTime = sent.getMarch().getArriveAt();
        // 在首次到达之前，正式 GAME 必须已经打开清过的卷、用保留下来的 J3 核验 SQL，
        // 然后投影 J4。连续确认在途状态以及进程死亡时间，避免把结算后的强杀误算为在途强杀。
        try (var game = launch(testRoot, env, "game-in-flight", GameServer.class, "realm", "serve",
                directory.toString(), String.valueOf(REALM), volume.toString())) {
            game.await(() -> caughtUp(healthPort, volume, 4), "GAME did not recover the in-flight march at J4");
            try (var client = new GameClient(rpcPort, token)) {
                client.assertOriginalReceiptAndReplay(sent);
                var marching = client.march(marchId);
                assertEquals(MarchState.MARCH_STATE_OUTBOUND, marching.getState());
                assertEquals(arrivalTime, marching.getArriveAt());
                var player = client.player();
                assertEquals(2880, player.getTroopsOrThrow("INFANTRY"));
                assertEquals(0, player.getResourcesOrThrow("WOOD"));
                assertEquals(1, player.getActiveMarchIdsCount());
                assertEquals(marchId, player.getActiveMarchIds(0));
                assertEquals(marchId, player.getHeroes(0).getAssignedMarch());
                assertTrue(caughtUp(healthPort, volume, 4), "A read or duplicate departure created a decision");
                game.kill();
                assertTrue(System.currentTimeMillis() < arrivalTime, "In-flight kill did not finish before arrival");
            }
        }
        System.out.println("PASS forced termination of live GAME during OUTBOUND: journal=SQL=4, city infantry=2880, wood=0");

        // 强杀在途 GAME 后，继任者仅通过普通后台待办自行走完三个阶段；结算后再次强杀，
        // 重新启动仍须读到唯一的返城和奖励。轮询健康水位，不用玩家写命令驱动任务。
        PlayerView finalPlayer = null;
        MarchView finalMarch = null;
        for (int boot = 1; boot <= 2; boot++) {
            try (var game = launch(testRoot, env, "game-" + boot, GameServer.class, "realm", "serve",
                    directory.toString(), String.valueOf(REALM), volume.toString())) {
                game.await(() -> caughtUp(healthPort, volume, 7), "GAME did not settle and project all seven decisions");
                try (var client = new GameClient(rpcPort, token)) {
                    client.assertOriginalReceiptAndReplay(sent);
                    var player = client.player();
                    var march = client.march(marchId);
                    assertEquals(3000, player.getTroopsOrThrow("INFANTRY"));
                    assertEquals(600, player.getResourcesOrThrow("WOOD"));
                    assertTrue(player.getActiveMarchIdsList().isEmpty());
                    assertFalse(player.getHeroes(0).hasAssignedMarch());
                    assertEquals(MarchState.MARCH_STATE_COMPLETED, march.getState());
                    if (boot == 2) {
                        assertEquals(finalPlayer, player);
                        assertEquals(finalMarch, march);
                    }
                    finalPlayer = player; finalMarch = march;
                }
                assertTrue(caughtUp(healthPort, volume, 7), "Request replay appended a new log entry");
                game.kill();
            }
        }

        try (var original = RealmVolume.open(directory, REALM, volume)) {
            var journal = original.journal();
            var page = journal.inspectPage(0, 32);
            assertNull(page.issue());
            assertEquals(2, page.prunedThrough());
            assertEquals(3, page.first());
            assertEquals(7, page.verifiedThrough());
            var receipt = journal.lookup(PLAYER, REQUEST, evidence.path("digest").asText());
            assertEquals(2, receipt.sequence());
            var saved = WriteReceipt.parseFrom(Json.MAPPER.readValue(receipt.resultJson(), byte[].class));
            assertEquals(GameplayRoutes.START_MARCH, saved.getMsgId());
            assertEquals(sent, saved.getStarted());
            checkSql(mysql, volume, journal, finalPlayer, finalMarch, marchId);
        }
        System.out.println("PASS recovery after in-flight and post-settlement GAME kills: original receipt seq=2, journal=SQL=7, one return, 120 troops and 600 wood");
    }

    /** 与网关采用相同的 RPC 封包、鉴权和业务命令，始终只连接本次新开的回环端口。 */
    private static final class GameClient implements AutoCloseable {
        private final ManagedChannel channel;
        private final GameRpcGrpc.GameRpcBlockingStub stub;

        GameClient(int rpcPort, String token) {
            channel = RpcTransport.channel("127.0.0.1:" + rpcPort);
            Metadata headers = new Metadata();
            headers.put(RpcMetadata.AUTHORIZATION, "Bearer " + token);
            stub = GameRpcGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }

        private <R extends MessageLite> R play(String id, int msgId, MessageLite request, Parser<R> parser)
                throws IOException {
            var reply = stub.withDeadlineAfter(5, TimeUnit.SECONDS).play(PlayRequest.newBuilder()
                    .setMsgId(msgId).setRequestId(id).setBody(request.toByteString()).build());
            assertEquals(id, reply.getRequestId());
            assertEquals(msgId, reply.getMsgId());
            assertEquals(200, reply.getStatus());
            assertTrue(reply.hasBody(), reply.getError().getCode());
            return parser.parseFrom(reply.getBody());
        }

        PlayerView player() throws IOException {
            return play("read-player", GameplayRoutes.GET_PLAYER, GetPlayerReq.getDefaultInstance(),
                    GetPlayerRsp.parser()).getPlayer();
        }

        MarchView march(String id) throws IOException {
            return play("read-march", GameplayRoutes.GET_MARCH, marchById(id), GetMarchRsp.parser()).getMarch();
        }

        /** J2 清除后，回执键仍包含第一次成功的类型和正文。重试及查回执都须保留完整快照。 */
        void assertOriginalReceiptAndReplay(StartMarchRsp departure) throws IOException {
            var found = play("inspect-departure", GameplayRoutes.GET_RECEIPT, receiptFor(REQUEST), GetReceiptRsp.parser());
            assertEquals(REQUEST, found.getTargetRequestId());
            assertEquals(2L, found.getJournalSeq());
            assertEquals(GameplayRoutes.START_MARCH, found.getReceipt().getMsgId());
            assertEquals(departure, found.getReceipt().getStarted());
            assertEquals(departure, play(REQUEST, GameplayRoutes.START_MARCH, gather(120), StartMarchRsp.parser()));
        }

        @Override public void close() throws InterruptedException { channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
    }

    private static void assertJsonEquals(JsonNode expected, JsonNode actual) {
        // MySQL JSON 和 RPC 可采用 0 / 0.0 表示同一数值；对象、数组及其他字段必须逐一相同。
        assertTrue(expected.equals((a, b) -> a.isNumber() && b.isNumber() ? a.decimalValue().compareTo(b.decimalValue())
                : (a.equals(b) ? 0 : 1), actual), () -> "expected=" + expected + ", actual=" + actual);
    }

    private static void checkSql(DataSource mysql, UUID volume, RealmJournal journal, PlayerView player,
                                 MarchView march, String marchId) throws Exception {
        assertEquals(7, journal.highSequence());
        assertEquals(7, new MySqlProjectionStore(mysql, REALM, volume).checkpoint());
        try (var connection = mysql.getConnection(); var query = connection.createStatement()) {
            var facts = new ArrayList<String>();
            try (var rows = query.executeQuery("SELECT journal_seq,fact_type FROM projection_event WHERE realm_id=1 ORDER BY journal_seq")) {
                while (rows.next()) {
                    assertEquals(facts.size() + 1, rows.getLong(1));
                    facts.add(rows.getString(2));
                }
            }
            assertEquals(EVENTS, facts);
            var expected = Map.of("account-role:" + ACCOUNT, 1L, "player:" + PLAYER, 7L, "march:" + marchId, 7L,
                    "task:arrive-" + marchId, 5L, "task:gather-" + marchId, 6L, "task:return-" + marchId, 7L,
                    "resource:150-150", 6L);
            var seen = new HashSet<String>();
            try (var rows = query.executeQuery("SELECT object_type,object_id,object_version,journal_seq,state_json FROM projection_object WHERE realm_id=1")) {
                while (rows.next()) {
                    String type = rows.getString(1), key = type + ":" + rows.getString(2);
                    assertTrue(seen.add(key));
                    var raw = journal.read(key);
                    assertNotNull(raw);
                    assertEquals(raw.version(), rows.getLong(3));
                    assertEquals(expected.get(key), rows.getLong(4));
                    JsonNode state = Json.MAPPER.readTree(raw.json());
                    assertJsonEquals(state, Json.MAPPER.readTree(rows.getString(5)));
                    if (type.equals("player")) {
                        assertEquals(raw.version(), player.getVersion());
                        assertEquals(player.getPlayerId(), state.required("playerId").textValue());
                        assertEquals(player.getAccountId(), state.required("accountId").textValue());
                        assertEquals(player.getFoundingRealmId(), state.required("foundingRealmId").intValue());
                        assertEquals(player.getCurrentRealmId(), state.required("currentRealmId").intValue());
                        assertEquals(player.getName(), state.required("name").textValue());
                        assertJsonEquals(Json.MAPPER.valueToTree(player.getTroopsMap()), state.required("troops"));
                        assertJsonEquals(Json.MAPPER.valueToTree(player.getResourcesMap()), state.required("resources"));
                        assertJsonEquals(Json.MAPPER.valueToTree(player.getWoundedMap()), state.required("wounded"));
                        assertJsonEquals(Json.MAPPER.valueToTree(player.getDeadMap()), state.required("dead"));
                        assertJsonEquals(Json.MAPPER.valueToTree(player.getActiveMarchIdsList()), state.required("activeMarchIds"));
                        assertEquals(1, state.required("heroes").size());
                        assertEquals(1, player.getHeroesCount());
                        assertEquals(player.getHeroes(0).getHeroId(), state.at("/heroes/0/heroId").textValue());
                        assertFalse(player.getHeroes(0).hasAssignedMarch());
                        assertTrue(state.at("/heroes/0/assignedMarch").isNull());
                    } else if (type.equals("account-role")) assertEquals(PLAYER, state.required("playerId").textValue());
                    else if (type.equals("march")) {
                        assertEquals(march.getMarchId(), state.required("marchId").textValue());
                        assertEquals(march.getPlayerId(), state.required("playerId").textValue());
                        assertEquals("MARCH_STATE_" + state.required("state").textValue(), march.getState().name());
                        assertEquals("MARCH_MISSION_" + state.required("mission").textValue(), march.getMission().name());
                        assertEquals(march.getFromX(), state.required("fromX").doubleValue());
                        assertEquals(march.getFromY(), state.required("fromY").doubleValue());
                        assertEquals(march.getTargetX(), state.required("targetX").intValue());
                        assertEquals(march.getTargetY(), state.required("targetY").intValue());
                        assertEquals(march.getDepartAt(), state.required("departAt").longValue());
                        assertEquals(march.getArriveAt(), state.required("arriveAt").longValue());
                        assertEquals(march.getTier(), state.required("tier").intValue());
                        assertEquals(march.getRulesVersion(), state.required("rulesVersion").textValue());
                        assertEquals(march.getTaskId(), state.required("taskId").asText(""));
                        assertEquals(state.required("troops").size(), march.getTroopsCount());
                        for (var group : march.getTroopsList()) {
                            String typeName = group.getType().name().substring("TROOP_TYPE_".length());
                            assertEquals(group.getCount(), state.required("troops").required(typeName).longValue());
                        }
                        assertJsonEquals(Json.MAPPER.valueToTree(march.getHeroIdsList()), state.required("heroIds"));
                        assertJsonEquals(Json.MAPPER.valueToTree(march.getCargoMap()), state.required("cargo"));
                    }
                    else if (type.equals("resource")) {
                        // J5 占矿、J6 采满并释放，J7 的返城不能再动这个矿点。
                        assertEquals(2, raw.version());
                        assertTrue(state.path("occupantMarchId").isNull());
                        assertTrue(state.path("occupantPlayerId").isNull());
                        assertEquals(0, state.required("generation").intValue());
                    } else assertEquals("DONE", state.path("state").asText());
                }
            }
            assertEquals(expected.keySet(), seen);
        }
    }

    /** 只供本测试 fork 的 JVM 加载；在同一线程独占的卷上运行真实行军领域、原子提交及清理。 */
    public static final class Writer {
        public static void main(String[] args) throws Exception {
            Path root = Path.of(args[0]); UUID id = UUID.fromString(args[1]);
            try (var isolated = IsolatedCleanupVolume.create(root, REALM, id); var mysql = new HikariDataSource()) {
                var journal = isolated.journal();
                // 稍微提前业务时钟，使首次启动的正式 GAME 有约 33 秒打开卷、验证原回执、追上 SQL，
                // 并在真实到达前被父进程强杀。后续 GAME 始终用正常时钟及实际定时器推进，时间不足即失败。
                Clock departureClock = Clock.fixed(Instant.now().plusSeconds(30), ZoneOffset.UTC);
                var game = new RealmMarchService(journal, departureClock);
                var active = game.loadPlayer(ACCOUNT);
                game.createPlayer(active, "create", named("Alice"));
                var sent = game.startMarch(active, REQUEST, gather(120));
                String digest = journal.entry(2).digest();
                for (String idempotencyKey : List.of("deny-3", "deny-4")) {
                    assertEquals("PLAYER_EXISTS", rejection(() -> game.createPlayer(active, idempotencyKey, named("Alice"))));
                }
                mysql.setJdbcUrl(System.getenv("SLG_PROJECTION_JDBC_URL"));
                mysql.setUsername(System.getenv("SLG_PROJECTION_DB_USER"));
                mysql.setPassword(System.getenv("SLG_PROJECTION_DB_PASSWORD"));
                mysql.setMaximumPoolSize(2);
                var store = new MySqlProjectionStore(mysql, REALM, id);
                assertEquals(3, store.apply(journal.entriesAfter(0, 3)));
                assertEquals(4, journal.highSequence());
                isolated.checkpoint();
                // 取证失败不能借上一次的游标继续删除；使用已关闭的连接池模拟不能取得这次 SQL 锁。
                try (var offline = new HikariDataSource()) {
                    offline.close();
                    assertEquals("BLOCKED", isolated.clean(offline, 1).state());
                }
                assertEquals(0, journal.inspectPage(0, 32).prunedThrough());
                assertTrue(isolated.retains(1) && isolated.retains(2));
                var plan = isolated.clean(mysql, 1);
                assertEquals("CANDIDATE", plan.state());
                assertEquals(1, plan.firstEligibleSeq());
                assertEquals(2, plan.lastEligibleSeq());
                assertEquals(List.of("consumer:mysql"), plan.limitingFactors());
                assertEquals(3, store.checkpoint(), "The cleanup cannot move the SQL cursor");
                assertFalse(isolated.retains(1) || isolated.retains(2));
                assertTrue(isolated.retains(3) && isolated.retains(4));
                var retained = journal.entriesAfter(2, 32).stream().map(RealmJournal.Entry::sequence).toList();
                assertEquals(List.of(3L, 4L), retained);
                assertThrows(RealmJournal.InvalidPrefix.class, () -> journal.entry(2));
                assertEquals(2, journal.lookup(PLAYER, REQUEST, digest).sequence());

                Path partial = root.resolve("cleanup-committed.tmp");
                Files.writeString(partial, Json.write(Map.of("departure", sent.toByteArray(), "digest", digest, "retained", retained)));
                Files.move(partial, root.resolve("cleanup-committed.json"), StandardCopyOption.ATOMIC_MOVE);
                // 保持数据库及文件锁原样；父进程将在此强杀，不给此进程优雅关闭的机会。
                new CountDownLatch(1).await();
            }
        }
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();

    private static boolean caughtUp(int port, UUID volume, long expectedSeq) {
        try {
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
                    .timeout(Duration.ofSeconds(2)).build();
            var h = Json.MAPPER.readTree(HTTP.send(request, HttpResponse.BodyHandlers.ofString()).body());
            var projection = h.path("projection");
            return h.path("status").asText().equals("UP") && h.path("volumeId").asText().equals(volume.toString())
                    && h.path("journalSeq").asLong() == expectedSeq && projection.path("state").asText().equals("CAUGHT_UP")
                    && projection.path("lastAppliedSeq").asLong() == expectedSeq && projection.path("lag").asLong(-1) == 0;
        } catch (IOException connecting) { return false; }
        catch (InterruptedException stopped) { Thread.currentThread().interrupt(); throw new AssertionError(stopped); }
    }

    private static ServerSocket port() throws IOException {
        return new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    }

    private static Child launch(Path root, Map<String, String> env, String name, Class<?> main, String... args) throws IOException {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx160m", "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), main.getName()));
        command.addAll(List.of(args));
        Path log = root.resolve(name + ".log");
        var builder = new ProcessBuilder(command).directory(root.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
        // 移除宿主的所有 SLG_*（包括真实区服路径、目标、密钥和数据库配置），再注入本次测试独有的值。
        builder.environment().keySet().removeIf(key -> key.startsWith("SLG_"));
        builder.environment().putAll(env);
        return new Child(builder.start(), log);
    }

    private record Child(Process process, Path log) implements AutoCloseable {
        void await(BooleanSupplier ready, String timeout) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(65);
            while (process.isAlive() && System.nanoTime() < deadline) {
                if (ready.getAsBoolean()) return;
                Thread.sleep(100);
            }
            fail(timeout + ": " + Files.readString(log));
        }

        void kill() throws InterruptedException {
            assertTrue(process.isAlive(), "Expected the child to still own its volume");
            process.destroyForcibly();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Child did not exit after forced termination");
            assertNotEquals(0, process.exitValue(), "Forced termination unexpectedly completed normally");
        }

        @Override public void close() throws InterruptedException {
            if (process.isAlive()) kill();
        }
    }
}

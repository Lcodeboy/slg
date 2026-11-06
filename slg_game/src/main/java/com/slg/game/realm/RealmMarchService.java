package com.slg.game.realm;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.MessageLite;
import com.slg.core.battle.BattleInput;
import com.slg.core.battle.BattleResult;
import com.slg.core.protocol.Json;
import com.slg.game.domain.GameException;
import com.slg.game.realm.ActivePlayer.Hero;
import com.slg.game.realm.ActivePlayer.Player;
import com.slg.game.storage.RealmJournal;
import com.slg.game.storage.RealmJournal.Change;
import com.slg.game.storage.RealmJournal.Decision;
import com.slg.game.storage.RealmJournal.Receipt;
import com.slg.game.storage.RealmJournal.Value;
import com.slg.game.realm.MarchTasks.March;
import com.slg.game.realm.MarchTasks.Task;
import com.slg.rpc.generated.GameplayRoutes;
import com.slg.rpc.v1.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.function.LongPredicate;
import java.util.function.BooleanSupplier;

import static com.slg.game.domain.GameException.require;

/** 新区服的建角与派兵。按认证账号串行，操作共享矿点时还需持有目标执行资格；本地提交后发布草案。 */
public final class RealmMarchService {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final String STARTER_HERO = "sergey";
    static final String RULES_VERSION = "realm-march-v1";
    private final RealmJournal journal;
    private final int realmId;
    private final Clock clock;
    private final MarchTasks tasks;
    private final NpcBattles battles;
    private final LongPredicate admitClientWrite;
    private final BooleanSupplier diskAvailable;

    public RealmMarchService(RealmJournal journal, Clock clock) throws IOException {
        this(journal, clock, ignored -> true);
    }

    public RealmMarchService(RealmJournal journal, Clock clock, LongPredicate admitClientWrite) throws IOException {
        this(journal, clock, admitClientWrite, () -> true);
    }

    public RealmMarchService(RealmJournal journal, Clock clock, LongPredicate admitClientWrite, BooleanSupplier diskAvailable)
            throws IOException {
        this.journal = Objects.requireNonNull(journal);
        this.realmId = journal.roleRealmId();
        this.clock = Objects.requireNonNull(clock);
        this.admitClientWrite = Objects.requireNonNull(admitClientWrite);
        this.diskAvailable = Objects.requireNonNull(diskAvailable);
        tasks = new MarchTasks(journal, clock);
        battles = new NpcBattles(journal, clock);
    }

    /** 扫描器仅发现到期事项；调用方持有所属玩家的执行资格，采集到达、采集完成还要持有矿点资格。 */
    public boolean applyDue(ActivePlayer active, RealmJournal.DueTask task) throws IOException {
        return tasks.applyDue(active, task);
    }

    /** 派发前验证卷内保存的不可变输入。耗时的 Worker 调用在邮箱外执行。 */
    public BattleInput battleInput(Value job, String battleId) throws IOException {
        return NpcBattles.battleInput(job, battleId);
    }

    /** Worker 返回后，重新进入所属玩家邮箱，验证并只结算一次。 */
    public boolean settleBattle(ActivePlayer active, String battleId, String inputHash, String rulesVersion,
                                BattleResult result) throws IOException {
        return battles.settle(active, battleId, inputHash, rulesVersion, result);
    }

    private record RoleBinding(String playerId) {}

    /** 本区最多有一个归属该账号的角色。登录从本地目录解析，不用当前区服号重算一个已存在的角色。 */
    public ActivePlayer loadPlayer(String accountId) throws IOException {
        String candidate = PlayerIdentity.atCreation(realmId, accountId);
        var snapshot = journal.readLinkedSnapshot("account-role:" + accountId, binding -> {
            if (binding == null) return "player:" + candidate;
            String target = decode(binding.json(), RoleBinding.class).playerId();
            try {
                if (!accountId.equals(PlayerIdentity.accountOf(target))) throw new IOException("Role binding owner mismatch");
            } catch (IllegalArgumentException malformed) {
                throw new IOException("Invalid role binding", malformed);
            }
            return "player:" + target;
        });
        Value value = snapshot.target();
        if ((value == null) != (snapshot.binding() == null)) throw new IOException("Incomplete account-to-role binding");
        Player data = value == null ? null : decodePlayer(value);
        if (data != null && (!accountId.equals(data.accountId()) || data.currentRealmId() != realmId
                || !snapshot.targetKey().equals("player:" + data.playerId())))
            throw new IOException("Stored player differs from the role binding or current realm");
        return new ActivePlayer(accountId, realmId, data, value == null ? 0 : value.version(), snapshot.sequence());
    }

    /** 身份使用严格的线协议类型，避免反序列化把数值账号或小数区服号悄悄转换成另一份合法身份。 */
    private static Player decodePlayer(Value stored) throws IOException {
        JsonNode data = Json.MAPPER.readTree(stored.json());
        if (!data.path("playerId").isTextual() || !data.path("accountId").isTextual()
                || !validRealmField(data.path("foundingRealmId")) || !validRealmField(data.path("currentRealmId")))
            throw new IOException("Invalid stored player identity fields");
        return Json.MAPPER.treeToValue(data, Player.class);
    }

    private static boolean validRealmField(JsonNode field) {
        return field.isIntegralNumber() && field.canConvertToInt() && field.intValue() > 0;
    }

    /** 业务方法仅接收已认证邮箱持有的玩家。所有读取也检查原卷是否仍可使用，故障后不能返回旧内存。 */
    private void checkActive(ActivePlayer active, String id) throws IOException {
        require(id != null && ID.matcher(id).matches(), "INVALID_REQUEST", "Valid requestId required");
        if (!active.valid()) throw new IOException("Active player requires reload");
        journal.highSequence();
    }

    @FunctionalInterface private interface Read<T> { T load() throws IOException; }

    /** 关卷或读取了损坏的权威记录，必须先使缓存失效。正常业务拒绝仍能复用这个玩家。 */
    private <T> T read(ActivePlayer active, String id, Read<T> query) throws IOException {
        try {
            checkActive(active, id);
            return query.load();
        } catch (IOException unavailable) {
            active.invalidate();
            throw unavailable;
        }
    }

    public GetPlayerRsp getPlayer(ActivePlayer active, String id, GetPlayerReq request) throws IOException {
        return read(active, id, () -> {
            var snapshot = active.snapshot();
            return GetPlayerRsp.newBuilder().setPlayer(GameplayViews.player(requirePlayer(snapshot), snapshot.version())).build();
        });
    }

    public GetMarchRsp getMarch(ActivePlayer active, String id, GetMarchReq request) throws IOException {
        return read(active, id, () -> {
            String marchId = request.getMarchId();
            require(ID.matcher(marchId).matches(), "INVALID_ARGUMENT", "Invalid march ID");
            Value value = journal.read("march:" + marchId);
            require(value != null, "MARCH_NOT_FOUND", "March not found");
            March march = decode(value.json(), March.class);
            require(march.playerId().equals(active.playerId()), "MARCH_NOT_FOUND", "March not found");
            return GetMarchRsp.newBuilder().setMarch(GameplayViews.march(march)).build();
        });
    }

    public GetBattleRsp getBattle(ActivePlayer active, String id, GetBattleReq request) throws IOException {
        return read(active, id, () -> {
            String battleId = request.getBattleId();
            require(ID.matcher(battleId).matches(), "INVALID_ARGUMENT", "Invalid battle ID");
            return GameplayViews.battle(battles.report(active.playerId(), battleId));
        });
    }

    /** 只从角色私有的 R 键查回已提交决定。日志允许清前缀，MySQL 可能落后，都不能回答此查询。
     * 缺少回执仅表示本次读取尚未发现它，另一个连接的请求仍可能在途。 */
    public GetReceiptRsp getReceipt(ActivePlayer active, String id, GetReceiptReq request) throws IOException {
        return read(active, id, () -> {
            String target = request.getTargetRequestId();
            require(ID.matcher(target).matches(), "INVALID_ARGUMENT", "Valid targetRequestId required");
            var response = GetReceiptRsp.newBuilder().setTargetRequestId(target);
            var saved = journal.findReceipt(active.playerId(), target);
            if (saved != null) response.setJournalSeq(saved.sequence()).setReceipt(decodeReceipt(saved));
            return response.build();
        });
    }

    public StartMarchRsp startMarch(ActivePlayer active, String id, StartMarchReq request) throws IOException {
        var order = new StartMarchOrder(request);
        return mutate(active, id, GameplayRoutes.START_MARCH, order.canonicalRequest(),
                () -> proposeMarch(active.snapshot(), id, order)).getStarted();
    }

    public CreatePlayerRsp createPlayer(ActivePlayer active, String id, CreatePlayerReq request) throws IOException {
        return mutate(active, id, GameplayRoutes.CREATE_PLAYER, request,
                () -> proposeCreation(active, request)).getCreated();
    }

    public RecallMarchRsp recallMarch(ActivePlayer active, String id, RecallMarchReq request) throws IOException {
        return mutate(active, id, GameplayRoutes.RECALL_MARCH, request, () -> {
            String marchId = request.getMarchId();
            require(ID.matcher(marchId).matches(), "INVALID_ARGUMENT", "Invalid march ID");
            var recall = tasks.recall(active, marchId);
            var result = RecallMarchRsp.newBuilder().setMarch(GameplayViews.march(recall.march()));
            return new Proposal("MARCH_RECALLED", recall.march(), WriteReceipt.newBuilder().setRecalled(result),
                    recall.changes(), null);
        }).getRecalled();
    }

    @FunctionalInterface private interface Prepare { Proposal run() throws IOException; }

    /** 开始执行业务之前先按玩家、请求 ID 和内容指纹查重。生成的操作编号也参与摘要，
     * 因此建角和出征即使使用同一个请求 ID，也不能取走对方的成功回执。 */
    private WriteReceipt mutate(ActivePlayer active, String id, int msgId, MessageLite request, Prepare prepare) throws IOException {
        try {
            checkActive(active, id);
            MessageDigest hash = sha256();
            hash.update(ByteBuffer.allocate(Integer.BYTES).putInt(msgId).array());
            String digest = HexFormat.of().formatHex(hash.digest(request.toByteArray()));
            Receipt previous = journal.lookup(active.playerId(), id, digest);
            WriteReceipt result;
            if (previous != null) {
                active.replayed(previous.sequence());
                result = decodeReceipt(previous);
            } else {
                result = commitMutation(active, id, msgId, digest, prepare);
            }
            if (result.getMsgId() != msgId) throw new IOException("Stored receipt has a different operation");
            if (result.hasError()) throw new GameException(result.getError().getCode(), result.getError().getMessage());
            return result;
        } catch (RealmJournal.Conflict conflict) {
            throw new GameException("REQUEST_CONFLICT", "Request ID was already used for different input");
        } catch (IOException failure) {
            active.invalidate();
            throw failure;
        }
    }

    /** R/J 仍共享存储层的 JSON 列。玩家的结果是同一份 WriteReceipt 的原始字节，以 Base64 字符串保存。
     * 重启只解析这一种格式。一次性拒绝（背压、磁盘保护）根本不会生成此记录。 */
    private static WriteReceipt decodeReceipt(Receipt saved) throws IOException {
        JsonNode encoded = Json.MAPPER.readTree(saved.resultJson());
        if (encoded == null || !encoded.isTextual()) throw new IOException("Invalid typed receipt encoding");
        WriteReceipt result;
        try {
            result = WriteReceipt.parseFrom(encoded.binaryValue());
        } catch (IOException malformed) {
            throw new IOException("Corrupt stored gameplay receipt", malformed);
        }
        boolean valid = switch (result.getMsgId()) {
            case GameplayRoutes.CREATE_PLAYER -> result.hasCreated() && result.getCreated().hasPlayer();
            case GameplayRoutes.START_MARCH -> result.hasStarted() && result.getStarted().hasMarch();
            case GameplayRoutes.RECALL_MARCH -> result.hasRecalled() && result.getRecalled().hasMarch();
            default -> throw new IOException("Unknown operation in stored gameplay receipt");
        };
        if (!valid && !(result.hasError() && !result.getError().getCode().isBlank()))
            throw new IOException("Stored gameplay receipt outcome disagrees with its operation");
        return result;
    }

    private WriteReceipt commitMutation(ActivePlayer active, String id, int msgId, String digest, Prepare prepare)
            throws IOException {
        Proposal proposal;
        try {
            proposal = prepare.run();
        } catch (GameException rejected) {
            // 确定性的玩法拒绝也写回执，避免同一请求随玩家状态变化突然生效。
            var error = BusinessError.newBuilder().setCode(rejected.code()).setMessage(rejected.getMessage());
            proposal = new Proposal("COMMAND_REJECTED", Map.of("msgId", msgId, "code", rejected.code()),
                    WriteReceipt.newBuilder().setError(error), List.of(), null);
        }
        var result = proposal.result().setMsgId(msgId).build();
        var decision = new Decision(active.playerId(), id, digest, proposal.type(), Json.write(proposal.fact()),
                Json.write(result.toByteArray()), proposal.changes());
        var snapshot = active.snapshot(); // 召回可能已先在该玩家和矿点的执行资格内补做一个到期阶段。
        try {
            var outcome = journal.commitAdmitted(decision, high -> {
                // 资源不足时给后台返城保留写空间。两个准入判定都不能在提交锁内访问数据库。
                if (!diskAvailable.getAsBoolean()) return RealmJournal.Admission.DISK_PRESSURE;
                return admitClientWrite.test(high) ? RealmJournal.Admission.ALLOW : RealmJournal.Admission.PROJECTION_BACKLOG;
            });
            var committed = outcome.receipt();
            if (outcome.applied()) {
                active.committed(committed.sequence(), snapshot.version(), proposal.next());
                return result;
            }
            active.replayed(committed.sequence());
            return decodeReceipt(committed);
        } catch (RealmJournal.WriteDeferred busy) {
            if (busy.reason == RealmJournal.Admission.DISK_PRESSURE)
                throw new GameException("DISK_PRESSURE", "Realm disk budget unavailable; retry with the same requestId");
            throw new GameException("PROJECTION_BACKLOG", "Realm backlog at limit; retry with the same requestId");
        } catch (RealmJournal.Stale stale) {
            active.invalidate();
            throw new GameException("STATE_CHANGED", "State changed before commit; retry with the same ID");
        } catch (RuntimeException | Error uncertain) {
            // 批次可能已经落盘。下一条操作必须从卷重新装载，再按原 ID 判断结果。
            active.invalidate();
            throw uncertain;
        }
    }

    private Proposal proposeCreation(ActivePlayer active, CreatePlayerReq request) {
        String name = request.getName().strip();
        require(!name.isEmpty() && name.length() <= 24 && name.codePoints().noneMatch(Character::isISOControl),
                "INVALID_ARGUMENT", "name is invalid");
        require(active.snapshot().player() == null, "PLAYER_EXISTS", "Character already exists");
        // 客户端只提供昵称。建角区来自已登记的原卷，账号来自已验签会话，首次建角的当前区就是建角区。
        Player initial = new Player(active.playerId(), active.accountId(), realmId, realmId, name,
                Map.of("INFANTRY", 3000L, "LANCER", 1000L, "MARKSMAN", 1000L),
                List.of(new Hero(STARTER_HERO, null)), List.of(), Map.of("WOOD", 0L), Map.of(), Map.of());
        // 两条记录都要求版本为零，且与回执在同一 WAL 批次。进程崩溃后不可能只建成角色或只留下占号。
        Change player = new Change("player:" + initial.playerId(), 0, Json.write(initial));
        Change binding = new Change("account-role:" + active.accountId(), 0, Json.write(new RoleBinding(initial.playerId())));
        var result = CreatePlayerRsp.newBuilder().setPlayer(GameplayViews.player(initial, 1));
        return new Proposal("PLAYER_CREATED", initial, WriteReceipt.newBuilder().setCreated(result), List.of(player, binding), initial);
    }

    private Proposal proposeMarch(ActivePlayer.Snapshot snapshot, String id, StartMarchOrder order) throws IOException {
        order.validateCoordinatesAndTier();
        int x = order.x(), y = order.y();
        String mission = order.mission();
        validateTarget(mission, x, y);
        Player player = requirePlayer(snapshot);
        require(player.activeMarchIds().size() < 3, "MARCH_LIMIT", "All march queues are occupied");
        Map<String, Long> selected = order.troops();
        Map<String, Long> available = new HashMap<>(player.troops());
        for (var group : selected.entrySet()) {
            long inCity = available.getOrDefault(group.getKey(), 0L);
            require(inCity >= group.getValue(), "INSUFFICIENT_TROOPS", "Not enough available troops");
            available.put(group.getKey(), inCity - group.getValue());
        }
        List<String> heroIds = order.heroes(STARTER_HERO);
        Hero hero = player.heroes().getFirst();
        if (!heroIds.isEmpty())
            require(hero.assignedMarch() == null, "HERO_BUSY", "Hero is already assigned to a march");

        // 确定的 ID 和服务端时间写入事实；回放不会按新时钟或新规则重新出征。
        String playerId = player.playerId();
        String marchId = hash("march-v1\n" + playerId + "\n" + id).substring(0, 32);
        long depart = clock.millis();
        long travelSeconds = Math.max(3, (long) Math.ceil(Math.hypot(x, y) / 100.0));
        long arrive = Math.addExact(depart, travelSeconds * 1000L);
        String taskId = "arrive-" + marchId;
        March march = new March(marchId, playerId, "OUTBOUND", mission, 0, 0, x, y, depart, arrive,
                1, selected, heroIds, taskId, "GATHER".equals(mission) ? GatherSite.RULES_VERSION : RULES_VERSION, Map.of());
        Task task = new Task(taskId, playerId, marchId, "MARCH_ARRIVE", "PENDING", 1, arrive, null);
        List<String> active = new ArrayList<>(player.activeMarchIds());
        active.add(marchId);
        List<Hero> heroes = heroIds.isEmpty() ? player.heroes() : List.of(new Hero(hero.heroId(), marchId));
        Player updated = player.withArmy(available, heroes, active, player.resources(),
                player.wounded(), player.dead());
        var result = StartMarchRsp.newBuilder().setMarch(GameplayViews.march(march));
        return new Proposal("MARCH_STARTED", march, WriteReceipt.newBuilder().setStarted(result), List.of(
                new Change("player:" + playerId, snapshot.version(), Json.write(updated)),
                new Change("march:" + marchId, 0, Json.write(march)),
                new Change("task:" + taskId, 0, Json.write(task))), updated);
    }

    private static Player requirePlayer(ActivePlayer.Snapshot snapshot) {
        require(snapshot.player() != null, "PLAYER_NOT_FOUND", "Create a character first");
        return snapshot.player();
    }

    private static void validateTarget(String mission, int x, int y) {
        boolean valid = switch (mission) {
            case "GATHER" -> GatherSite.exists(x, y);
            case "ATTACK_NPC" -> x == y && (x == 120 || x == 600 || x == 1100);
            default -> throw new GameException("INVALID_ARGUMENT", "Supported missions: GATHER, ATTACK_NPC");
        };
        require(valid, "INVALID_TARGET", "Target does not match the mission");
    }

    static String hash(String input) {
        return HexFormat.of().formatHex(sha256().digest(input.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 unavailable", e); }
    }

    private static <T> T decode(String json, Class<T> type) throws IOException {
        try {
            T result = Json.MAPPER.readValue(json, type);
            if (result == null) throw new IOException("Empty realm record");
            return result;
        } catch (IOException e) {
            throw new IOException("Invalid stored realm record", e);
        }
    }

    private record Proposal(String type, Object fact, WriteReceipt.Builder result, List<Change> changes, Player next) {}
}

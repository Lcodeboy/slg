package com.slg.game.infrastructure.mysql.repository;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.codec.CodecException;
import com.slg.game.infrastructure.mysql.mapper.StateMapper;
import com.slg.game.infrastructure.mysql.row.StateRows;
import com.slg.game.infrastructure.mysql.session.ReadMeter;
import org.apache.ibatis.session.SqlSession;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import static com.slg.game.application.port.StorageAccessException.Code.*;

/**
 * 结构化状态仓储。调用者拥有 Session 和事务：本类从不 commit，也不把半个聚合交给应用。
 * 写入前须按 ObjectKey.ORDER 锁完全部根，且所有 CAS/唯一键 INSERT 与明细替换都在 cursor 前完成。
 * 新建根的 last_seq=0 只在本事务内有效；事务协调器在 COMMIT 前必须逐根最终化。
 */
public final class MySqlStateRepository {
    // 与格式 v1 的单个明细集合上限一致；调用者的大 ReadBudget 不能解除解码前的内存保护。
    private static final int MAX_CHILDREN = 10_000;
    private final PersistenceCodec codec;

    public MySqlStateRepository(PersistenceCodec codec) { this.codec = Objects.requireNonNull(codec, "codec"); }

    public long lockVersion(SqlSession session, int realmId, ObjectKey key) throws StorageAccessException {
        var row = session.getMapper(StateMapper.class).lockVersion(realmId, key);
        if (row == null) return 0;
        requireCommitted(row.version(), row.lastSeq());
        return row.version();
    }

    public void write(SqlSession session, int realmId, ObjectChange change) {
        codec.validateState(change.afterState());
        if (change.afterState() instanceof PlayerState player && player.currentRealmId() != realmId)
            throw new IllegalArgumentException("Player current realm does not match transaction realm");
        var mapper = session.getMapper(StateMapper.class);
        var row = toRow(change.afterState(), change.expectedVersion() + 1);
        int affected = change.expectedVersion() == 0 ? mapper.insert(realmId, row)
                : mapper.update(realmId, row, change.expectedVersion());
        if (affected != 1) throw new VersionConflict(change.key());

        // 先完成根 CAS，才允许删除旧明细；失败由外层回滚整笔事务，不能单独保留已改资产。
        if (change.afterState() instanceof MarchState march) {
            mapper.deleteMarchTroops(realmId, march.marchId());
            mapper.deleteMarchCargo(realmId, march.marchId());
            mapper.deleteMarchHeroes(realmId, march.marchId());
            march.troops().entrySet().stream().sorted(Comparator.comparingInt(e -> e.getKey().code()))
                    .forEach(e -> requireInserted(mapper.insertMarchTroop(realmId, march.marchId(),
                            new StateRows.MarchTroop(e.getKey(), e.getValue())), march.key()));
            march.cargo().entrySet().stream().sorted(Comparator.comparingInt(e -> e.getKey().code()))
                    .forEach(e -> requireInserted(mapper.insertMarchCargo(realmId, march.marchId(),
                            new StateRows.MarchCargo(e.getKey(), e.getValue())), march.key()));
            // 锁按 hero_id 主键顺序取得，slot_index 仍保存原业务顺序，二者不互相替代。
            IntStream.range(0, march.heroIds().size()).boxed().sorted(Comparator.comparing(march.heroIds()::get))
                    .forEach(slot -> requireInserted(mapper.insertMarchHero(realmId, march.marchId(),
                            new StateRows.HeroSlot(march.heroIds().get(slot), slot)), march.key()));
        } else if (change.afterState() instanceof BattleState battle) {
            mapper.deleteBattleHeroes(realmId, battle.battleId());
            IntStream.range(0, battle.heroIds().size()).boxed().sorted(Comparator.comparing(battle.heroIds()::get))
                    .forEach(slot -> requireInserted(mapper.insertBattleHero(realmId, battle.battleId(),
                            new StateRows.HeroSlot(battle.heroIds().get(slot), slot)), battle.key()));
        }
    }

    public void finalizeSequence(SqlSession session, int realmId, ObjectKey key, long sequence) {
        if (sequence <= 0) throw new IllegalArgumentException("Committed sequence must be positive");
        // 只能更新本事务已经持锁且写成功的根；此方法不发现新对象，也不读取其他业务表。
        if (session.getMapper(StateMapper.class).finalizeSequence(realmId, key, sequence) != 1)
            throw new VersionConflict(key);
    }

    /** 裸仓储读取仍有有限上限；端口快照使用下方重载共享其整次读取预算。 */
    public Optional<VersionedState> find(SqlSession session, int realmId, ObjectKey key) throws StorageAccessException {
        return find(session, realmId, key, new ReadMeter(new ReadBudget(10_000, 8L * 1024 * 1024, Duration.ofSeconds(30))));
    }

    public Optional<VersionedState> find(SqlSession session, int realmId, ObjectKey key, ReadMeter meter)
            throws StorageAccessException {
        return find(session, realmId, key, meter, Long.MAX_VALUE);
    }

    public Optional<VersionedState> find(SqlSession session, int realmId, ObjectKey key, ReadMeter meter, long visibleHigh)
            throws StorageAccessException {
        meter.checkTime();
        var rows = roots(session, key.kind(), StateRows.Query.exact(realmId, key).withPayloadBudget(meter.remainingBytes()));
        if (rows.isEmpty()) return Optional.empty();
        if (rows.size() != 1) throw invalid("Duplicate authority object key");
        var loaded = load(session, realmId, rows.getFirst(), meter);
        if (loaded.lastSequence() > visibleHigh) throw invalid("Authority state is beyond snapshot watermark");
        meter.add(loaded.rows(), loaded.bytes());
        if (!key.equals(loaded.state().state().key())) throw invalid("Authority state key mismatch");
        return Optional.of(loaded.state());
    }

    public List<? extends StateRows.Root> roots(SqlSession session, ObjectKind kind, StateRows.Query query) {
        var mapper = session.getMapper(StateMapper.class);
        return switch (kind) {
            case PLAYER -> mapper.players(query);
            case RESOURCE -> mapper.resources(query);
            case TROOP -> mapper.troops(query);
            case HERO -> mapper.heroes(query);
            case MARCH -> mapper.marches(query);
            case GATHER_SITE -> mapper.gatherSites(query);
            case TASK -> mapper.tasks(query);
            case BATTLE -> mapper.battles(query);
        };
    }

    /** 预算先约束 SQL 行数，再组装完整聚合；返回前尚未消耗 meter，页读取可在此完整边界截页。 */
    public Loaded load(SqlSession session, int realmId, StateRows.Root row, ReadMeter meter) throws StorageAccessException {
        return load(session, realmId, row, meter, false);
    }

    /** rootCharged 表示扫描已为本页全部根行记账，子表不能再次借用这些已读主行的行数预算。 */
    public Loaded load(SqlSession session, int realmId, StateRows.Root row, ReadMeter meter, boolean rootCharged)
            throws StorageAccessException {
        meter.checkTime();
        requireCommitted(row.version(), row.lastSeq());
        int rows = 1;
        int rowBudget = meter.remainingRows() + (rootCharged ? 1 : 0);
        if (rowBudget < rows) throw exceeded();
        try {
            ObjectState state;
            var mapper = session.getMapper(StateMapper.class);
            switch (row) {
                case StateRows.Player player -> {
                    if (player.currentRealmId() != realmId) throw invalid("Player belongs to another realm");
                    state = new PlayerState(player.playerId(), player.accountId(), player.foundingRealmId(), player.currentRealmId(), player.name());
                }
                case StateRows.Resource resource -> state = new ResourceState(resource.playerId(), resource.resourceType(), resource.amount());
                case StateRows.Troop troop -> state = new TroopState(troop.playerId(), troop.troopType(), troop.available(), troop.wounded(), troop.dead());
                case StateRows.Hero hero -> state = new HeroState(hero.playerId(), hero.heroId(), hero.assignedMarchId());
                case StateRows.March march -> {
                    var troops = mapper.marchTroops(realmId, march.marchId(), childLimit(meter, rowBudget, rows));
                    rows = count(meter, rowBudget, rows, troops.size());
                    var cargo = mapper.marchCargo(realmId, march.marchId(), childLimit(meter, rowBudget, rows));
                    rows = count(meter, rowBudget, rows, cargo.size());
                    var heroes = mapper.marchHeroes(realmId, march.marchId(), childLimit(meter, rowBudget, rows));
                    rows = count(meter, rowBudget, rows, heroes.size());
                    var troopAmounts = new EnumMap<TroopType, Long>(TroopType.class);
                    for (var troop : troops) if (troopAmounts.put(Objects.requireNonNull(troop.troopType()),
                            Objects.requireNonNull(troop.count())) != null) throw invalid("Duplicate march troop type");
                    var cargoAmounts = new EnumMap<ResourceType, Long>(ResourceType.class);
                    for (var item : cargo) if (cargoAmounts.put(Objects.requireNonNull(item.resourceType()),
                            Objects.requireNonNull(item.count())) != null) throw invalid("Duplicate march cargo type");
                    state = new MarchState(march.marchId(), march.playerId(), march.status(), march.mission(),
                            march.fromX(), march.fromY(), march.targetX(), march.targetY(), march.departAt(), march.arriveAt(),
                            march.tier(), troopAmounts, heroIds(heroes), march.taskId(), march.rulesVersion(), cargoAmounts);
                }
                case StateRows.GatherSite site -> state = new GatherSiteState(site.x(), site.y(), site.occupantPlayerId(), site.occupantMarchId(), site.generation());
                case StateRows.Task task -> state = new TaskState(task.taskId(), task.playerId(), task.marchId(), task.type(), task.status(), task.generation(), task.dueAt(), task.reason());
                case StateRows.Battle battle -> {
                    if (battle.inputBytes() != null && battle.inputBytes() > PersistenceCodec.DEFAULT_MAX_JSON_BYTES
                            || battle.resultBytes() != null && battle.resultBytes() > PersistenceCodec.DEFAULT_MAX_JSON_BYTES)
                        throw invalid("Battle payload exceeds stored format limit");
                    if (battle.inputJson() != null && Objects.requireNonNull(battle.inputBytes()) > battle.inputJson().length
                            || battle.resultJson() != null && Objects.requireNonNull(battle.resultBytes()) > battle.resultJson().length)
                        throw exceeded();
                    var heroes = mapper.battleHeroes(realmId, battle.battleId(), childLimit(meter, rowBudget, rows));
                    rows = count(meter, rowBudget, rows, heroes.size());
                    if ((battle.resultSchema() == null) != (battle.resultJson() == null)) throw invalid("Incomplete battle result payload");
                    state = new BattleState(battle.battleId(), battle.marchId(), battle.playerId(), battle.status(),
                            heroIds(heroes), battle.inputHash(), battle.rulesVersion(), battle.marchVersion(),
                            codec.decodeJsonPayload(battle.inputSchema(), battle.inputJson()), battle.resultSchema() == null ? null
                            : codec.decodeJsonPayload(battle.resultSchema(), battle.resultJson()));
                }
            }
            codec.validateState(state);
            long bytes = Math.addExact(16, codec.encodedStateBytes(state));
            meter.checkTime();
            return new Loaded(new VersionedState(row.version(), state), rows, bytes, row.lastSeq());
        } catch (CodecException failure) {
            throw new StorageAccessException(failure.unsupportedSchema() ? UNSUPPORTED_SCHEMA : INVALID_DATA,
                    "Invalid authority state payload");
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException failure) {
            throw new StorageAccessException(INVALID_DATA, "Invalid structured authority state");
        }
    }

    public record Loaded(VersionedState state, int rows, long bytes, long lastSequence) {}

    private static int childLimit(ReadMeter meter, int rowBudget, int rows) throws StorageAccessException {
        meter.checkTime();
        return Math.min(MAX_CHILDREN + 1, Math.max(1, rowBudget - rows + 1));
    }
    private static int count(ReadMeter meter, int rowBudget, int rows, int additional) throws StorageAccessException {
        meter.checkTime();
        if (additional > MAX_CHILDREN) throw invalid("Authority child collection exceeds stored format limit");
        if (additional > rowBudget - rows) throw exceeded();
        return rows + additional;
    }
    private static List<String> heroIds(List<StateRows.HeroSlot> rows) throws StorageAccessException {
        var result = new ArrayList<String>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            if (!Objects.equals(rows.get(i).slotIndex(), i)) throw invalid("Hero slot sequence has a gap or duplicate");
            result.add(Objects.requireNonNull(rows.get(i).heroId()));
        }
        return List.copyOf(result);
    }
    private static void requireCommitted(Long version, Long lastSeq) throws StorageAccessException {
        if (version == null || version <= 0 || lastSeq == null || lastSeq <= 0)
            throw invalid("Authority state has an invalid committed version or source sequence");
    }
    private static StorageAccessException invalid(String message) { return new StorageAccessException(INVALID_DATA, message); }
    private static StorageAccessException exceeded() { return new StorageAccessException(READ_BUDGET_EXCEEDED, "Authority aggregate exceeds read budget"); }
    private static void requireInserted(int count, ObjectKey key) { if (count != 1) throw new VersionConflict(key); }

    public static final class VersionConflict extends RuntimeException {
        private final ObjectKey key;
        public VersionConflict(ObjectKey key) { super("Authority root or child CAS failed"); this.key = key; }
        public ObjectKey key() { return key; }
    }

    private static StateRows.Root toRow(ObjectState state, long version) {
        return switch (state) {
            case PlayerState value -> new StateRows.Player(value.playerId(), value.accountId(), value.foundingRealmId(), value.currentRealmId(), value.name(), version, 0L);
            case ResourceState value -> new StateRows.Resource(value.playerId(), value.resourceType(), value.amount(), version, 0L);
            case TroopState value -> new StateRows.Troop(value.playerId(), value.troopType(), value.available(), value.wounded(), value.dead(), version, 0L);
            case HeroState value -> new StateRows.Hero(value.playerId(), value.heroId(), value.assignedMarchId(), version, 0L);
            case MarchState value -> new StateRows.March(value.marchId(), value.playerId(), value.status(), value.mission(), value.fromX(), value.fromY(), value.targetX(), value.targetY(), value.departAt(), value.arriveAt(), value.tier(), value.taskId(), value.rulesVersion(), version, 0L);
            case GatherSiteState value -> new StateRows.GatherSite(value.x(), value.y(), value.occupantPlayerId(), value.occupantMarchId(), value.generation(), version, 0L);
            case TaskState value -> new StateRows.Task(value.taskId(), value.playerId(), value.marchId(), value.type(), value.status(), value.generation(), value.dueAt(), value.reason(), version, 0L);
            case BattleState value -> new StateRows.Battle(value.battleId(), value.marchId(), value.playerId(), value.status(), value.inputHash(), value.rulesVersion(), value.marchVersion(), value.input().schemaVersion(), value.input().json().getBytes(StandardCharsets.UTF_8), value.result() == null ? null : value.result().schemaVersion(), value.result() == null ? null : value.result().json().getBytes(StandardCharsets.UTF_8), 0L, null, version, 0L);
        };
    }
}

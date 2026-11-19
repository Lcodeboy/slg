package com.slg.game.infrastructure.mysql.repository;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.game.application.port.ReadBudget;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.domain.persistence.*;
import com.slg.game.infrastructure.mysql.codec.PersistenceCodec;
import com.slg.game.infrastructure.mysql.mapper.StateMapper;
import com.slg.game.infrastructure.mysql.row.StateRows;
import com.slg.game.infrastructure.mysql.session.ReadMeter;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MySqlStateRepositoryTest {
    private final MySqlStateRepository repository = new MySqlStateRepository(new PersistenceCodec());
    private final Calls calls = new Calls();

    @Test void absentLockIsVersionZeroButCommittedZeroSourceSequenceIsCorruption() throws Exception {
        var key = new ObjectKey.Resource("1_7", ResourceType.WOOD);
        calls.results.put("lockVersion", null);
        assertEquals(0, repository.lockVersion(calls.session, 1, key));
        calls.results.put("lockVersion", new StateRows.Version(4L, 8L));
        assertEquals(4, repository.lockVersion(calls.session, 1, key));
        calls.results.put("lockVersion", new StateRows.Version(4L, 0L));
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> repository.lockVersion(calls.session, 1, key)).code());
    }

    @Test void rootCasFailureNeverDeletesOrInsertsChildren() {
        var march = march();
        calls.results.put("update", 0);
        assertThrows(MySqlStateRepository.VersionConflict.class,
                () -> repository.write(calls.session, 1, new ObjectChange(march.key(), 1, march)));
        assertEquals(List.of("update"), calls.names());
    }

    @Test void creationReservesPositiveVersionAndZeroSourceSequenceUntilFinalization() {
        var state = new ResourceState("1_7", ResourceType.WOOD, 99);
        repository.write(calls.session, 1, new ObjectChange(state.key(), 0, state));
        var row = (StateRows.Resource) calls.entries.getFirst().arguments()[1];
        assertEquals(1L, row.version());
        assertEquals(0L, row.lastSeq());
        assertEquals(99L, row.amount());
        repository.finalizeSequence(calls.session, 1, state.key(), 8);
        assertEquals(List.of("insert", "finalizeSequence"), calls.names());
        assertThrows(IllegalArgumentException.class, () -> repository.finalizeSequence(calls.session, 1, state.key(), 0));
    }

    @Test void detailWritesUseNumericCodesAndHeroKeysWhileKeepingOriginalHeroSlots() {
        var march = march();
        repository.write(calls.session, 1, new ObjectChange(march.key(), 1, march));
        assertEquals("update", calls.names().getFirst());
        var troopCodes = calls.entries.stream().filter(call -> call.name().equals("insertMarchTroop"))
                .map(call -> ((StateRows.MarchTroop) call.arguments()[2]).troopType().code()).toList();
        assertEquals(List.of(1, 2, 3), troopCodes);
        var resources = calls.entries.stream().filter(call -> call.name().equals("insertMarchCargo"))
                .map(call -> ((StateRows.MarchCargo) call.arguments()[2]).resourceType()).toList();
        assertEquals(List.of(ResourceType.WOOD, ResourceType.FOOD), resources);
        var heroes = calls.entries.stream().filter(call -> call.name().equals("insertMarchHero"))
                .map(call -> (StateRows.HeroSlot) call.arguments()[2]).toList();
        assertEquals(List.of(new StateRows.HeroSlot("a", 1), new StateRows.HeroSlot("z", 0)), heroes);
    }

    @Test void completeAggregateCountsEachPhysicalChildAndPreservesOrderedHeroes() throws Exception {
        childRows();
        var loaded = repository.load(calls.session, 1, marchRow(), meter(5));
        assertEquals(5, loaded.rows());
        var state = (MarchState) loaded.state().state();
        assertEquals(List.of("z", "a"), state.heroIds());
        assertEquals(Map.of(TroopType.INFANTRY, 10L), state.troops());
        assertEquals(Map.of(ResourceType.WOOD, 30L), state.cargo());
        assertEquals(7, loaded.lastSequence());
        assertTrue(loaded.bytes() > 0);
    }

    @Test void childLookaheadRejectsTruncationInsteadOfReturningAPartialMarch() {
        childRows();
        var failure = assertThrows(StorageAccessException.class,
                () -> repository.load(calls.session, 1, marchRow(), meter(4)));
        assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED, failure.code());
        var heroQuery = calls.entries.stream().filter(call -> call.name().equals("marchHeroes")).findFirst().orElseThrow();
        assertEquals(2, heroQuery.arguments()[2]);
    }

    @Test void prechargedRootRowsCannotBeBorrowedAgainByChildLoads() {
        childRows();
        var meter = meter(5);
        assertDoesNotThrow(() -> meter.add(2, 0)); // 一页两条主行，其中另一条尚未组装。
        assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED,
                assertThrows(StorageAccessException.class,
                        () -> repository.load(calls.session, 1, marchRow(), meter, true)).code());
    }

    @Test void missingOrDuplicateHeroSlotsAreInvalidData() {
        childRows();
        calls.results.put("marchHeroes", List.of(new StateRows.HeroSlot("z", 0), new StateRows.HeroSlot("a", 2)));
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class,
                        () -> repository.load(calls.session, 1, marchRow(), meter(10))).code());
    }

    @Test void nullSqlAmountDoesNotSilentlyBecomeAZeroBalance() {
        var row = new StateRows.Resource("1_7", ResourceType.WOOD, null, 1L, 1L);
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> repository.load(calls.session, 1, row, meter(10))).code());
    }

    @Test void sourceSequenceBeyondVisibleHighFailsTheWholeRead() {
        var row = new StateRows.Resource("1_7", ResourceType.WOOD, 100L, 1L, 5L);
        calls.results.put("resources", List.of(row));
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class,
                        () -> repository.find(calls.session, 1, row.key(), meter(10), 4)).code());
    }

    @Test void oversizedBattleBlobIsRejectedBeforePayloadParsing() {
        var row = new StateRows.Battle("battle", "march", "1_7", BattleState.Status.PENDING,
                "hash", "rules", 1L, 1, new byte[0], null, null, 1_000_000L, null, 1L, 1L);
        assertEquals(StorageAccessException.Code.READ_BUDGET_EXCEEDED,
                assertThrows(StorageAccessException.class, () -> repository.load(calls.session, 1, row, meter(10))).code());
        assertTrue(calls.names().isEmpty());
    }

    @Test void hugeCallerBudgetCannotLoadPayloadBeyondTheStoredFormatLimit() {
        var row = new StateRows.Battle("battle", "march", "1_7", BattleState.Status.PENDING,
                "hash", "rules", 1L, 1, new byte[0], null, null,
                (long) PersistenceCodec.DEFAULT_MAX_JSON_BYTES + 1, null, 1L, 1L);
        var meter = new ReadMeter(new ReadBudget(Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofSeconds(10)));
        var failure = assertThrows(StorageAccessException.class, () -> repository.load(calls.session, 1, row, meter));
        assertEquals(StorageAccessException.Code.INVALID_DATA, failure.code());
        assertNull(failure.getCause());
    }

    @Test void hugeCallerRowBudgetStillLimitsChildQueryBeforeMaterializingTheCollection() {
        calls.results.put("marchTroops", List.of());
        calls.results.put("marchCargo", List.of());
        calls.results.put("marchHeroes", Collections.nCopies(10_001, new StateRows.HeroSlot("hero", 0)));
        var meter = new ReadMeter(new ReadBudget(Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofSeconds(10)));
        assertEquals(StorageAccessException.Code.INVALID_DATA,
                assertThrows(StorageAccessException.class, () -> repository.load(calls.session, 1, marchRow(), meter)).code());
        var heroQuery = calls.entries.stream().filter(call -> call.name().equals("marchHeroes")).findFirst().orElseThrow();
        assertEquals(10_001, heroQuery.arguments()[2]);
    }

    private void childRows() {
        calls.results.put("marchTroops", List.of(new StateRows.MarchTroop(TroopType.INFANTRY, 10L)));
        calls.results.put("marchCargo", List.of(new StateRows.MarchCargo(ResourceType.WOOD, 30L)));
        calls.results.put("marchHeroes", List.of(new StateRows.HeroSlot("z", 0), new StateRows.HeroSlot("a", 1)));
    }
    private static ReadMeter meter(int rows) { return new ReadMeter(new ReadBudget(rows, 100_000, Duration.ofSeconds(10))); }
    private static StateRows.March marchRow() {
        return new StateRows.March("march", "1_7", MarchState.Status.OUTBOUND, MarchState.Mission.GATHER,
                0.0, 0.0, 100, 100, 0L, 1000L, 1, "task", "rules", 1L, 7L);
    }
    private static MarchState march() {
        return new MarchState("march", "1_7", MarchState.Status.OUTBOUND, MarchState.Mission.GATHER,
                0, 0, 100, 100, 0, 1000, 1,
                Map.of(TroopType.MARKSMAN, 30L, TroopType.INFANTRY, 10L, TroopType.LANCER, 20L),
                List.of("z", "a"), "task", "rules", Map.of(ResourceType.FOOD, 20L, ResourceType.WOOD, 30L));
    }

    private record Call(String name, Object[] arguments) {}
    private static final class Calls {
        final List<Call> entries = new ArrayList<>();
        final Map<String, Object> results = new HashMap<>();
        final StateMapper mapper = (StateMapper) Proxy.newProxyInstance(StateMapper.class.getClassLoader(),
                new Class<?>[]{StateMapper.class}, (proxy, method, arguments) -> {
                    entries.add(new Call(method.getName(), arguments));
                    if (results.containsKey(method.getName())) return results.get(method.getName());
                    if (method.getReturnType() == int.class) return 1;
                    throw new AssertionError("Unexpected mapper call: " + method.getName());
                });
        final SqlSession session = (SqlSession) Proxy.newProxyInstance(SqlSession.class.getClassLoader(),
                new Class<?>[]{SqlSession.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("getMapper") && arguments[0] == StateMapper.class) return mapper;
                    throw new AssertionError("Repository must not manage Session lifecycle: " + method.getName());
                });
        List<String> names() { return entries.stream().map(Call::name).toList(); }
    }
}

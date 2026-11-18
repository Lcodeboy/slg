package com.slg.core.type;

import com.slg.core.battle.TroopType;
import com.slg.core.economy.ResourceType;
import com.slg.core.protocol.Json;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DomainTypeCodeTest {
    @Test void assetCodesAreFixedContractsAndRoundTrip() {
        // 固定编码是外部契约；新增/重排声明不得悄悄改变已记录资产的含义。
        var resources = Map.of(ResourceType.WOOD, 1, ResourceType.FOOD, 2, ResourceType.GEMS, 3);
        assertEquals(resources.size(), ResourceType.values().length);
        resources.forEach((type, code) -> {
            assertEquals(code.intValue(), type.code());
            assertSame(type, ResourceType.fromCode(code));
        });
        var troops = Map.of(TroopType.INFANTRY, 1, TroopType.LANCER, 2, TroopType.MARKSMAN, 3);
        assertEquals(troops.size(), TroopType.values().length);
        troops.forEach((type, code) -> {
            assertEquals(code.intValue(), type.code());
            assertSame(type, TroopType.fromCode(code));
        });
    }

    @Test void unknownAssetCodesNeverBecomeDefaultResourcesOrTroops() {
        for (int code : new int[]{Integer.MIN_VALUE, -1, 0, 4, Integer.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> ResourceType.fromCode(code));
            assertThrows(IllegalArgumentException.class, () -> TroopType.fromCode(code));
        }
    }

    @Test void newPersistenceCodesPreserveExistingCombatOrderAndJsonNames() throws Exception {
        // 现有战斗引擎按枚举声明顺序选前排，旧战斗输入/回执 JSON 则按名称编解码。
        assertArrayEquals(new TroopType[]{TroopType.INFANTRY, TroopType.LANCER, TroopType.MARKSMAN},
                TroopType.values());
        for (TroopType type : TroopType.values()) {
            String encoded = "\"" + type.name() + "\"";
            assertEquals(encoded, Json.MAPPER.writeValueAsString(type));
            assertSame(type, Json.MAPPER.readValue(encoded, TroopType.class));
        }
    }
}

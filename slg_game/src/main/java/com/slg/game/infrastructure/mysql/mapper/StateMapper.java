package com.slg.game.infrastructure.mysql.mapper;

import com.slg.game.domain.persistence.ObjectKey;
import com.slg.game.infrastructure.mysql.row.StateRows;
import org.apache.ibatis.annotations.*;

import java.util.List;

/** 一份业务/快照只使用调用方持有的 Session；Mapper 不开连接、不提交，也不计算玩法。 */
public interface StateMapper {
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.Player> players(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.Resource> resources(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.Troop> troops(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.Hero> heroes(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.March> marches(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.GatherSite> gatherSites(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.Task> tasks(@Param("q") StateRows.Query query);
    @SelectProvider(type=StateSql.class, method="select")
    List<StateRows.Battle> battles(@Param("q") StateRows.Query query);

    @SelectProvider(type=StateSql.class, method="lockVersion")
    StateRows.Version lockVersion(@Param("realmId") int realmId, @Param("key") ObjectKey key);
    @InsertProvider(type=StateSql.class, method="insert")
    int insert(@Param("realmId") int realmId, @Param("row") StateRows.Root row);
    @UpdateProvider(type=StateSql.class, method="update")
    int update(@Param("realmId") int realmId, @Param("row") StateRows.Root row, @Param("expectedVersion") long expectedVersion);
    @UpdateProvider(type=StateSql.class, method="finalizeSequence")
    int finalizeSequence(@Param("realmId") int realmId, @Param("key") ObjectKey key, @Param("sequence") long sequence);

    @Select("SELECT troop_type,`count` FROM game_march_troop WHERE realm_id=#{realmId} AND march_id=#{id} ORDER BY troop_type LIMIT #{limit}")
    List<StateRows.MarchTroop> marchTroops(@Param("realmId") int realmId, @Param("id") String id, @Param("limit") int limit);
    @Select("SELECT resource_type,`count` FROM game_march_cargo WHERE realm_id=#{realmId} AND march_id=#{id} ORDER BY resource_type LIMIT #{limit}")
    List<StateRows.MarchCargo> marchCargo(@Param("realmId") int realmId, @Param("id") String id, @Param("limit") int limit);
    @Select("SELECT hero_id,slot_index FROM game_march_hero WHERE realm_id=#{realmId} AND march_id=#{id} ORDER BY slot_index LIMIT #{limit}")
    List<StateRows.HeroSlot> marchHeroes(@Param("realmId") int realmId, @Param("id") String id, @Param("limit") int limit);
    @Select("SELECT hero_id,slot_index FROM game_battle_hero WHERE realm_id=#{realmId} AND battle_id=#{id} ORDER BY slot_index LIMIT #{limit}")
    List<StateRows.HeroSlot> battleHeroes(@Param("realmId") int realmId, @Param("id") String id, @Param("limit") int limit);

    @Delete("DELETE FROM game_march_troop WHERE realm_id=#{realmId} AND march_id=#{id}")
    int deleteMarchTroops(@Param("realmId") int realmId, @Param("id") String id);
    @Delete("DELETE FROM game_march_cargo WHERE realm_id=#{realmId} AND march_id=#{id}")
    int deleteMarchCargo(@Param("realmId") int realmId, @Param("id") String id);
    @Delete("DELETE FROM game_march_hero WHERE realm_id=#{realmId} AND march_id=#{id}")
    int deleteMarchHeroes(@Param("realmId") int realmId, @Param("id") String id);
    @Delete("DELETE FROM game_battle_hero WHERE realm_id=#{realmId} AND battle_id=#{id}")
    int deleteBattleHeroes(@Param("realmId") int realmId, @Param("id") String id);

    @Insert("INSERT INTO game_march_troop (realm_id,march_id,troop_type,`count`) VALUES (#{realmId},#{id},#{row.troopType},#{row.count})")
    int insertMarchTroop(@Param("realmId") int realmId, @Param("id") String id, @Param("row") StateRows.MarchTroop row);
    @Insert("INSERT INTO game_march_cargo (realm_id,march_id,resource_type,`count`) VALUES (#{realmId},#{id},#{row.resourceType},#{row.count})")
    int insertMarchCargo(@Param("realmId") int realmId, @Param("id") String id, @Param("row") StateRows.MarchCargo row);
    @Insert("INSERT INTO game_march_hero (realm_id,march_id,hero_id,slot_index) VALUES (#{realmId},#{id},#{row.heroId},#{row.slotIndex})")
    int insertMarchHero(@Param("realmId") int realmId, @Param("id") String id, @Param("row") StateRows.HeroSlot row);
    @Insert("INSERT INTO game_battle_hero (realm_id,battle_id,hero_id,slot_index) VALUES (#{realmId},#{id},#{row.heroId},#{row.slotIndex})")
    int insertBattleHero(@Param("realmId") int realmId, @Param("id") String id, @Param("row") StateRows.HeroSlot row);
}

package com.slg.game.application.player;

import com.slg.game.application.port.RealmReceiptReader;
import com.slg.game.domain.player.PlayerDetails;
import com.slg.game.domain.player.PlayerSnapshot;

/**
 * 玩家输入、原响应及纯读结果的有界版本化编码端口。应用层不依赖JSON/Protobuf或MyBatis；
 * 实现必须严格校验完整结构和UTF-8，枚举按固定code编码，拒绝坏数据而非补默认资产。
 */
public interface PlayerPayloadCodec {
    byte[] encodeInvocation(PlayerInvocation invocation);
    PlayerInvocation decodeInvocation(byte[] bytes);
    String createDigest(String rawName);
    String encodeOutcome(PlayerOutcome outcome);
    PlayerOutcome decodeOutcome(String json);
    String creationFact(long accountId, String playerId, String rejectionCode);
    byte[] encodeDetails(PlayerDetails details);
    PlayerDetails decodeDetails(byte[] bytes);
    byte[] encodeObservation(RealmReceiptReader.Observation observation);
    RealmReceiptReader.Observation decodeObservation(byte[] bytes);
    long measureSnapshot(PlayerSnapshot snapshot);
    int maxDetailsBytes();
    int maxResultBytes();
}

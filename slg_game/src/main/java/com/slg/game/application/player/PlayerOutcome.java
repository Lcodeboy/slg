package com.slg.game.application.player;

import com.slg.game.domain.player.PlayerDetails;
import com.slg.game.domain.persistence.MarchState;
import java.util.Objects;

/** 持久原响应的类型化模型；业务拒绝也是已提交决定，基础设施未受理不生成本对象。 */
public record PlayerOutcome(String operation, PlayerDetails player, MarchState march, String errorCode, String errorMessage) {
    public static final String CREATE = "CREATE_PLAYER";
    public static final String START = "START_MARCH";
    public static final String RECALL = "RECALL_MARCH";

    public PlayerOutcome(String operation, PlayerDetails player, String errorCode, String errorMessage) {
        this(operation, player, null, errorCode, errorMessage);
    }
    public PlayerOutcome {
        if (!CREATE.equals(operation) && !START.equals(operation) && !RECALL.equals(operation))
            throw new IllegalArgumentException("Unsupported player receipt operation");
        if (player == null && march == null) {
            if (errorCode == null || errorCode.isBlank() || errorCode.length() > 128)
                throw new IllegalArgumentException("Missing business rejection code");
            Objects.requireNonNull(errorMessage);
        } else {
            if ((player != null) == (march != null) || errorCode != null || errorMessage != null)
                throw new IllegalArgumentException("Ambiguous player outcome");
            if (CREATE.equals(operation) != (player != null))
                throw new IllegalArgumentException("Receipt result differs from operation");
            if (march != null && ((START.equals(operation) && march.status() != MarchState.Status.OUTBOUND)
                    || (RECALL.equals(operation) && march.status() != MarchState.Status.RETURNING)))
                throw new IllegalArgumentException("Original march result has an invalid stage");
        }
    }
    public static PlayerOutcome created(PlayerDetails details) { return new PlayerOutcome(CREATE, details, null, null); }
    public static PlayerOutcome rejected(String code, String message) { return new PlayerOutcome(CREATE, null, code, message); }
    public static PlayerOutcome started(MarchState march) { return new PlayerOutcome(START, null, march, null, null); }
    public static PlayerOutcome recalled(MarchState march) { return new PlayerOutcome(RECALL, null, march, null, null); }
    public static PlayerOutcome rejected(String operation, String code, String message) {
        return new PlayerOutcome(operation, null, null, code, message);
    }
}

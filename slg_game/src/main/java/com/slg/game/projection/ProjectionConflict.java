package com.slg.game.projection;

import java.sql.SQLException;

/** 投影的身份、顺序或内容与本区已提交历史矛盾；重复尝试同一条写入无法修复这种错误。 */
public final class ProjectionConflict extends SQLException {
    /** 原因只包含定位信息（如日志序号或对象键），不要将玩家状态、完整请求或连接凭证写入异常。 */
    public ProjectionConflict(String safeReason) {
        super(safeReason, "45000");
    }
}

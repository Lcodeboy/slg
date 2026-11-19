package com.slg.game.application.commit;

import com.slg.game.domain.persistence.WriterToken;

/** 无 I/O 的进程内观察；不包含请求正文，也不授予写入或释放对象资格。 */
public record CommitQueueSnapshot(Mode mode, WriterToken writer, int acceptedTasks, long retainedBytes,
                                   int queued, int executing, int reconciling, int publicationHolds) {
    public enum Mode { ACTIVE, ISOLATED, OWNER_LOST, DRAINING, CLOSED }
}

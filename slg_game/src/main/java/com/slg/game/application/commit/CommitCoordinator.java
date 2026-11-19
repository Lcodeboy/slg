package com.slg.game.application.commit;

import com.slg.game.domain.persistence.WriterToken;
import java.util.Optional;

/** 应用调度使用的提交与写权观察边界；观察 token 不代替提交时的原子准入及数据库核验。 */
public interface CommitCoordinator extends RealmCommitService {
    /** 仅在可受理新业务时返回有效候选；准备期间任期可能变化，旧决定不得换 token 盲重投。 */
    Optional<WriterToken> currentWriter();

    /**
     * 已提交结果无法发布时暂停本区新写。每次隔离返回独立屏障，避免一项修复解除另一项隔离。
     * 保持对象资格并完成受影响缓存的失效/重载后才能释放；数据库结果不会因此回滚。
     */
    PublicationBarrier isolatePublication();

    interface PublicationBarrier {
        /** 幂等释放本屏障；内部 UNKNOWN 仍未收敛时，释放不能让提交器重新受理业务。 */
        void releaseAfterReload();
    }
}

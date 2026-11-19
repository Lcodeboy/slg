package com.slg.game;

/** GAME 只从显式绑定的 MySQL 权威数据集启动；普通启动不初始化库或回退旧存储。 */
public final class GameServer {
    private GameServer() {}

    public static void main(String[] args) {
        if (args.length == 0) {
            String realm = System.getenv("SLG_REALM_ID");
            String dataset = System.getenv("SLG_REALM_DATASET_ID");
            if (realm != null && !realm.isBlank() && dataset != null && !dataset.isBlank()) {
                args = new String[]{"realm", "serve", realm, dataset};
            }
        }
        int result = RealmCommand.run(args, System.out, System.err);
        if (result != 0) System.exit(result);
    }
}

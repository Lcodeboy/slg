package com.slg.game;

/** GAME 只从显式绑定的区服原卷启动；管理命令单独负责初始化与校验。 */
public final class GameServer {
    private GameServer() {}

    /** 邮箱工作线程与全局、单玩家的排队预算独立于数据库连接池。 */
    record Settings(int workers, int queueCapacity, int perAccountCapacity, int deadlineMillis) {
        Settings {
            if (workers < 1 || workers > 62 || queueCapacity < workers || queueCapacity > 16_384
                    || perAccountCapacity < 1 || perAccountCapacity > queueCapacity
                    || deadlineMillis < 1 || deadlineMillis > 5000)
                throw new IllegalArgumentException("Invalid GAME execution settings");
        }
        static Settings fromEnvironment(int workers) {
            return new Settings(number("SLG_GAME_WORKERS", Math.min(8, workers)),
                    number("SLG_GAME_QUEUE_CAPACITY", 512), number("SLG_GAME_ACCOUNT_QUEUE_CAPACITY", 16),
                    number("SLG_GAME_DEADLINE_MS", 3000));
        }
        private static int number(String key, int fallback) {
            try { return Integer.parseInt(System.getenv().getOrDefault(key, Integer.toString(fallback))); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid integer configuration: " + key); }
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            String directory = System.getenv("SLG_REALM_DIR");
            String realm = System.getenv("SLG_REALM_ID");
            String volume = System.getenv("SLG_REALM_VOLUME_ID");
            if (directory != null && !directory.isBlank() && realm != null && volume != null) {
                args = new String[]{"realm", "serve", directory, realm, volume};
            }
        }
        int result = RealmVolumeCommand.run(args, System.out, System.err);
        if (result != 0) System.exit(result);
    }
}

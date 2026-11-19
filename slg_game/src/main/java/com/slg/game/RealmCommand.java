package com.slg.game;

import com.slg.game.domain.persistence.RealmIdentity;
import com.slg.game.application.port.StorageAccessException;
import com.slg.game.infrastructure.mysql.runtime.AuthorityDatabaseSettings;
import com.slg.game.infrastructure.mysql.runtime.AuthorityDataSources;
import com.slg.game.infrastructure.mysql.runtime.MySqlStartupCheck;
import java.io.PrintStream;
import java.util.Map;
import java.util.UUID;

/**
 * MySQL 区服的显式入口。serve/check 均只连接预先初始化的权威库，不创建身份、不执行 DDL，
 * 也不从旧目录或投影配置推导数据集。数据库初始化与旧数据丢弃属于独立运维命令。
 */
final class RealmCommand {
    private static final String USAGE = "Usage: java -jar <game.jar> realm {serve|check} <realm-id> <dataset-uuid>\n"
            + "       With no arguments: SLG_REALM_ID and SLG_REALM_DATASET_ID select realm serve.\n"
            + "       Configure SLG_GAME_JDBC_URL, SLG_GAME_DB_USER and SLG_GAME_DB_PASSWORD; initialize the empty authority database explicitly first.";
    private RealmCommand() { }

    static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 1 && args[0].equals("--help")
                || args.length == 2 && args[0].equals("realm") && args[1].equals("--help")) {
            out.println(USAGE); return 0;
        }
        if (args.length != 4 || !args[0].equals("realm") || !(args[1].equals("serve") || args[1].equals("check"))) {
            err.println(USAGE); return 2;
        }
        try {
            int number = Integer.parseInt(args[2]);
            UUID dataset = UUID.fromString(args[3]);
            if (number <= 0 || !Integer.toString(number).equals(args[2]) || !dataset.toString().equalsIgnoreCase(args[3]))
                throw new IllegalArgumentException("Canonical positive realm ID and dataset UUID required");
            var realm = new RealmIdentity(number, dataset);
            var environment = System.getenv();
            requireSameBinding(environment, realm);
            if (args[1].equals("serve")) RealmGameProcess.serve(realm, environment, out);
            else {
                var settings = AuthorityDatabaseSettings.fromEnvironment(environment);
                try (var pools = AuthorityDataSources.open(settings)) {
                    new MySqlStartupCheck(pools.control(), settings.statementTimeoutSeconds()).verify(realm);
                    out.printf("Authority verified: realm=%d dataset=%s (read only; writer not acquired)%n", number, dataset);
                }
            }
            return 0;
        } catch (IllegalArgumentException invalid) {
            // 配置检查只记录稳定键名/原因；不要把 JDBC URL、驱动异常栈或口令写到 CLI。
            err.println("Invalid realm arguments or authority configuration; check the explicit binding and documented budgets");
            return 2;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); err.println("Realm lifecycle interrupted; drain is not confirmed"); return 1;
        } catch (StorageAccessException failure) {
            err.println("Authority access failed: " + failure.code()); return 1;
        } catch (Exception failure) {
            err.println("Realm lifecycle failed: " + failure.getClass().getSimpleName() + "; shutdown may be incomplete"); return 1;
        }
    }

    private static void requireSameBinding(Map<String, String> env, RealmIdentity realm) {
        String number = env.get("SLG_REALM_ID"), dataset = env.get("SLG_REALM_DATASET_ID");
        if (number != null && !number.isBlank() && !Integer.toString(realm.realmId()).equals(number)
                || dataset != null && !dataset.isBlank() && !realm.datasetId().toString().equalsIgnoreCase(dataset))
            throw new IllegalArgumentException("CLI identity differs from environment binding");
    }
}

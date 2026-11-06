package com.slg.game;

import com.slg.game.storage.RealmVolume;
import com.slg.game.storage.RealmVolumePreview;
import com.slg.core.protocol.Json;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

/** 区服进程入口。只有显式 init 可以创建库；serve 和 check 均要求原卷。 */
final class RealmVolumeCommand {
    private static final String USAGE =
            "Usage: java -jar <game.jar> {volume init|volume check|realm serve} <existing-directory> <realm-id> <volume-uuid>\n"
            + "       java -jar <game.jar> volume preview <existing-directory> <realm-id> <volume-uuid> [independent-checkpoint-directory]";

    private RealmVolumeCommand() {}

    static int run(String[] args, PrintStream out, PrintStream err) {
        if ((args.length == 1 && args[0].equals("--help"))
                || (args.length == 2 && (args[0].equals("volume") || args[0].equals("realm")) && args[1].equals("--help"))) {
            out.println(USAGE);
            return 0;
        }
        String operation = args.length >= 2 ? args[0] + " " + args[1] : "";
        boolean preview = operation.equals("volume preview");
        if ((args.length != 5 && !(preview && args.length == 6))
                || !Set.of("volume init", "volume check", "realm serve", "volume preview").contains(operation)) {
            err.println(USAGE);
            return 2;
        }
        try {
            Path directory = Path.of(args[2]);
            int realmId = Integer.parseInt(args[3]);
            UUID volumeId = UUID.fromString(args[4]);
            if (realmId <= 0 || !volumeId.toString().equalsIgnoreCase(args[4])) {
                throw new IllegalArgumentException("Expected a positive realm ID and a canonical volume UUID");
            }
            if (preview) {
                Path checkpoint = args.length == 6 ? Path.of(args[5]) : null;
                out.println(Json.write(RealmVolumePreview.observe(directory, realmId, volumeId, checkpoint, System.getenv())));
            } else if (operation.equals("realm serve")) {
                RealmGameProcess.serve(directory, realmId, volumeId, out);
            } else if (args[1].equals("init")) {
                RealmVolume.initialize(directory, realmId, volumeId);
                out.printf("Volume initialized: realm=%d volume=%s%n", realmId, volumeId);
            } else {
                // 真正获取独占句柄并核对库内身份；不能只检查目录或文件名。
                try (RealmVolume volume = RealmVolume.open(directory, realmId, volumeId)) {
                    out.printf("Volume verified: realm=%d volume=%s%n", realmId, volumeId);
                }
            }
            return 0;
        } catch (IllegalArgumentException e) {
            err.println("Invalid arguments or configuration for " + operation + ": " + e.getMessage());
            return 2;
        } catch (IOException e) {
            err.println(operation + " failed: " + e.getMessage());
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println("Realm server interrupted");
            return 1;
        }
    }
}

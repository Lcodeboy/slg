#!/usr/bin/env python3
"""只读检查已登记区服。沿用启动器的配置优先级，不建卷、不启动 GAME、不生成新凭据。"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
from local_services import ROOT, read_env, realm_identity, service_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / "data/mysql.env")
    parser.add_argument("--jar", type=Path, default=ROOT / "slg_game/target/slg_game-1.0-SNAPSHOT.jar")
    parser.add_argument("--checkpoint", type=Path, help="独立 checkpoint 所在目录；仅在临时副本上进行恢复演练")
    args = parser.parse_args()
    if not args.env_file.is_file() or not args.jar.is_file():
        parser.error("需要现有环境文件和包含预演命令的 GAME 包")
    env = {**read_env(ROOT / "data/local.env"), **read_env(args.env_file), **os.environ}
    cmd = ["java", "-jar", str(args.jar.resolve()), "volume", "preview", *realm_identity(env)]
    if args.checkpoint is not None:
        cmd.append(str(args.checkpoint.absolute()))
    # MySQL 密码沿 GAME 专属环境传递；UC、数据库管理员凭据都不进入参数或预演进程。
    return subprocess.run(cmd, env=service_environment("game", env)).returncode


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print("Preview failed: " + str(error), file=sys.stderr)
        sys.exit(1)

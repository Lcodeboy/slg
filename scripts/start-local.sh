#!/usr/bin/env bash
# 启动UC、Battle、GAME 和 Gateway；定位脚本目录而非调用者当前目录，保证从任意路径执行行为一致。
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# 参数原样传给 Python 启动器，由其完成密钥、端口、身份记录和失败清理。
exec python3 "$SCRIPT_DIR/local_services.py" start "$@"

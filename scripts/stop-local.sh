#!/usr/bin/env bash
# 安全停止本工程拥有的服务，具体 PID/启动时间/命令行校验由 Python 启动器完成。
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# 停止不会删除数据库、日志或密钥，后续启动继续复用持久化数据。
exec python3 "$SCRIPT_DIR/local_services.py" stop "$@"

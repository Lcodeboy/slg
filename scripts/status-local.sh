#!/usr/bin/env bash
# 查询启动器记录的服务身份与健康状态，不启动进程、不生成密钥。
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# 使用 exec 保留真实退出状态，供终端和自动化脚本判断命令结果。
exec python3 "$SCRIPT_DIR/local_services.py" status "$@"

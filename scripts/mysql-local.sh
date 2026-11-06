#!/usr/bin/env bash
# 开发 MySQL 的薄入口，按脚本位置定位管理程序，所有用户参数保持原样。
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# 配置生成、容器健康等待和保留数据的停止行为集中在 Python 实现中。
exec python3 "$SCRIPT_DIR/mysql_local.py" "$@"

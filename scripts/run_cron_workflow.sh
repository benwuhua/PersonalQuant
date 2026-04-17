#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT/logs/cron"
STAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/run_${STAMP}.log"
CONFIG_PATH="${PERSONALQUANT_CONFIG:-}"

mkdir -p "$LOG_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "[cron] start_at=$(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[cron] root=$ROOT"
echo "[cron] log_file=$LOG_FILE"

if [[ -f "$HOME/.venvs/qlib-activate.sh" ]]; then
  # shellcheck disable=SC1090
  source "$HOME/.venvs/qlib-activate.sh"
  echo "[cron] activated_qlib_env=$HOME/.venvs/qlib-activate.sh"
else
  echo "[cron] missing_qlib_env=$HOME/.venvs/qlib-activate.sh"
  exit 1
fi

cd "$ROOT"

if [[ -n "$CONFIG_PATH" ]]; then
  echo "[cron] using_config=$CONFIG_PATH"
fi

python scripts/dev.py run

echo "[cron] finished_at=$(date '+%Y-%m-%d %H:%M:%S %Z')"

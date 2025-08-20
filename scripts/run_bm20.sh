#!/usr/bin/env bash
set -euo pipefail

attempts=0
max_attempts=${MAX_ATTEMPTS:-3}
sleep_sec=${RETRY_SLEEP_SECONDS:-45}

while (( attempts < max_attempts )); do
  echo "[INFO] Attempt $((attempts+1))/${max_attempts}"
  if python bm20_daily.py; then
    echo "[INFO] bm20_daily.py succeeded"
    exit 0
  fi
  attempts=$((attempts+1))
  if (( attempts < max_attempts )); then
    echo "[WARN] attempt ${attempts} failed; sleeping ${sleep_sec}s..."
    sleep "${sleep_sec}"
  fi
done

echo "::error ::bm20_daily.py failed after ${max_attempts} attempts"
exit 1

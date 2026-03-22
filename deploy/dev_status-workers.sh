#!/usr/bin/env bash
set -euo pipefail

UPLOAD_UNIT="asr-worker-batch-dev@1.service"
LIVE_UNIT="asr-worker-live-dev@1.service"

print_status() {
  local maxlen=0 unit state
  for unit in "$@"; do
    if (( ${#unit} > maxlen )); then
      maxlen=${#unit}
    fi
  done
  for unit in "$@"; do
    state="$(systemctl --user is-active "$unit" || true)"
    printf "  - %-*s  %s\n" "$maxlen" "$unit" "$state"
  done
}

echo "[dev-status-workers] Service status:"
print_status "$UPLOAD_UNIT" "$LIVE_UNIT"

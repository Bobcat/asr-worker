#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
UNIT_SRC_DIR="${REPO_ROOT}/deploy/systemd"
UNIT_DST_DIR="/etc/systemd/system"
ENV_DST_DIR="/etc/asr-worker"

run_root() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

run_root install -m 0644 "${UNIT_SRC_DIR}/asr-worker-live.service" "${UNIT_DST_DIR}/asr-worker-live.service"
run_root install -m 0644 "${UNIT_SRC_DIR}/asr-worker-batch.service" "${UNIT_DST_DIR}/asr-worker-batch.service"
run_root mkdir -p "${ENV_DST_DIR}"

if ! run_root test -f "${ENV_DST_DIR}/asr-worker.env"; then
  run_root install -m 0644 "${REPO_ROOT}/deploy/env/asr-worker.env.example" "${ENV_DST_DIR}/asr-worker.env"
fi

run_root systemctl daemon-reload

echo "Installed system units in ${UNIT_DST_DIR}:"
echo "  - asr-worker-live.service"
echo "  - asr-worker-batch.service"
echo
echo "Use:"
echo "  sudo systemctl enable --now asr-worker-live.service asr-worker-batch.service"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
UNIT_SRC_DIR="${REPO_ROOT}/deploy/systemd"
UNIT_DST_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
ENV_DST_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/asr-worker"

mkdir -p "${UNIT_DST_DIR}" "${ENV_DST_DIR}"

install -m 0644 "${UNIT_SRC_DIR}/asr-worker-live-dev@.service" "${UNIT_DST_DIR}/asr-worker-live-dev@.service"
install -m 0644 "${UNIT_SRC_DIR}/asr-worker-batch-dev@.service" "${UNIT_DST_DIR}/asr-worker-batch-dev@.service"

if [[ ! -f "${ENV_DST_DIR}/asr-worker.dev.env" ]]; then
  install -m 0644 "${REPO_ROOT}/deploy/env/asr-worker.dev.env.example" "${ENV_DST_DIR}/asr-worker.dev.env"
fi

systemctl --user daemon-reload

echo "Installed user units in ${UNIT_DST_DIR}:"
echo "  - asr-worker-live-dev@.service"
echo "  - asr-worker-batch-dev@.service"
echo
echo "Use:"
echo "  systemctl --user enable --now asr-worker-live-dev@1.service asr-worker-batch-dev@1.service"

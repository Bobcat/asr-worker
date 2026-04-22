from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.config import get_str
from app.worker.contract import _worker_contract_sections


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve_worker_queue_base() -> Path:
  env_base = str(os.getenv("ASR_WORKER_QUEUE_BASE") or "").strip()
  cfg_base = get_str("worker.queue_base", "data/jobs/upload_worker").strip()
  raw = env_base or cfg_base
  p = Path(raw) if raw else Path("data/jobs/upload_worker")
  return p if p.is_absolute() else (_REPO_ROOT / p).resolve()


def _default_runs_v1_path() -> Path:
  queue_base = _resolve_worker_queue_base()
  data_root = queue_base.parent.parent
  return (data_root / "progress_db" / "runs_v1.jsonl").resolve()


_runs_path = get_str("worker.progress_runs_path", "").strip()
if _runs_path:
  RUNS_V1_PATH = Path(_runs_path)
  if not RUNS_V1_PATH.is_absolute():
    RUNS_V1_PATH = (_REPO_ROOT / RUNS_V1_PATH).resolve()
else:
  RUNS_V1_PATH = _default_runs_v1_path()


def _noop(*_args: Any, **_kwargs: Any) -> None:
  return None


@dataclass
class PendingWorkerJob:
  job: object
  job_cfg: dict[str, Any]
  job_t0_mono: float
  request_id: str = ""
  srt_output_path: Path | None = None
  request_cfg: dict[str, Any] = field(default_factory=dict)
  features: dict[str, bool] = field(default_factory=dict)
  timing_rows: list[tuple[str, float]] = field(default_factory=list)
  eta_confidence: float = 0.0
  eta_hints: list[str] = field(default_factory=list)
  wx_t0_mono: float = 0.0
  asr_stage: str = ""
  progress_start_phase: Any = _noop
  progress_finish_phase: Any = _noop
  progress_heartbeat: Any = _noop


def _worker_status_owner() -> str:
  raw = str(os.getenv("ASR_WORKER_STATUS_OWNER") or "").strip() or get_str("worker.status_owner", "").strip()
  return raw or "asr-worker"


def _pool_status_owner() -> str:
  raw = str(os.getenv("ASR_POOL_STATUS_OWNER") or "").strip() or get_str("worker.asr_pool_status_owner", "").strip()
  return raw or "asr-pool"


def _feature_flags(job_cfg: dict[str, Any]) -> dict[str, bool]:
  _input_cfg, _request_cfg, _outputs_cfg, features_cfg = _worker_contract_sections(job_cfg)
  write_status_json = bool(features_cfg.get("write_status_json", False))
  include_runtime_meta = bool(features_cfg.get("include_runtime_meta", False))
  predictive_progress = bool(features_cfg.get("predictive_progress", False))
  if not write_status_json:
    raise RuntimeError("worker_features.write_status_json must be true for file-backed worker v1")
  if include_runtime_meta and not write_status_json:
    raise RuntimeError("include_runtime_meta requires write_status_json=true")
  if predictive_progress and not write_status_json:
    raise RuntimeError("predictive_progress requires write_status_json=true")
  return {
    "download_srt": bool(features_cfg.get("download_srt", False)),
    "track_pending_status": bool(features_cfg.get("track_pending_status", False)),
    "predictive_progress": predictive_progress,
    "write_timings_text": bool(features_cfg.get("write_timings_text", False)),
    "include_runtime_meta": include_runtime_meta,
  }


def _wait_message(
  *,
  request_id: str,
  state: str,
  stage: str,
  queue_position: Any = None,
) -> str:
  rid = str(request_id or "").strip()
  state_key = str(state or "").strip().lower()
  stage_key = str(stage or "").strip().lower()
  try:
    queue_pos = int(queue_position) if queue_position is not None else None
  except Exception:
    queue_pos = None
  base = f"Waiting for ASR completion ({rid})..." if rid else "Waiting for ASR completion..."
  if state_key == "queued":
    if queue_pos is not None and queue_pos > 0:
      return f"Queued for ASR (position {queue_pos})..."
    return "Queued for ASR..."
  if state_key in {"running", "cancel_requested"}:
    stage_messages = {
      "prepare": "Preparing WhisperX...",
      "transcribe": "Transcribing...",
      "align": "Aligning...",
      "diarize": "Diarizing...",
      "done": "Finalizing...",
    }
    if stage_key in stage_messages:
      return stage_messages[stage_key]
    if state_key == "cancel_requested":
      return "ASR cancel requested..."
    return "ASR running..."
  return base


def _parse_timing_rows(timings_text: Any) -> list[tuple[str, float]]:
  text = str(timings_text or "").strip()
  if not text:
    return []
  rows: list[tuple[str, float]] = []
  seen: set[str] = set()
  for raw_part in text.split("|"):
    part = str(raw_part or "").strip()
    if not part or "=" not in part:
      continue
    name_raw, sec_raw = part.split("=", 1)
    name = str(name_raw or "").strip()
    if not name or name == "total" or name in seen:
      continue
    sec_str = str(sec_raw or "").strip()
    if sec_str.endswith("s"):
      sec_str = sec_str[:-1]
    try:
      sec = max(0.0, float(sec_str))
    except Exception:
      continue
    rows.append((name, sec))
    seen.add(name)
  return rows


def _hardware_key() -> str:
  raw = get_str("worker.hardware_key", "").strip()
  if raw:
    return raw
  host_id = get_str("worker.host_id", "").strip() or (socket.gethostname().split(".")[0] or "unknown-host").strip() or "unknown-host"
  if host_id == "dc1":
    return "dc1-rtx5070ti-cuda"
  if host_id == "dc2":
    return "dc2-rtx5090-cuda"
  return f"{host_id}-unknown"

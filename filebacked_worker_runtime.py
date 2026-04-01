from __future__ import annotations

import json
import os
import queue
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from queue_fs import QueueRoot, claim_next_job, finish_job
from worker_status_io import _append_log, _utc_iso, _write_status
from progress_predictor import build_prediction, phase_order_for_job
from asr_client_remote import (
  fetch_remote_pending_status,
  submit_remote_pool_request,
)
from event_loop import WorkerEventBus, WorkerEventType
from filebacked_finalization import (
  finalize_filebacked_job_error,
  finalize_filebacked_job_terminal,
)
from progress_tracker import _build_progress_tracker, _format_timings_text
from runtime_common import (
  _asr_stage_to_phase,
  _build_remote_pool_request_from_contract,
  _elapsed_utc_s,
  _read_worker_job_contract,
  _resolve_job_relpath,
  _worker_contract_sections,
  is_asr_terminal_state,
  normalize_speaker_mode,
)
from worker_config import get_str


_REPO_ROOT = Path(__file__).resolve().parent


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
  asr_stage_started_at_utc: str = ""
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


def _record_phase_timing(*, pending: PendingWorkerJob, name: str, elapsed_s: float) -> None:
  safe_elapsed = max(0.0, float(elapsed_s))
  pending.timing_rows.append((name, safe_elapsed))
  if pending.features.get("write_timings_text", False):
    _write_status(pending.job.status_path, timings_text=_format_timings_text(pending.timing_rows))


def _apply_pending_status(*, pending: PendingWorkerJob, row: dict[str, Any]) -> None:
  state = str(row.get("state") or "").strip().lower()
  stage = str(row.get("stage") or "").strip().lower()
  stage_started_at_utc = str(row.get("stage_started_at_utc") or row.get("started_at_utc") or "").strip()
  if state in {"running", "cancel_requested"} and stage:
    prev_stage = str(pending.asr_stage or "").strip().lower()
    prev_started = str(pending.asr_stage_started_at_utc or "").strip()
    if prev_stage and stage != prev_stage and prev_started and stage_started_at_utc:
      phase_name = _asr_stage_to_phase(prev_stage)
      recorded_phase_names = {name for name, _elapsed in pending.timing_rows}
      if phase_name and phase_name not in recorded_phase_names:
        elapsed = _elapsed_utc_s(prev_started, stage_started_at_utc)
        if elapsed is not None:
          _record_phase_timing(pending=pending, name=phase_name, elapsed_s=float(elapsed))
          pending.progress_finish_phase(phase_name, float(elapsed))
    phase_name = _asr_stage_to_phase(stage)
    if phase_name and stage != prev_stage:
      pending.progress_start_phase(
        phase_name,
        _wait_message(
          request_id=pending.request_id,
          state=state,
          stage=stage,
          queue_position=row.get("queue_position"),
        ),
        "whisperx_wait",
      )
    pending.asr_stage = stage
    if stage_started_at_utc:
      pending.asr_stage_started_at_utc = stage_started_at_utc

  msg = _wait_message(
    request_id=pending.request_id,
    state=state,
    stage=stage,
    queue_position=row.get("queue_position"),
  )
  _write_status(
    pending.job.status_path,
    phase="whisperx_wait",
    status_owner=_pool_status_owner(),
    message=msg,
  )


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


def _prepare_worker_job_for_submit(
  *,
  pending: PendingWorkerJob,
  consumer_id: str,
) -> dict[str, Any]:
  job = pending.job
  job_cfg = dict(pending.job_cfg or {})
  input_cfg, request_cfg, outputs_cfg, _features_cfg = _worker_contract_sections(job_cfg)
  pending.request_cfg = dict(request_cfg)
  pending.features = _feature_flags(job_cfg)
  status_before = json.loads(job.status_path.read_text(encoding="utf-8"))

  if pending.features.get("download_srt", False):
    pending.srt_output_path = _resolve_job_relpath(
      job=job,
      relpath=outputs_cfg.get("srt_relpath"),
      field_name="outputs.srt_relpath",
    )
  else:
    pending.srt_output_path = None

  asr_input_path = _resolve_job_relpath(
    job=job,
    relpath=input_cfg.get("audio_relpath"),
    field_name="input.audio_relpath",
  )
  if not asr_input_path.exists():
    raise RuntimeError(f"ASR input missing before submit: {asr_input_path}")

  speaker_mode = normalize_speaker_mode(request_cfg.get("speaker_mode", "auto"))
  if pending.features.get("predictive_progress", False):
    try:
      duration_ms = max(1, int(input_cfg.get("duration_ms") or 0))
    except Exception as e:
      raise RuntimeError("Missing or invalid input.duration_ms for predictive_progress=true") from e
    audio_duration_s = max(1, int((duration_ms + 999) // 1000))
    prediction = build_prediction(
      runs_path=RUNS_V1_PATH,
      hardware_key=_hardware_key(),
      speaker_mode=speaker_mode,
      audio_duration_s=audio_duration_s,
    )
    pending.eta_confidence = float(prediction.confidence)
    pending.eta_hints = list(prediction.hints)
    pending.progress_start_phase, pending.progress_finish_phase, pending.progress_heartbeat, _ignored_set_message = _build_progress_tracker(
      status_path=job.status_path,
      phase_order=phase_order_for_job(speaker_mode=speaker_mode),
      phase_expected_s=prediction.phase_expected_s,
      eta_confidence=prediction.confidence,
      eta_hints=pending.eta_hints,
    )
    _ = _ignored_set_message
  else:
    pending.eta_confidence = 0.0
    pending.eta_hints = []

  try:
    elapsed_s = max(0.0, float(status_before.get("elapsed_s") or 0.0))
  except Exception:
    elapsed_s = 0.0
  if elapsed_s > 0.0:
    pending.job_t0_mono = max(0.0, time.monotonic() - elapsed_s)

  _write_status(
    job.status_path,
    state="running",
    status_owner=_worker_status_owner(),
    started_at=str(status_before.get("started_at") or "").strip() or _utc_iso(),
  )
  pending.wx_t0_mono = time.monotonic()

  try:
    _append_log(
      job.log_path,
      f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] WORKER submit job_cfg={json.dumps(job_cfg, ensure_ascii=False)}",
    )
  except Exception:
    pass

  asr_request, _ = _build_remote_pool_request_from_contract(job=job, job_cfg=job_cfg)
  submit = submit_remote_pool_request(
    request_payload=asr_request,
    consumer_id=consumer_id,
  )
  if not bool(submit.get("ok", False)):
    err_resp = dict(submit.get("error_response") or {})
    err = dict(err_resp.get("error") or {})
    raise RuntimeError(
      f"{err.get('code') or err_resp.get('code') or 'ASR_SUBMIT_FAILED'}: "
      f"{err.get('message') or err_resp.get('message') or 'ASR submit failed'}"
    )

  pending.request_id = str(submit.get("request_id") or "").strip()
  if not pending.request_id:
    raise RuntimeError("ASR submit response missing request_id")

  submit_lifecycle = dict(submit.get("submit_lifecycle") or {})
  lifecycle_state = str(submit_lifecycle.get("state") or "").strip().lower()
  lifecycle_stage = str(submit_lifecycle.get("stage") or "").strip().lower()
  wait_msg = _wait_message(
    request_id=pending.request_id,
    state=(lifecycle_state or "running"),
    stage=lifecycle_stage,
    queue_position=submit_lifecycle.get("queue_position"),
  )
  _write_status(
    job.status_path,
    phase="whisperx_wait",
    status_owner=_pool_status_owner(),
    progress=0.1,
    message=wait_msg,
    asr_request_id=pending.request_id,
  )
  return submit


def filebacked_submit_worker_loop(
  *,
  submit_queue: "queue.Queue[PendingWorkerJob | None]",
  event_bus: WorkerEventBus,
  consumer_id: str,
) -> None:
  while True:
    pending = submit_queue.get()
    if pending is None:
      return
    payload: dict[str, Any] = {"pending": pending}
    try:
      payload["submit"] = _prepare_worker_job_for_submit(
        pending=pending,
        consumer_id=consumer_id,
      )
    except Exception as e:
      payload["error"] = str(e)
    event_bus.put(WorkerEventType.SUBMIT_RESULT, payload)


def handle_filebacked_submit_result(*, payload: dict[str, Any], pending: dict[str, PendingWorkerJob]) -> bool:
  pending_job = payload.get("pending")
  if pending_job is None:
    return False
  err_msg = str(payload.get("error") or "").strip()
  if err_msg:
    finalize_filebacked_job_error(pending=pending_job, exc=RuntimeError(err_msg))
    print(f"Error {pending_job.job.job_id}: {err_msg}")
    return True

  submit = dict(payload.get("submit") or {})
  request_id = str(submit.get("request_id") or pending_job.request_id or "").strip()
  if not request_id:
    finalize_filebacked_job_error(pending=pending_job, exc=RuntimeError("Invalid submit result payload: missing request_id"))
    print(f"Error {pending_job.job.job_id}: invalid_submit_result missing_request_id")
    return True
  pending_job.request_id = request_id

  submit_lifecycle = dict(submit.get("submit_lifecycle") or {})
  lifecycle_state = str(submit_lifecycle.get("state") or "").strip().lower()
  if lifecycle_state in {"completed", "failed", "cancelled"}:
    submit_lifecycle.setdefault("request_id", request_id)
    try:
      finalize_filebacked_job_terminal(pending=pending_job, event=submit_lifecycle)
      print(f"Done {pending_job.job.job_id} state={lifecycle_state}")
    except Exception as e:
      finalize_filebacked_job_error(pending=pending_job, exc=e)
      print(f"Error {pending_job.job.job_id}: {e!r}")
    return True

  pending[request_id] = pending_job
  return True


def poll_filebacked_pending(
  *,
  consumer_id: str,
  pending: dict[str, PendingWorkerJob],
  poll_state: dict[str, Any],
) -> None:
  tracked_pending = {rid: job for rid, job in pending.items() if job.features.get("track_pending_status", False)}
  now_mono = time.monotonic()
  interval_s = max(0.2, float(poll_state.get("interval_s") or 1.0))
  last_pending_status_poll_mono = float(poll_state.get("last_pending_status_poll_mono") or 0.0)
  if tracked_pending and (now_mono - last_pending_status_poll_mono) >= interval_s:
    poll_state["last_pending_status_poll_mono"] = now_mono
    rows = fetch_remote_pending_status(
      consumer_id=consumer_id,
      request_ids=list(tracked_pending.keys()),
      limit=200,
    )
    for row in rows:
      rid = str(row.get("request_id") or "").strip()
      if not rid:
        continue
      pending_job = tracked_pending.get(rid)
      if pending_job is None:
        continue
      # SSE is the only terminal owner; pending polling only enriches interim status.
      if is_asr_terminal_state(str(row.get("state") or "").strip().lower()):
        continue
      try:
        _apply_pending_status(pending=pending_job, row=row)
      except Exception:
        pass
  for pending_job in pending.values():
    if not pending_job.features.get("predictive_progress", False):
      continue
    # Keep queued/wait messages intact until we have an actual ASR stage signal.
    if not str(pending_job.asr_stage or "").strip():
      continue
    try:
      pending_job.progress_heartbeat()
    except Exception:
      pass


def refill_filebacked_from_inbox(
  *,
  queue_root: QueueRoot,
  pending: dict[str, PendingWorkerJob],
  submitting: dict[str, PendingWorkerJob],
  max_outstanding: int,
  submit_queue: "queue.Queue[Any]",
  counters: Any,
) -> tuple[bool, bool]:
  did_work = False
  while (len(pending) + len(submitting)) < max_outstanding:
    job = claim_next_job(queue_root=queue_root)
    if not job:
      return did_work, False
    did_work = True
    try:
      job_cfg = _read_worker_job_contract(job.job_path)
      pending_job = PendingWorkerJob(
        job=job,
        job_cfg=job_cfg,
        job_t0_mono=time.monotonic(),
      )
      pending_job.features = _feature_flags(job_cfg)
      submitting[str(job.job_id)] = pending_job
      counters.submits_started += 1
      submit_queue.put(pending_job)
    except Exception as e:
      _write_status(
        job.status_path,
        state="error",
        phase="error",
        progress=1.0,
        finished_at=_utc_iso(),
        message=f"Worker error: {e!r}",
        error=str(e),
      )
      finish_job(job, ok=False)
      print(f"Error {job.job_id}: {e!r}")
  return did_work, True

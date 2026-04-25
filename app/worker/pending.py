from __future__ import annotations

import time
from typing import Any

from app.remote.asr_bridge import fetch_remote_pending_status
from app.worker.progress.tracker import _format_timings_text
from app.worker.runtime import (
  PendingWorkerJob,
  _pool_status_owner,
  _wait_message,
)
from app.worker.status.io import _write_status


_ROW_TIMING_PHASES: tuple[tuple[str, str], ...] = (
  ("prepare_s", "whisperx_prepare"),
  ("transcribe_s", "whisperx_transcribe"),
  ("align_s", "whisperx_align"),
  ("diarize_s", "whisperx_diarize"),
  ("finalize_s", "whisperx_finalize"),
)


def _asr_stage_to_phase(stage: str) -> str:
  key = str(stage or "").strip().lower()
  mapping = {
    "prepare": "whisperx_prepare",
    "transcribe": "whisperx_transcribe",
    "align": "whisperx_align",
    "diarize": "whisperx_diarize",
    "done": "whisperx_finalize",
  }
  return str(mapping.get(key) or "")


def _is_asr_terminal_state(state: str) -> bool:
  return str(state or "").strip().lower() in {"completed", "failed", "cancelled"}


def _record_phase_timing(*, pending: PendingWorkerJob, name: str, elapsed_s: float) -> None:
  safe_elapsed = max(0.0, float(elapsed_s))
  pending.timing_rows.append((name, safe_elapsed))
  if pending.features.get("write_timings_text", False):
    _write_status(pending.job.status_path, timings_text=_format_timings_text(pending.timing_rows))


def _apply_runtime_phase_timings(*, pending: PendingWorkerJob, row: dict[str, Any]) -> None:
  raw_timings = dict(row.get("timings") or {})
  if not raw_timings:
    return
  recorded_phase_names = {name for name, _elapsed in pending.timing_rows}
  for timing_key, phase_name in _ROW_TIMING_PHASES:
    if phase_name in recorded_phase_names:
      continue
    if timing_key not in raw_timings:
      continue
    try:
      elapsed_s = max(0.0, float(raw_timings[timing_key]))
    except Exception:
      continue
    _record_phase_timing(pending=pending, name=phase_name, elapsed_s=elapsed_s)
    pending.progress_finish_phase(phase_name, elapsed_s)
    recorded_phase_names.add(phase_name)


def _apply_pending_status(*, pending: PendingWorkerJob, row: dict[str, Any]) -> None:
  state = str(row.get("state") or "").strip().lower()
  stage = str(row.get("stage") or "").strip().lower()
  if state in {"running", "cancel_requested"}:
    _apply_runtime_phase_timings(pending=pending, row=row)
    prev_stage = str(pending.asr_stage or "").strip().lower()
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
    if stage:
      pending.asr_stage = stage

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
    asr_phase=_asr_stage_to_phase(stage) or "whisperx_wait",
  )


def poll_pending_jobs(
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
      if _is_asr_terminal_state(str(row.get("state") or "").strip().lower()):
        continue
      try:
        _apply_pending_status(pending=pending_job, row=row)
      except Exception:
        pass
  for pending_job in pending.values():
    if not pending_job.features.get("predictive_progress", False):
      continue
    if not str(pending_job.asr_stage or "").strip():
      continue
    try:
      pending_job.progress_heartbeat()
    except Exception:
      pass

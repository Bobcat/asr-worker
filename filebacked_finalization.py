from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from queue_fs import finish_job
from asr_client_remote import download_remote_request_srt_to_path
from progress_tracker import _format_timings_text
from runtime_meta import _extended_runtime_meta_patch
from worker_status_io import _utc_iso, _write_status
from worker_config import get_str


def _worker_status_owner() -> str:
  raw = str(os.getenv("ASR_WORKER_STATUS_OWNER") or "").strip() or get_str("worker.status_owner", "").strip()
  return raw or "asr-worker"


def _pool_status_owner() -> str:
  raw = str(os.getenv("ASR_POOL_STATUS_OWNER") or "").strip() or get_str("worker.asr_pool_status_owner", "").strip()
  return raw or "asr-pool"


def _record_phase_timing(*, pending: Any, name: str, elapsed_s: float) -> None:
  safe_elapsed = max(0.0, float(elapsed_s))
  pending.timing_rows.append((name, safe_elapsed))
  if pending.features.get("write_timings_text", False):
    _write_status(pending.job.status_path, timings_text=_format_timings_text(pending.timing_rows))


def finalize_filebacked_job_terminal(
  *,
  pending: Any,
  event: dict[str, Any],
) -> None:
  terminal_state = str((event or {}).get("state") or "").strip().lower()
  job = pending.job
  if terminal_state in {"failed", "cancelled"}:
    err_obj = dict((event or {}).get("error") or {})
    err_code = str(err_obj.get("code") or "ASR_REMOTE_TERMINAL_ERROR")
    err_msg = str(err_obj.get("message") or f"ASR terminal state: {terminal_state}")
    patch: dict[str, Any] = {
      "state": ("cancelled" if terminal_state == "cancelled" else "error"),
      "phase": "error",
      "status_owner": _pool_status_owner(),
      "progress": 1.0,
      "finished_at": _utc_iso(),
      "message": f"Worker error: {err_code}: {err_msg}",
      "error": f"{err_code}: {err_msg}",
      "asr_request_id": str(pending.request_id or ""),
    }
    if pending.features.get("include_runtime_meta", False):
      patch["asr_state"] = terminal_state
    _write_status(job.status_path, **patch)
    finish_job(job, ok=False)
    return
  if terminal_state != "completed":
    raise RuntimeError(f"Unsupported terminal state: {terminal_state or 'unknown'}")

  asr_response = dict((event or {}).get("response") or {})
  if not asr_response:
    raise RuntimeError("ASR terminal completion missing response payload")

  srt_path: Path | None = None
  if pending.features.get("download_srt", False):
    if pending.srt_output_path is None:
      raise RuntimeError("Missing outputs.srt_relpath while download_srt=true")
    try:
      srt_path = download_remote_request_srt_to_path(
        request_id=str(pending.request_id or ""),
        dst_path=pending.srt_output_path,
        allow_empty=True,
      )
    except Exception as e:
      raise RuntimeError(f"Failed to fetch SRT artifact from ASR pool: {e!r}") from e

  wx_timings = dict(asr_response.get("timings") or {})
  wx_elapsed = max(0.0, float(time.monotonic() - pending.wx_t0_mono))
  recorded_phase_names = {name for name, _elapsed in pending.timing_rows}
  emitted = False
  for timing_key, phase_name, phase_msg in (
    ("prepare_s", "whisperx_prepare", "Preparing WhisperX..."),
    ("transcribe_s", "whisperx_transcribe", "Transcribing..."),
    ("align_s", "whisperx_align", "Aligning..."),
    ("diarize_s", "whisperx_diarize", "Diarizing..."),
    ("finalize_s", "whisperx_finalize", "Finalizing..."),
  ):
    if phase_name in recorded_phase_names or timing_key not in wx_timings:
      continue
    try:
      elapsed_s = float(wx_timings[timing_key])
    except Exception:
      continue
    pending.progress_start_phase(phase_name, phase_msg, phase_name)
    recorded_phase_names.add(phase_name)
    _record_phase_timing(pending=pending, name=phase_name, elapsed_s=elapsed_s)
    pending.progress_finish_phase(phase_name, elapsed_s)
    emitted = True
  if not emitted and (pending.features.get("write_timings_text", False) or pending.features.get("predictive_progress", False)):
    _record_phase_timing(pending=pending, name="whisperx", elapsed_s=wx_elapsed)
    pending.progress_finish_phase("whisperx", wx_elapsed)

  actual_total_s = max(0.0, float(time.monotonic() - pending.job_t0_mono))
  patch: dict[str, Any] = {
    "state": "done",
    "phase": "done",
    "status_owner": _pool_status_owner(),
    "progress": 1.0,
    "finished_at": _utc_iso(),
    "message": "Done",
    "asr_request_id": str(pending.request_id or ""),
  }
  if srt_path is not None:
    patch["srt_filename"] = srt_path.name
  if pending.features.get("write_timings_text", False):
    patch["timings_text"] = _format_timings_text(pending.timing_rows, total_s=actual_total_s)
  if pending.features.get("predictive_progress", False):
    patch.update({
      "progress_mode": "predictive_v1",
      "eta_total_s": round(actual_total_s, 3),
      "eta_remaining_s": 0.0,
      "elapsed_s": round(actual_total_s, 3),
      "eta_confidence": round(float(pending.eta_confidence), 3),
      "eta_hints": list(pending.eta_hints),
    })
  if pending.features.get("include_runtime_meta", False):
    patch.update(_extended_runtime_meta_patch(
      terminal_state="completed",
      response=asr_response,
      request_cfg=pending.request_cfg,
    ))
  _write_status(job.status_path, **patch)
  finish_job(job, ok=True)


def finalize_filebacked_job_error(*, pending: Any, exc: Exception) -> None:
  patch: dict[str, Any] = {
    "state": "error",
    "phase": "error",
    "status_owner": _worker_status_owner(),
    "progress": 1.0,
    "finished_at": _utc_iso(),
    "message": f"Worker error: {exc!r}",
    "error": str(exc),
  }
  if pending.request_id:
    patch["asr_request_id"] = str(pending.request_id)
  actual_total_s = max(0.0, float(time.monotonic() - pending.job_t0_mono))
  if pending.features.get("write_timings_text", False) and pending.timing_rows:
    patch["timings_text"] = _format_timings_text(pending.timing_rows, total_s=actual_total_s)
  if pending.features.get("predictive_progress", False):
    patch.update({
      "progress_mode": "predictive_v1",
      "eta_total_s": round(actual_total_s, 3),
      "eta_remaining_s": 0.0,
      "elapsed_s": round(actual_total_s, 3),
      "eta_confidence": round(float(pending.eta_confidence), 3),
      "eta_hints": list(pending.eta_hints),
    })
  _write_status(pending.job.status_path, **patch)
  finish_job(pending.job, ok=False)

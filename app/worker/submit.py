from __future__ import annotations

import json
import queue
import time
from datetime import datetime, timezone
from typing import Any

from app.queue.fs import QueueRoot, claim_next_job, finish_job
from app.remote.asr_bridge import submit_remote_pool_request
from app.worker.contract import (
  _build_remote_pool_request_from_contract,
  _read_worker_job_contract,
  _resolve_job_relpath,
  _worker_contract_sections,
)
from app.worker.coordination.events import WorkerEventBus, WorkerEventType
from app.worker.finalization import finalize_job_error, finalize_job_terminal
from app.worker.progress.predictor import build_prediction, phase_order_for_job
from app.worker.progress.tracker import _build_progress_tracker
from app.worker.runtime import (
  RUNS_V1_PATH,
  PendingWorkerJob,
  _feature_flags,
  _hardware_key,
  _parse_timing_rows,
  _pool_status_owner,
  _wait_message,
  _worker_status_owner,
)
from app.worker.status.io import _append_log, _utc_iso, _write_status


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
  pending.timing_rows = _parse_timing_rows(status_before.get("timings_text"))
  try:
    elapsed_s = max(0.0, float(status_before.get("elapsed_s") or 0.0))
  except Exception:
    elapsed_s = 0.0
  if elapsed_s > 0.0:
    pending.job_t0_mono = max(0.0, time.monotonic() - elapsed_s)

  completed_actual_seed: dict[str, float] | None = None
  if pending.timing_rows:
    seed: dict[str, float] = {}
    for phase_name, phase_elapsed_s in pending.timing_rows:
      seed[phase_name] = seed.get(phase_name, 0.0) + max(0.0, float(phase_elapsed_s))
    if seed:
      completed_actual_seed = seed
  elif elapsed_s > 0.0:
    completed_actual_seed = {"snipping": float(elapsed_s)}

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

  speaker_mode = request_cfg.get("speaker_mode", "auto")
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
      completed_actual_seed=completed_actual_seed,
    )
    _ = _ignored_set_message
  else:
    pending.eta_confidence = 0.0
    pending.eta_hints = []

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


def submit_worker_loop(
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


def handle_submit_result(*, payload: dict[str, Any], pending: dict[str, PendingWorkerJob]) -> bool:
  pending_job = payload.get("pending")
  if pending_job is None:
    return False
  err_msg = str(payload.get("error") or "").strip()
  if err_msg:
    finalize_job_error(pending=pending_job, exc=RuntimeError(err_msg))
    print(f"Error {pending_job.job.job_id}: {err_msg}")
    return True

  submit = dict(payload.get("submit") or {})
  request_id = str(submit.get("request_id") or pending_job.request_id or "").strip()
  if not request_id:
    finalize_job_error(pending=pending_job, exc=RuntimeError("Invalid submit result payload: missing request_id"))
    print(f"Error {pending_job.job.job_id}: invalid_submit_result missing_request_id")
    return True
  pending_job.request_id = request_id

  submit_lifecycle = dict(submit.get("submit_lifecycle") or {})
  lifecycle_state = str(submit_lifecycle.get("state") or "").strip().lower()
  if lifecycle_state in {"completed", "failed", "cancelled"}:
    submit_lifecycle.setdefault("request_id", request_id)
    try:
      finalize_job_terminal(pending=pending_job, event=submit_lifecycle)
      print(f"Done {pending_job.job.job_id} state={lifecycle_state}")
    except Exception as e:
      finalize_job_error(pending=pending_job, exc=e)
      print(f"Error {pending_job.job.job_id}: {e!r}")
    return True

  pending[request_id] = pending_job
  return True


def refill_from_inbox(
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

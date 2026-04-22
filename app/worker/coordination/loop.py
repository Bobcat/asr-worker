from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from app.ops.server import OpsSnapshotStore, WorkerOpsServer
from app.queue.fs import QueueRoot
from app.remote.asr_bridge import fetch_remote_pending_status, stream_remote_completions_forever
from app.worker.coordination.events import WorkerEventBus, WorkerEventType
from app.worker.coordination.inbox import start_inbox_watcher
from app.worker.coordination.ops import WorkerOpsWindows, build_ops_snapshot, record_window_event
from app.worker.finalization import finalize_job_error, finalize_job_terminal
from app.worker.pending import poll_pending_jobs
from app.worker.runtime import PendingWorkerJob
from app.worker.submit import handle_submit_result, refill_from_inbox, submit_worker_loop


@dataclass(frozen=True)
class WorkerCoordinatorConfig:
  queue_root: QueueRoot
  consumer_id: str
  max_outstanding: int
  inbox_debounce_ms: int
  tick_interval_s: float
  metrics_log_interval_s: float
  pending_status_poll_s: float
  ops_enabled: bool
  ops_host: str
  ops_port: int
  ops_window_s: float
  ops_running_stuck_threshold_s: int
  submit_thread_name: str = "worker-submit"
  completion_thread_name: str = "worker-completion-stream"


@dataclass
class _WorkerLoopCounters:
  inbox_events: int = 0
  sse_reconnects: int = 0
  feed_resets: int = 0
  submits_started: int = 0
  submits_succeeded: int = 0
  submits_failed: int = 0
  scheduler_refill_cycles: int = 0
  completions_seen: int = 0
  completions_matched: int = 0
  last_log_mono: float = field(default_factory=time.monotonic)


@dataclass
class _WorkerRuntime:
  event_bus: WorkerEventBus
  inbox_watcher: Any
  submit_queue: "queue.Queue[Any]"
  submit_thread: threading.Thread
  completion_stop: threading.Event
  completion_thread: threading.Thread


def _maybe_log_worker_counters(
  *,
  queue_name: str,
  consumer_id: str,
  counters: _WorkerLoopCounters,
  pending_count: int,
  submitting_count: int,
  interval_s: float,
  force: bool = False,
) -> None:
  now = time.monotonic()
  if not force and (now - float(counters.last_log_mono)) < max(0.0, float(interval_s)):
    return
  counters.last_log_mono = now
  print(
    "worker_daemon counters "
    f"queue={queue_name} consumer_id={consumer_id} "
    f"inbox_events={int(counters.inbox_events)} "
    f"sse_reconnects={int(counters.sse_reconnects)} "
    f"feed_resets={int(counters.feed_resets)} "
    f"submits_started={int(counters.submits_started)} "
    f"submits_succeeded={int(counters.submits_succeeded)} "
    f"submits_failed={int(counters.submits_failed)} "
    f"scheduler_refill_cycles={int(counters.scheduler_refill_cycles)} "
    f"completions_seen={int(counters.completions_seen)} "
    f"completions_matched={int(counters.completions_matched)} "
    f"pending={max(0, int(pending_count))} "
    f"submitting={max(0, int(submitting_count))}",
    flush=True,
  )


def _start_worker_runtime(*, cfg: WorkerCoordinatorConfig) -> _WorkerRuntime:
  event_bus = WorkerEventBus()
  inbox_watcher = start_inbox_watcher(
    inbox_dir=cfg.queue_root.inbox,
    event_bus=event_bus,
    debounce_ms=cfg.inbox_debounce_ms,
  )
  submit_queue: "queue.Queue[Any]" = queue.Queue(maxsize=max(1, int(cfg.max_outstanding)))
  submit_thread = threading.Thread(
    target=submit_worker_loop,
    kwargs={
      "submit_queue": submit_queue,
      "event_bus": event_bus,
      "consumer_id": cfg.consumer_id,
    },
    name=cfg.submit_thread_name,
    daemon=True,
  )
  submit_thread.start()
  completion_stop = threading.Event()
  completion_thread = threading.Thread(
    target=_completion_stream_worker_loop,
    kwargs={
      "consumer_id": cfg.consumer_id,
      "event_bus": event_bus,
      "stop_event": completion_stop,
    },
    name=cfg.completion_thread_name,
    daemon=True,
  )
  completion_thread.start()
  return _WorkerRuntime(
    event_bus=event_bus,
    inbox_watcher=inbox_watcher,
    submit_queue=submit_queue,
    submit_thread=submit_thread,
    completion_stop=completion_stop,
    completion_thread=completion_thread,
  )


def _stop_worker_runtime(runtime: _WorkerRuntime) -> None:
  runtime.completion_stop.set()
  runtime.completion_thread.join(timeout=1.0)
  runtime.inbox_watcher.close()
  runtime.submit_queue.put(None)
  runtime.submit_thread.join(timeout=1.0)


def _handle_submit_result_event(
  *,
  payload: dict[str, Any],
  pending: dict[str, PendingWorkerJob],
  submitting: dict[str, PendingWorkerJob],
  counters: _WorkerLoopCounters,
  windows: WorkerOpsWindows,
  window_s: float,
) -> bool:
  now_mono = time.monotonic()
  pending_job = payload.get("pending")
  job_id = str(getattr(getattr(pending_job, "job", None), "job_id", "") or "").strip()
  if job_id:
    submitting.pop(job_id, None)
  err_msg = str(payload.get("error") or "").strip()
  submit = dict(payload.get("submit") or {})
  request_id = str(submit.get("request_id") or "").strip()
  if err_msg or not request_id:
    counters.submits_failed += 1
    record_window_event(windows.submit_failed_ts, now_mono, window_s=window_s)
  else:
    counters.submits_succeeded += 1
  return bool(handle_submit_result(payload=payload, pending=pending))


def _handle_completion_event(
  *,
  event: dict[str, Any],
  pending: dict[str, PendingWorkerJob],
  counters: _WorkerLoopCounters,
  windows: WorkerOpsWindows,
  window_s: float,
) -> bool:
  rid = str(event.get("request_id") or "").strip()
  if not rid:
    return False
  counters.completions_seen += 1
  pending_job = pending.pop(rid, None)
  if pending_job is None:
    return False
  counters.completions_matched += 1
  record_window_event(windows.completions_ts, time.monotonic(), window_s=window_s)
  try:
    finalize_job_terminal(pending=pending_job, event=event)
    print(f"Done {pending_job.job.job_id} state={str(event.get('state') or '')}")
  except Exception as e:
    finalize_job_error(pending=pending_job, exc=e)
    print(f"Error {pending_job.job.job_id}: {e!r}")
  return True


def _completion_feed_reset_error(*, old_feed_id: str, new_feed_id: str) -> str:
  old_short = (str(old_feed_id or "").strip() or "unknown")[:12]
  new_short = (str(new_feed_id or "").strip() or "unknown")[:12]
  return (
    "ASR pool completion feed reset detected "
    f"(old_feed_id={old_short}, new_feed_id={new_short}); "
    "in-flight jobs before the restart are not recovered in v3."
  )


def _pending_request_ids_still_visible(*, consumer_id: str, request_ids: list[str]) -> set[str]:
  rows = fetch_remote_pending_status(
    consumer_id=consumer_id,
    request_ids=list(request_ids or []),
    limit=200,
  )
  keep_request_ids: set[str] = set()
  for row in rows:
    rid = str(row.get("request_id") or "").strip()
    if rid:
      keep_request_ids.add(rid)
  return keep_request_ids


def _fail_pending_due_to_feed_reset(
  *,
  pending: dict[str, PendingWorkerJob],
  consumer_id: str,
  old_feed_id: str,
  new_feed_id: str,
) -> None:
  if not pending:
    return
  err_msg = _completion_feed_reset_error(old_feed_id=old_feed_id, new_feed_id=new_feed_id)
  keep_request_ids = _pending_request_ids_still_visible(
    consumer_id=consumer_id,
    request_ids=list(pending.keys()),
  )
  failed_request_ids: list[str] = [str(rid) for rid in pending.keys() if str(rid) not in keep_request_ids]
  for request_id in failed_request_ids:
    pending_job = pending.pop(request_id, None)
    if pending_job is None:
      continue
    finalize_job_error(
      pending=pending_job,
      exc=RuntimeError(f"ASR_POOL_FEED_RESET: {err_msg}"),
    )
    print(f"Error {pending_job.job.job_id}: {err_msg}")


def _completion_stream_worker_loop(
  *,
  consumer_id: str,
  event_bus: WorkerEventBus,
  stop_event: threading.Event,
) -> None:
  def _on_event(kind: str, payload: dict[str, Any]) -> None:
    if kind == "completion":
      event_bus.put(WorkerEventType.COMPLETION_EVENT, {"event": dict(payload or {})})
      return
    if kind == "feed_reset":
      event_bus.put(WorkerEventType.FEED_RESET, dict(payload or {}))
      return
    if kind == "stream_error":
      err_payload = dict(payload or {})
      err_payload["reason"] = "completion_stream_error"
      event_bus.put(WorkerEventType.TICK, err_payload)

  stream_remote_completions_forever(
    consumer_id=consumer_id,
    start_since_seq=0,
    stop_event=stop_event,
    on_event=_on_event,
  )


def run_worker_loop(cfg: WorkerCoordinatorConfig) -> int:
  queue_root = cfg.queue_root
  for state_dir in (queue_root.inbox, queue_root.running, queue_root.done, queue_root.error):
    state_dir.mkdir(parents=True, exist_ok=True)

  runtime = _start_worker_runtime(cfg=cfg)
  counters = _WorkerLoopCounters()
  pending: dict[str, PendingWorkerJob] = {}
  submitting: dict[str, PendingWorkerJob] = {}
  poll_state = {
    "interval_s": cfg.pending_status_poll_s,
    "last_pending_status_poll_mono": 0.0,
  }
  ops_store = OpsSnapshotStore()
  ops_server = WorkerOpsServer(host=cfg.ops_host, port=cfg.ops_port, store=ops_store)
  if cfg.ops_enabled:
    ops_server.start()
  windows = WorkerOpsWindows()
  inbox_dirty = True
  print(
    f"worker_daemon started queue={queue_root.name} "
    f"base={queue_root.base} consumer_id={cfg.consumer_id} max_outstanding={cfg.max_outstanding}"
  )
  runtime.event_bus.put(WorkerEventType.TICK, {"reason": "startup"})
  try:
    while True:
      ev = runtime.event_bus.get(timeout_s=cfg.tick_interval_s)
      if ev is not None and ev.kind == WorkerEventType.SHUTDOWN:
        break

      did_work = False
      if ev is not None:
        if ev.kind == WorkerEventType.INBOX_DIRTY:
          counters.inbox_events += 1
          record_window_event(windows.inbox_events_ts, time.monotonic(), window_s=cfg.ops_window_s)
          inbox_dirty = True
        elif ev.kind == WorkerEventType.SUBMIT_RESULT:
          payload = dict(ev.payload or {})
          event_did_work = _handle_submit_result_event(
            payload=payload,
            pending=pending,
            submitting=submitting,
            counters=counters,
            windows=windows,
            window_s=cfg.ops_window_s,
          )
          if event_did_work:
            did_work = True
            inbox_dirty = True
        elif ev.kind == WorkerEventType.COMPLETION_EVENT:
          event = dict((ev.payload or {}).get("event") or {})
          event_did_work = _handle_completion_event(
            event=event,
            pending=pending,
            counters=counters,
            windows=windows,
            window_s=cfg.ops_window_s,
          )
          if event_did_work:
            did_work = True
            inbox_dirty = True
        elif ev.kind == WorkerEventType.FEED_RESET:
          counters.feed_resets += 1
          record_window_event(windows.feed_resets_ts, time.monotonic(), window_s=cfg.ops_window_s)
          old_feed_id = str((ev.payload or {}).get("old_feed_id") or "").strip()
          new_feed_id = str((ev.payload or {}).get("new_feed_id") or "").strip()
          _fail_pending_due_to_feed_reset(
            pending=pending,
            consumer_id=cfg.consumer_id,
            old_feed_id=old_feed_id,
            new_feed_id=new_feed_id,
          )
          did_work = True
          inbox_dirty = True
          print(
            f"worker_daemon queue={queue_root.name} completion_feed_reset "
            f"old_feed_id={old_feed_id[:12]} new_feed_id={new_feed_id[:12]} since_seq_reset=0"
          )
        elif ev.kind == WorkerEventType.TICK:
          reason = str((ev.payload or {}).get("reason") or "").strip().lower()
          if reason == "completion_stream_error":
            counters.sse_reconnects += 1
            record_window_event(windows.sse_reconnects_ts, time.monotonic(), window_s=cfg.ops_window_s)
            code = str((ev.payload or {}).get("code") or "ASR_COMPLETIONS_STREAM_ERROR").strip()
            message = str((ev.payload or {}).get("message") or "completion stream error").strip()
            since_seq = int((ev.payload or {}).get("since_seq") or 0)
            retryable = (ev.payload or {}).get("retryable")
            print(
              "worker_daemon completion_stream_error "
              f"queue={queue_root.name} consumer_id={cfg.consumer_id} "
              f"code={code} message={message} since_seq={since_seq} retryable={retryable}",
              flush=True,
            )
        elif ev.kind != WorkerEventType.TICK:
          continue

      if pending:
        poll_pending_jobs(
          consumer_id=cfg.consumer_id,
          pending=pending,
          poll_state=poll_state,
        )

      if inbox_dirty:
        counters.scheduler_refill_cycles += 1
        refill_did_work, inbox_dirty = refill_from_inbox(
          queue_root=queue_root,
          pending=pending,
          submitting=submitting,
          max_outstanding=cfg.max_outstanding,
          submit_queue=runtime.submit_queue,
          counters=counters,
        )
        did_work = refill_did_work or did_work

      ops_store.set_snapshot(
        build_ops_snapshot(
          queue_root=queue_root,
          max_outstanding=cfg.max_outstanding,
          pending_count=len(pending),
          submitting_count=len(submitting),
          windows=windows,
          window_s=cfg.ops_window_s,
          running_stuck_threshold_s=cfg.ops_running_stuck_threshold_s,
        )
      )

      if did_work:
        runtime.event_bus.put(WorkerEventType.TICK, {"reason": "followup"})
      _maybe_log_worker_counters(
        queue_name=queue_root.name,
        consumer_id=cfg.consumer_id,
        counters=counters,
        pending_count=len(pending),
        submitting_count=len(submitting),
        interval_s=cfg.metrics_log_interval_s,
        force=False,
      )
  finally:
    _maybe_log_worker_counters(
      queue_name=queue_root.name,
      consumer_id=cfg.consumer_id,
      counters=counters,
      pending_count=len(pending),
      submitting_count=len(submitting),
      interval_s=cfg.metrics_log_interval_s,
      force=True,
    )
    _stop_worker_runtime(runtime)
    ops_server.stop()
  return 0

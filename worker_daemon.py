from __future__ import annotations

import os
import queue
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent

from queue_fs import QueueRoot
from asr_client_remote import (
  fetch_remote_pending_status,
  stream_remote_completions_forever,
)
from event_loop import WorkerEventBus, WorkerEventType
from filebacked_finalization import (
  finalize_filebacked_job_error,
  finalize_filebacked_job_terminal,
)
from filebacked_worker_runtime import (
  PendingWorkerJob,
  filebacked_submit_worker_loop,
  handle_filebacked_submit_result,
  poll_filebacked_pending,
  refill_filebacked_from_inbox,
)
from inbox_watch import start_inbox_watcher
from ops_server import OpsSnapshotStore, WorkerOpsServer
from worker_config import get_float, get_int, get_str


def _resolve_queue_base() -> Path:
  env_base = str(os.getenv("ASR_WORKER_QUEUE_BASE") or "").strip()
  cfg_base = get_str("worker.queue_base", "").strip()
  raw = env_base or cfg_base
  if not raw:
    raise RuntimeError("Missing worker queue base: set ASR_WORKER_QUEUE_BASE or worker.queue_base")
  p = Path(raw)
  return p if p.is_absolute() else (_REPO_ROOT / p).resolve()


def _worker_queue_root() -> QueueRoot:
  base = _resolve_queue_base()
  queue_name = (
    str(os.getenv("ASR_WORKER_QUEUE_NAME") or "").strip()
    or get_str("worker.queue_name", "").strip()
    or str(base.name or "worker")
  )
  return QueueRoot(
    name=queue_name,
    base=base,
    inbox=base / "inbox",
    running=base / "running",
    done=base / "done",
    error=base / "error",
  )


def _worker_max_outstanding() -> int:
  raw = str(os.getenv("ASR_WORKER_MAX_OUTSTANDING") or "").strip()
  if raw:
    try:
      parsed = int(raw)
    except Exception as e:
      raise RuntimeError(f"Invalid ASR_WORKER_MAX_OUTSTANDING: {raw!r}") from e
    return max(1, parsed)
  return get_int("worker.max_outstanding_requests", 1, min_value=1)


def _worker_consumer_id() -> str:
  cid = str(os.getenv("ASR_WORKER_CONSUMER_ID") or "").strip() or get_str("worker.consumer_id", "").strip()
  if cid:
    return cid
  instance = get_str("worker.instance", "").strip() or "1"
  return f"worker@{instance}"


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
class _WorkerOpsWindows:
  submit_started_ts: deque[float] = field(default_factory=deque)
  submit_failed_ts: deque[float] = field(default_factory=deque)
  completions_ts: deque[float] = field(default_factory=deque)
  inbox_events_ts: deque[float] = field(default_factory=deque)
  sse_reconnects_ts: deque[float] = field(default_factory=deque)
  feed_resets_ts: deque[float] = field(default_factory=deque)


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


def _env_bool(name: str, default: bool) -> bool:
  raw = str(os.getenv(name) or "").strip().lower()
  if not raw:
    return bool(default)
  return raw in {"1", "true", "yes", "on"}


def _iso_utc_now() -> str:
  return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _record_window_event(window: deque[float], now_mono: float, *, window_s: float) -> None:
  window.append(float(now_mono))
  _trim_window(window, now_mono=now_mono, window_s=window_s)


def _trim_window(window: deque[float], *, now_mono: float, window_s: float) -> None:
  cutoff = float(now_mono) - max(1.0, float(window_s))
  while window and float(window[0]) < cutoff:
    window.popleft()


def _count_jobs(path: Path) -> int:
  try:
    return sum(1 for p in path.iterdir() if p.is_dir() and not str(p.name).startswith("."))
  except Exception:
    return 0


def _oldest_age_s(path: Path) -> float | None:
  oldest_mtime: float | None = None
  now = time.time()
  try:
    for p in path.iterdir():
      if (not p.is_dir()) or str(p.name).startswith("."):
        continue
      try:
        mtime = float(p.stat().st_mtime)
      except Exception:
        continue
      if oldest_mtime is None or mtime < oldest_mtime:
        oldest_mtime = mtime
  except Exception:
    return None
  if oldest_mtime is None:
    return None
  return max(0.0, now - oldest_mtime)


def _running_over_threshold_count(path: Path, *, threshold_s: float) -> int:
  if threshold_s <= 0.0:
    return 0
  now = time.time()
  out = 0
  try:
    for p in path.iterdir():
      if (not p.is_dir()) or str(p.name).startswith("."):
        continue
      try:
        age_s = max(0.0, now - float(p.stat().st_mtime))
      except Exception:
        continue
      if age_s >= threshold_s:
        out += 1
  except Exception:
    return 0
  return out


def _build_ops_snapshot(
  *,
  queue_root: QueueRoot,
  max_outstanding: int,
  pending_count: int,
  submitting_count: int,
  windows: _WorkerOpsWindows,
  window_s: float,
  running_stuck_threshold_s: int,
) -> dict[str, Any]:
  now_mono = time.monotonic()
  _trim_window(windows.submit_started_ts, now_mono=now_mono, window_s=window_s)
  _trim_window(windows.submit_failed_ts, now_mono=now_mono, window_s=window_s)
  _trim_window(windows.completions_ts, now_mono=now_mono, window_s=window_s)
  _trim_window(windows.inbox_events_ts, now_mono=now_mono, window_s=window_s)
  _trim_window(windows.sse_reconnects_ts, now_mono=now_mono, window_s=window_s)
  _trim_window(windows.feed_resets_ts, now_mono=now_mono, window_s=window_s)

  inbox_count = _count_jobs(queue_root.inbox)
  running_count = _count_jobs(queue_root.running)
  done_count = _count_jobs(queue_root.done)
  error_count = _count_jobs(queue_root.error)
  oldest_inbox_s = _oldest_age_s(queue_root.inbox)
  oldest_running_s = _oldest_age_s(queue_root.running)

  submits_started_5m = len(windows.submit_started_ts)
  submits_failed_5m = len(windows.submit_failed_ts)
  submit_fail_rate_5m = float(submits_failed_5m) / float(submits_started_5m) if submits_started_5m > 0 else 0.0
  jobs_completed_5m = len(windows.completions_ts)
  running_over_threshold_count = _running_over_threshold_count(
    queue_root.running,
    threshold_s=float(running_stuck_threshold_s),
  )

  health = "ok"
  health_reason = "Healthy"
  if (
    running_over_threshold_count > 0
    or ((oldest_inbox_s or 0.0) >= 60.0 and inbox_count > 0 and pending_count >= max_outstanding)
  ):
    health = "error"
    if running_over_threshold_count > 0:
      health_reason = "Running jobs exceeded stuck threshold"
    else:
      health_reason = "Inbox backlog age above 60s with no free submission capacity"
  elif (
    (oldest_inbox_s or 0.0) >= 20.0
    or submit_fail_rate_5m >= 0.05
    or len(windows.sse_reconnects_ts) >= 3
  ):
    health = "warn"
    if (oldest_inbox_s or 0.0) >= 20.0:
      health_reason = "Inbox backlog age above 20s"
    elif submit_fail_rate_5m >= 0.05:
      health_reason = "5m submit failure rate above threshold"
    else:
      health_reason = "Repeated SSE reconnects in 5m window"

  return {
    "service": "asr-worker",
    "version": "ops_v1",
    "now_utc": _iso_utc_now(),
    "window_s": int(window_s),
    "health": health,
    "health_reason": health_reason,
    "summary": {
      "queue_name": queue_root.name,
      "inbox_count": inbox_count,
      "running_count": running_count,
      "done_count": done_count,
      "error_count": error_count,
      "oldest_inbox_s": oldest_inbox_s,
      "oldest_running_s": oldest_running_s,
      "submit_fail_rate_5m": round(submit_fail_rate_5m, 4),
      "jobs_completed_5m": jobs_completed_5m,
    },
    "details": {
      "worker_loop": {
        "max_outstanding": int(max_outstanding),
        "pending_count": int(pending_count),
        "submitting_count": int(submitting_count),
      },
      "events_5m": {
        "feed_resets": len(windows.feed_resets_ts),
        "sse_reconnects": len(windows.sse_reconnects_ts),
        "inbox_events": len(windows.inbox_events_ts),
      },
      "stuck": {
        "running_over_threshold_count": int(running_over_threshold_count),
        "threshold_s": int(running_stuck_threshold_s),
      },
    },
  }


def _start_worker_runtime(
  *,
  inbox_dir: Path,
  consumer_id: str,
  max_outstanding: int,
  submit_thread_name: str,
  completion_thread_name: str,
) -> _WorkerRuntime:
  event_bus = WorkerEventBus()
  inbox_watcher = start_inbox_watcher(
    inbox_dir=inbox_dir,
    event_bus=event_bus,
    debounce_ms=get_int("worker_events.inbox_debounce_ms", 40, min_value=0),
  )
  submit_queue: "queue.Queue[Any]" = queue.Queue(maxsize=max(1, int(max_outstanding)))
  submit_thread = threading.Thread(
    target=filebacked_submit_worker_loop,
    kwargs={
      "submit_queue": submit_queue,
      "event_bus": event_bus,
      "consumer_id": consumer_id,
    },
    name=submit_thread_name,
    daemon=True,
  )
  submit_thread.start()
  completion_stop = threading.Event()
  completion_thread = threading.Thread(
    target=_completion_stream_worker_loop,
    kwargs={
      "consumer_id": consumer_id,
      "event_bus": event_bus,
      "stop_event": completion_stop,
    },
    name=completion_thread_name,
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
  windows: _WorkerOpsWindows,
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
    _record_window_event(windows.submit_failed_ts, now_mono, window_s=window_s)
  else:
    counters.submits_succeeded += 1
  return bool(handle_filebacked_submit_result(payload=payload, pending=pending))


def _handle_completion_event(
  *,
  event: dict[str, Any],
  pending: dict[str, PendingWorkerJob],
  counters: _WorkerLoopCounters,
  windows: _WorkerOpsWindows,
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
  _record_window_event(windows.completions_ts, time.monotonic(), window_s=window_s)
  try:
    finalize_filebacked_job_terminal(pending=pending_job, event=event)
    print(f"Done {pending_job.job.job_id} state={str(event.get('state') or '')}")
  except Exception as e:
    finalize_filebacked_job_error(pending=pending_job, exc=e)
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
    finalize_filebacked_job_error(
      pending=pending_job,
      exc=RuntimeError(f"ASR_POOL_FEED_RESET: {err_msg}"),
    )
    print(f"Error {pending_job.job.job_id}: {err_msg}")


def _run_worker_submit_reap() -> int:
  queue_root = _worker_queue_root()
  for state_dir in (queue_root.inbox, queue_root.running, queue_root.done, queue_root.error):
    state_dir.mkdir(parents=True, exist_ok=True)
  max_outstanding = _worker_max_outstanding()
  submit_thread_name = "worker-submit"
  completion_thread_name = "worker-completion-stream"

  consumer_id = _worker_consumer_id()
  tick_interval_s = max(0.05, float(get_float("worker_events.coordinator_tick_s", 0.2, min_value=0.05)))
  metrics_log_interval_s = max(1.0, float(get_float("worker_events.metrics_log_interval_s", 30.0, min_value=1.0)))

  runtime = _start_worker_runtime(
    inbox_dir=queue_root.inbox,
    consumer_id=consumer_id,
    max_outstanding=max_outstanding,
    submit_thread_name=submit_thread_name,
    completion_thread_name=completion_thread_name,
  )
  counters = _WorkerLoopCounters()
  pending: dict[str, PendingWorkerJob] = {}
  submitting: dict[str, PendingWorkerJob] = {}
  poll_state = {
    "interval_s": get_float("polling_intervals.asr_remote_pending_status_poll_s", 1.0, min_value=0.2),
    "last_pending_status_poll_mono": 0.0,
  }
  ops_enabled = _env_bool("ASR_WORKER_OPS_ENABLED", True)
  ops_host = str(os.getenv("ASR_WORKER_OPS_HOST") or "").strip() or "127.0.0.1"
  ops_port = int(str(os.getenv("ASR_WORKER_OPS_PORT") or "").strip() or "18110")
  ops_window_s = float(str(os.getenv("ASR_WORKER_OPS_WINDOW_S") or "").strip() or "300")
  ops_running_stuck_threshold_s = int(str(os.getenv("ASR_WORKER_OPS_RUNNING_STUCK_S") or "").strip() or "900")
  ops_store = OpsSnapshotStore()
  ops_server = WorkerOpsServer(host=ops_host, port=ops_port, store=ops_store)
  if ops_enabled:
    ops_server.start()
  windows = _WorkerOpsWindows()
  inbox_dirty = True
  print(
    f"worker_daemon started queue={queue_root.name} "
    f"base={queue_root.base} consumer_id={consumer_id} max_outstanding={max_outstanding}"
  )
  runtime.event_bus.put(WorkerEventType.TICK, {"reason": "startup"})
  try:
    while True:
      ev = runtime.event_bus.get(timeout_s=tick_interval_s)
      if ev is not None and ev.kind == WorkerEventType.SHUTDOWN:
        break

      did_work = False
      if ev is not None:
        if ev.kind == WorkerEventType.INBOX_DIRTY:
          counters.inbox_events += 1
          _record_window_event(windows.inbox_events_ts, time.monotonic(), window_s=ops_window_s)
          inbox_dirty = True
        elif ev.kind == WorkerEventType.SUBMIT_RESULT:
          payload = dict(ev.payload or {})
          event_did_work = _handle_submit_result_event(
            payload=payload,
            pending=pending,
            submitting=submitting,
            counters=counters,
            windows=windows,
            window_s=ops_window_s,
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
            window_s=ops_window_s,
          )
          if event_did_work:
            did_work = True
            inbox_dirty = True
        elif ev.kind == WorkerEventType.FEED_RESET:
          counters.feed_resets += 1
          _record_window_event(windows.feed_resets_ts, time.monotonic(), window_s=ops_window_s)
          old_feed_id = str((ev.payload or {}).get("old_feed_id") or "").strip()
          new_feed_id = str((ev.payload or {}).get("new_feed_id") or "").strip()
          _fail_pending_due_to_feed_reset(
            pending=pending,
            consumer_id=consumer_id,
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
            _record_window_event(windows.sse_reconnects_ts, time.monotonic(), window_s=ops_window_s)
            code = str((ev.payload or {}).get("code") or "ASR_COMPLETIONS_STREAM_ERROR").strip()
            message = str((ev.payload or {}).get("message") or "completion stream error").strip()
            since_seq = int((ev.payload or {}).get("since_seq") or 0)
            retryable = (ev.payload or {}).get("retryable")
            print(
              "worker_daemon completion_stream_error "
              f"queue={queue_root.name} consumer_id={consumer_id} "
              f"code={code} message={message} since_seq={since_seq} retryable={retryable}",
              flush=True,
            )
        elif ev.kind != WorkerEventType.TICK:
          continue

      if pending:
        poll_filebacked_pending(
          consumer_id=consumer_id,
          pending=pending,
          poll_state=poll_state,
        )

      if inbox_dirty:
        counters.scheduler_refill_cycles += 1
        refill_did_work, inbox_dirty = refill_filebacked_from_inbox(
          queue_root=queue_root,
          pending=pending,
          submitting=submitting,
          max_outstanding=max_outstanding,
          submit_queue=runtime.submit_queue,
          counters=counters,
        )
        did_work = refill_did_work or did_work

      ops_store.set_snapshot(
        _build_ops_snapshot(
          queue_root=queue_root,
          max_outstanding=max_outstanding,
          pending_count=len(pending),
          submitting_count=len(submitting),
          windows=windows,
          window_s=ops_window_s,
          running_stuck_threshold_s=ops_running_stuck_threshold_s,
        )
      )

      if did_work:
        runtime.event_bus.put(WorkerEventType.TICK, {"reason": "followup"})
      _maybe_log_worker_counters(
        queue_name=queue_root.name,
        consumer_id=consumer_id,
        counters=counters,
        pending_count=len(pending),
        submitting_count=len(submitting),
        interval_s=metrics_log_interval_s,
        force=False,
      )
  finally:
    _maybe_log_worker_counters(
      queue_name=queue_root.name,
      consumer_id=consumer_id,
      counters=counters,
      pending_count=len(pending),
      submitting_count=len(submitting),
      interval_s=metrics_log_interval_s,
      force=True,
    )
    _stop_worker_runtime(runtime)
    ops_server.stop()
  return 0


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


def main() -> int:
  return _run_worker_submit_reap()


if __name__ == "__main__":
  raise SystemExit(main())

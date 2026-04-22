from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.queue.fs import QueueRoot


@dataclass
class WorkerOpsWindows:
  submit_started_ts: deque[float] = field(default_factory=deque)
  submit_failed_ts: deque[float] = field(default_factory=deque)
  completions_ts: deque[float] = field(default_factory=deque)
  inbox_events_ts: deque[float] = field(default_factory=deque)
  sse_reconnects_ts: deque[float] = field(default_factory=deque)
  feed_resets_ts: deque[float] = field(default_factory=deque)


def _iso_utc_now() -> str:
  return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def record_window_event(window: deque[float], now_mono: float, *, window_s: float) -> None:
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


def build_ops_snapshot(
  *,
  queue_root: QueueRoot,
  max_outstanding: int,
  pending_count: int,
  submitting_count: int,
  windows: WorkerOpsWindows,
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

from __future__ import annotations

import os
from pathlib import Path

from app.config import get_float, get_int, get_str
from app.queue.fs import QueueRoot
from app.worker.coordination.loop import WorkerCoordinatorConfig, run_worker_loop

_REPO_ROOT = Path(__file__).resolve().parent.parent


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


def _env_bool(name: str, default: bool) -> bool:
  raw = str(os.getenv(name) or "").strip().lower()
  if not raw:
    return bool(default)
  return raw in {"1", "true", "yes", "on"}


def _worker_coordinator_config() -> WorkerCoordinatorConfig:
  return WorkerCoordinatorConfig(
    queue_root=_worker_queue_root(),
    consumer_id=_worker_consumer_id(),
    max_outstanding=_worker_max_outstanding(),
    inbox_debounce_ms=get_int("worker_events.inbox_debounce_ms", 40, min_value=0),
    tick_interval_s=max(0.05, float(get_float("worker_events.coordinator_tick_s", 0.2, min_value=0.05))),
    metrics_log_interval_s=max(1.0, float(get_float("worker_events.metrics_log_interval_s", 30.0, min_value=1.0))),
    pending_status_poll_s=get_float("polling_intervals.asr_remote_pending_status_poll_s", 1.0, min_value=0.2),
    ops_enabled=_env_bool("ASR_WORKER_OPS_ENABLED", True),
    ops_host=str(os.getenv("ASR_WORKER_OPS_HOST") or "").strip() or "127.0.0.1",
    ops_port=int(str(os.getenv("ASR_WORKER_OPS_PORT") or "").strip() or "18110"),
    ops_window_s=float(str(os.getenv("ASR_WORKER_OPS_WINDOW_S") or "").strip() or "300"),
    ops_running_stuck_threshold_s=int(str(os.getenv("ASR_WORKER_OPS_RUNNING_STUCK_S") or "").strip() or "900"),
  )


def main() -> int:
  return run_worker_loop(_worker_coordinator_config())


if __name__ == "__main__":
  raise SystemExit(main())

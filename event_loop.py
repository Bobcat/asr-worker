from __future__ import annotations

import queue
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class WorkerEventType(str, Enum):
  INBOX_DIRTY = "INBOX_DIRTY"
  COMPLETION_EVENT = "COMPLETION_EVENT"
  FEED_RESET = "FEED_RESET"
  SUBMIT_RESULT = "SUBMIT_RESULT"
  TICK = "TICK"
  SHUTDOWN = "SHUTDOWN"


@dataclass(frozen=True)
class WorkerEvent:
  kind: WorkerEventType
  payload: dict[str, Any] = field(default_factory=dict)
  ts_mono: float = field(default_factory=time.monotonic)


class WorkerEventBus:
  """
  Thread-safe event bus for worker coordinator loops.
  """

  def __init__(self) -> None:
    self._q: queue.Queue[WorkerEvent] = queue.Queue()

  def put(self, kind: WorkerEventType, payload: dict[str, Any] | None = None) -> None:
    self._q.put(WorkerEvent(kind=kind, payload=dict(payload or {})))

  def get(self, *, timeout_s: float | None = None) -> WorkerEvent | None:
    timeout: float | None
    if timeout_s is None:
      timeout = None
    else:
      timeout = max(0.0, float(timeout_s))
    try:
      return self._q.get(timeout=timeout)
    except queue.Empty:
      return None

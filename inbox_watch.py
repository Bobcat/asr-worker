from __future__ import annotations

import ctypes
import errno
import os
import select
import struct
import threading
import time
from pathlib import Path

from event_loop import WorkerEventBus, WorkerEventType

_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.inotify_init1.argtypes = [ctypes.c_int]
_libc.inotify_init1.restype = ctypes.c_int
_libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
_libc.inotify_add_watch.restype = ctypes.c_int
_libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
_libc.inotify_rm_watch.restype = ctypes.c_int

_IN_CLOSE_WRITE = 0x00000008
_IN_MOVED_TO = 0x00000080
_IN_CREATE = 0x00000100
_IN_DELETE_SELF = 0x00000400
_IN_MOVE_SELF = 0x00000800
_IN_Q_OVERFLOW = 0x00004000
_IN_IGNORED = 0x00008000

_WATCH_MASK = _IN_CLOSE_WRITE | _IN_MOVED_TO | _IN_CREATE | _IN_DELETE_SELF | _IN_MOVE_SELF | _IN_Q_OVERFLOW
_EVENT_HEADER_STRUCT = struct.Struct("iIII")
_READ_SIZE = 64 * 1024


def _inotify_init_nonblocking() -> int:
  fd = int(_libc.inotify_init1(int(os.O_NONBLOCK | os.O_CLOEXEC)))
  if fd < 0:
    err = ctypes.get_errno()
    raise OSError(err, f"inotify_init1 failed (errno={err})")
  return fd


def _inotify_add_watch(fd: int, path: Path, mask: int) -> int:
  wd = int(_libc.inotify_add_watch(int(fd), str(path).encode("utf-8"), int(mask)))
  if wd < 0:
    err = ctypes.get_errno()
    if int(err) == int(errno.ENOSPC):
      raise OSError(
        err,
        (
          f"inotify_add_watch failed for {path} (errno={err}); "
          "inotify watch quota reached (fs.inotify.max_user_watches/max_user_instances), not disk space"
        ),
      )
    raise OSError(err, f"inotify_add_watch failed for {path} (errno={err})")
  return wd


class InboxWatcher:
  """
  Minimal inotify watcher for worker inbox wakeups.
  """

  def __init__(self, *, inbox_dir: Path, event_bus: WorkerEventBus, debounce_ms: int) -> None:
    self._inbox_dir = Path(inbox_dir).resolve()
    self._event_bus = event_bus
    self._debounce_s = max(0.0, float(debounce_ms) / 1000.0)
    self._stop = threading.Event()
    self._fd = _inotify_init_nonblocking()
    try:
      self._wd = _inotify_add_watch(self._fd, self._inbox_dir, _WATCH_MASK)
    except Exception:
      try:
        os.close(self._fd)
      except Exception:
        pass
      raise
    self._thread = threading.Thread(target=self._run, name="worker-inbox-watch", daemon=True)

  def start(self) -> None:
    self._thread.start()

  def close(self) -> None:
    self._stop.set()
    if self._thread.is_alive():
      self._thread.join(timeout=1.0)
    try:
      _libc.inotify_rm_watch(int(self._fd), int(self._wd))
    except Exception:
      pass
    try:
      os.close(self._fd)
    except Exception:
      pass

  def _run(self) -> None:
    dirty = False
    last_emit_mono = 0.0
    try:
      while not self._stop.is_set():
        timeout_s = 0.2
        if dirty and self._debounce_s > 0.0:
          due = last_emit_mono + self._debounce_s
          timeout_s = max(0.0, min(timeout_s, due - time.monotonic()))
        ready, _, _ = select.select([self._fd], [], [], timeout_s)
        if ready:
          while True:
            try:
              blob = os.read(self._fd, _READ_SIZE)
            except BlockingIOError:
              break
            if not blob:
              break
            offset = 0
            while offset + _EVENT_HEADER_STRUCT.size <= len(blob):
              _, mask, _, name_len = _EVENT_HEADER_STRUCT.unpack_from(blob, offset)
              offset += _EVENT_HEADER_STRUCT.size + int(name_len)
              if mask & (_IN_DELETE_SELF | _IN_MOVE_SELF | _IN_IGNORED | _IN_Q_OVERFLOW):
                self._event_bus.put(
                  WorkerEventType.SHUTDOWN,
                  {
                    "reason": "inbox_watch_lost",
                    "mask": int(mask),
                    "inbox_dir": str(self._inbox_dir),
                  },
                )
                return
              if mask & (_IN_CREATE | _IN_CLOSE_WRITE | _IN_MOVED_TO):
                dirty = True
        if not dirty:
          continue
        now = time.monotonic()
        if (now - last_emit_mono) >= self._debounce_s:
          self._event_bus.put(WorkerEventType.INBOX_DIRTY, {"source": "inotify"})
          last_emit_mono = now
          dirty = False
    except Exception as e:
      self._event_bus.put(
        WorkerEventType.SHUTDOWN,
        {
          "reason": "inbox_watch_failed",
          "error": repr(e),
          "inbox_dir": str(self._inbox_dir),
        },
      )


def start_inbox_watcher(*, inbox_dir: Path, event_bus: WorkerEventBus, debounce_ms: int) -> InboxWatcher:
  watcher = InboxWatcher(inbox_dir=inbox_dir, event_bus=event_bus, debounce_ms=debounce_ms)
  watcher.start()
  return watcher

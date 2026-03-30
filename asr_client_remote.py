from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import os
import sys
import threading

from asr_schema import ASR_SCHEMA_VERSION

_POOL_REPO_ROOT_CANDIDATES = [
  str(os.getenv("ASR_POOL_REPO_ROOT") or "").strip(),
  "/home/gunnar/projects/asr-pool-dev",
  "/srv/asr-pool",
]
for _candidate in _POOL_REPO_ROOT_CANDIDATES:
  if not _candidate:
    continue
  _module_path = Path(_candidate) / "asr_pool_transport.py"
  if _module_path.exists():
    if _candidate not in sys.path:
      sys.path.insert(0, _candidate)
    break

from asr_pool_transport import PoolTransportConfig
from asr_pool_transport import PoolTransportMultipartBuildError
from asr_pool_transport import download_request_srt_to_path as _transport_download_request_srt_to_path
from asr_pool_transport import fetch_pending_status as _transport_fetch_pending_status
from asr_pool_transport import stream_completions_forever as _transport_stream_completions_forever
from asr_pool_transport import submit_multipart_request as _transport_submit_multipart_request
from worker_config import get_float, get_int, get_str


def _build_error_response(
  *,
  request: dict[str, Any] | None,
  code: str,
  message: str,
  retryable: bool = False,
  details: dict[str, Any] | None = None,
) -> dict[str, Any]:
  req = dict(request or {})
  return {
    "schema_version": ASR_SCHEMA_VERSION,
    "request_id": str(req.get("request_id") or ""),
    "ok": False,
    "effective_options": dict(req.get("effective_options") or {}),
    "error": {
      "code": str(code),
      "message": str(message),
      "retryable": bool(retryable),
      "details": dict(details or {}),
    },
    "warnings": [],
  }


def _pool_base_url() -> str:
  raw = get_str("asr_pool.base_url", "http://127.0.0.1:8090")
  return raw.rstrip("/")


def _http_timeout_s() -> float:
  return get_float("asr_remote.http_timeout_s", 10.0, min_value=1.0)


def _retry_attempts() -> int:
  return get_int("asr_remote.retry_attempts", 3, min_value=1)


def _retry_base_delay_s() -> float:
  return get_float("asr_remote.retry_base_delay_s", 0.2, min_value=0.0)


def _retry_max_delay_s() -> float:
  return get_float("asr_remote.retry_max_delay_s", 2.0, min_value=0.05)


def _retry_jitter_s() -> float:
  return get_float("asr_remote.retry_jitter_s", 0.1, min_value=0.0)


def _stream_heartbeat_s() -> float:
  return get_float("worker_events.sse_heartbeat_s", 10.0, min_value=1.0)


def _transport_config() -> PoolTransportConfig:
  retry_base_delay_s = _retry_base_delay_s()
  return PoolTransportConfig(
    base_url=_pool_base_url(),
    token=get_str("asr_pool.token", ""),
    http_timeout_s=_http_timeout_s(),
    retry_attempts=_retry_attempts(),
    retry_base_delay_s=retry_base_delay_s,
    retry_max_delay_s=max(retry_base_delay_s, _retry_max_delay_s()),
    retry_jitter_s=_retry_jitter_s(),
    stream_heartbeat_s=_stream_heartbeat_s(),
  ).normalized()


def _with_consumer_id(request_payload: dict[str, Any], *, consumer_id: str) -> dict[str, Any]:
  req = dict(request_payload or {})
  cid = str(consumer_id or "").strip()
  if cid:
    req["consumer_id"] = cid
  return req


def _prepare_submit_payload(
  *,
  request_payload: dict[str, Any],
  consumer_id: str,
) -> tuple[dict[str, Any], Path | None, dict[str, Any] | None]:
  req = _with_consumer_id(dict(request_payload or {}), consumer_id=consumer_id)
  audio = dict(req.get("audio") or {})
  local_path = str(audio.get("local_path") or "").strip()
  if not local_path:
    return req, None, _build_error_response(
      request=req,
      code="ASR_REMOTE_INPUT_PATH_REQUIRED",
      message="audio.local_path is required for multipart ASR submit",
      retryable=False,
      details={},
    )
  src = Path(local_path).expanduser().resolve()
  if not src.exists() or not src.is_file():
    return req, None, _build_error_response(
      request=req,
      code="ASR_REMOTE_INPUT_PATH_MISSING",
      message=f"ASR input file not found: {src}",
      retryable=False,
      details={"local_path": str(src)},
    )
  audio["local_path"] = str(src)
  req["audio"] = audio
  return req, src, None


def submit_remote_pool_request(
  *,
  request_payload: dict[str, Any],
  consumer_id: str,
) -> dict[str, Any]:
  req, audio_path, prep_error = _prepare_submit_payload(
    request_payload=request_payload,
    consumer_id=consumer_id,
  )
  if prep_error is not None:
    return {
      "ok": False,
      "request_id": str(req.get("request_id") or ""),
      "prepared_request": req,
      "error_response": prep_error,
      "submit_lifecycle": {},
      "http_status": 0,
    }

  cfg = _transport_config()
  request_id = str(req.get("request_id") or "").strip()
  try:
    status_code, submit_body, attempts_used = _transport_submit_multipart_request(
      config=cfg,
      request_payload=req,
      audio_path=audio_path,
    )
  except PoolTransportMultipartBuildError as e:
    cause = e.__cause__
    exc_type = type(cause).__name__ if cause is not None else type(e).__name__
    return {
      "ok": False,
      "request_id": str(request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code="ASR_REMOTE_MULTIPART_BUILD_FAILED",
        message=f"Failed to build multipart ASR submit payload: {e}",
        retryable=False,
        details={"exc_type": exc_type},
      ),
      "submit_lifecycle": {},
      "http_status": 0,
    }
  except Exception as e:
    return {
      "ok": False,
      "request_id": str(request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code="ASR_REMOTE_SUBMIT_IO_FAILURE",
        message=f"ASR pool submit I/O failed: {type(e).__name__}: {e}",
        retryable=True,
        details={
          "pool_base_url": cfg.base_url,
          "request_id": request_id,
          "attempts": int(cfg.retry_attempts),
          "http_timeout_s": float(cfg.http_timeout_s),
          "exc_type": type(e).__name__,
        },
      ),
      "submit_lifecycle": {},
      "http_status": 0,
    }

  if status_code not in {200, 202}:
    return {
      "ok": False,
      "request_id": str(request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code=str(submit_body.get("code") or "ASR_REMOTE_SUBMIT_FAILED"),
        message=str(submit_body.get("message") or f"ASR pool submit failed with HTTP {status_code}"),
        retryable=bool(submit_body.get("retryable", True)),
        details={
          "http_status": int(status_code),
          "pool_base_url": cfg.base_url,
          "request_id": request_id,
          "submit_attempts": int(attempts_used),
          **dict(submit_body.get("details") or {}),
        },
      ),
      "submit_lifecycle": dict(submit_body or {}),
      "http_status": int(status_code),
    }

  rid = str(submit_body.get("request_id") or request_id or "").strip()
  return {
    "ok": True,
    "request_id": rid,
    "prepared_request": req,
    "submit_lifecycle": dict(submit_body or {}),
    "http_status": int(status_code),
  }


def fetch_remote_pending_status(
  *,
  consumer_id: str,
  request_ids: list[str],
  limit: int = 200,
) -> list[dict[str, Any]]:
  return _transport_fetch_pending_status(
    config=_transport_config(),
    consumer_id=consumer_id,
    request_ids=request_ids,
    limit=limit,
  )


def download_remote_request_srt_to_path(
  *,
  request_id: str,
  dst_path: Path,
  allow_empty: bool = False,
) -> Path:
  return _transport_download_request_srt_to_path(
    config=_transport_config(),
    request_id=request_id,
    dst_path=dst_path,
    allow_empty=allow_empty,
  )


def stream_remote_completions_forever(
  *,
  consumer_id: str,
  start_since_seq: int,
  stop_event: threading.Event,
  on_event: Callable[[str, dict[str, Any]], None],
) -> None:
  _transport_stream_completions_forever(
    config=_transport_config(),
    consumer_id=consumer_id,
    start_since_seq=start_since_seq,
    stop_event=stop_event,
    on_event=on_event,
  )

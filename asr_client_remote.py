from __future__ import annotations

import json
import mimetypes
import random
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from asr_schema import ASR_SCHEMA_VERSION
from worker_config import get_str, get_int, get_float


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


def _json_or_empty(raw: bytes) -> dict[str, Any]:
  if not raw:
    return {}
  try:
    parsed = json.loads(raw.decode("utf-8", errors="replace"))
  except Exception:
    return {}
  return dict(parsed) if isinstance(parsed, dict) else {}


def _http_request_once(
  *,
  method: str,
  url: str,
  token: str,
  timeout_s: float,
  body_bytes: bytes | None = None,
  content_type: str | None = None,
) -> tuple[int, dict[str, Any]]:
  req = urlrequest.Request(
    url,
    data=(bytes(body_bytes) if body_bytes is not None else None),
    method=str(method).upper(),
  )
  if content_type:
    req.add_header("Content-Type", str(content_type))
  if token:
    req.add_header("X-ASR-Token", token)
  try:
    with urlrequest.urlopen(req, timeout=float(timeout_s)) as resp:
      return int(getattr(resp, "status", 200) or 200), _json_or_empty(resp.read())
  except urlerror.HTTPError as e:
    return int(getattr(e, "code", 500) or 500), _json_or_empty(e.read())


def _retryable_http_status(status_code: int) -> bool:
  code = int(status_code)
  return code == 429 or code >= 500


def _backoff_sleep_s(*, retry_index: int, base_s: float, max_s: float, jitter_s: float) -> float:
  idx = max(0, int(retry_index))
  expo = float(base_s) * (2 ** idx)
  bounded = min(float(max_s), max(0.0, float(expo)))
  if float(jitter_s) > 0.0:
    bounded += random.uniform(0.0, float(jitter_s))
  return max(0.0, float(bounded))


def _http_request_with_retry(
  *,
  method: str,
  url: str,
  token: str,
  timeout_s: float,
  body_bytes: bytes | None = None,
  content_type: str | None = None,
  attempts: int,
  backoff_base_s: float,
  backoff_max_s: float,
  jitter_s: float,
) -> tuple[int, dict[str, Any], int]:
  max_attempts = max(1, int(attempts))
  last_exc: Exception | None = None
  for attempt in range(1, max_attempts + 1):
    try:
      status_code, body = _http_request_once(
        method=method,
        url=url,
        token=token,
        timeout_s=timeout_s,
        body_bytes=body_bytes,
        content_type=content_type,
      )
    except Exception as e:
      last_exc = e
      if attempt >= max_attempts:
        raise
      sleep_s = _backoff_sleep_s(
        retry_index=(attempt - 1),
        base_s=backoff_base_s,
        max_s=backoff_max_s,
        jitter_s=jitter_s,
      )
      if sleep_s > 0.0:
        time.sleep(sleep_s)
      continue

    if _retryable_http_status(status_code) and attempt < max_attempts:
      sleep_s = _backoff_sleep_s(
        retry_index=(attempt - 1),
        base_s=backoff_base_s,
        max_s=backoff_max_s,
        jitter_s=jitter_s,
      )
      if sleep_s > 0.0:
        time.sleep(sleep_s)
      continue
    return int(status_code), dict(body or {}), int(attempt)

  if last_exc is not None:
    raise last_exc
  return 500, {}, int(max_attempts)


def _remote_http_retry_config() -> tuple[str, str, float, int, float, float, float]:
  retry_base_delay_s = _retry_base_delay_s()
  return (
    _pool_base_url(),
    get_str("asr_pool.token", ""),
    _http_timeout_s(),
    _retry_attempts(),
    retry_base_delay_s,
    max(retry_base_delay_s, _retry_max_delay_s()),
    _retry_jitter_s(),
  )


def _with_consumer_id(request_payload: dict[str, Any], *, consumer_id: str) -> dict[str, Any]:
  req = dict(request_payload or {})
  cid = str(consumer_id or "").strip()
  if cid:
    req["consumer_id"] = cid
  return req


def _multipart_content_type_for_path(path: Path) -> str:
  guessed, _enc = mimetypes.guess_type(str(path.name))
  return str(guessed or "application/octet-stream")


def _build_multipart_submit_body(
  *,
  request_payload: dict[str, Any],
  audio_path: Path,
) -> tuple[bytes, str]:
  boundary = f"----asr-{uuid.uuid4().hex}"
  request_bytes = json.dumps(request_payload, ensure_ascii=False).encode("utf-8")
  file_bytes = audio_path.read_bytes()
  filename = str(audio_path.name or "audio.bin").replace("\"", "_")
  file_content_type = _multipart_content_type_for_path(audio_path)
  rows: list[bytes] = []
  rows.append(f"--{boundary}\r\n".encode("ascii"))
  rows.append(b"Content-Disposition: form-data; name=\"request_json\"\r\n")
  rows.append(b"Content-Type: application/json; charset=utf-8\r\n\r\n")
  rows.append(request_bytes)
  rows.append(b"\r\n")
  rows.append(f"--{boundary}\r\n".encode("ascii"))
  rows.append(f"Content-Disposition: form-data; name=\"audio_file\"; filename=\"{filename}\"\r\n".encode("utf-8"))
  rows.append(f"Content-Type: {file_content_type}\r\n\r\n".encode("ascii"))
  rows.append(file_bytes)
  rows.append(b"\r\n")
  rows.append(f"--{boundary}--\r\n".encode("ascii"))
  return b"".join(rows), f"multipart/form-data; boundary={boundary}"


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

  pool_base_url, token, http_timeout_s, retry_attempts, retry_base_delay_s, retry_max_delay_s, retry_jitter_s = _remote_http_retry_config()
  request_id = str(req.get("request_id") or "").strip()
  submit_url = urlparse.urljoin(pool_base_url + "/", "asr/v1/requests")
  try:
    body_bytes, content_type = _build_multipart_submit_body(
      request_payload=req,
      audio_path=audio_path,
    )
  except Exception as e:
    return {
      "ok": False,
      "request_id": str(request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code="ASR_REMOTE_MULTIPART_BUILD_FAILED",
        message=f"Failed to build multipart ASR submit payload: {type(e).__name__}: {e}",
        retryable=False,
        details={"exc_type": type(e).__name__},
      ),
      "submit_lifecycle": {},
      "http_status": 0,
    }
  try:
    status_code, submit_body, attempts_used = _http_request_with_retry(
      method="POST",
      url=submit_url,
      token=token,
      timeout_s=http_timeout_s,
      body_bytes=body_bytes,
      content_type=content_type,
      attempts=retry_attempts,
      backoff_base_s=retry_base_delay_s,
      backoff_max_s=retry_max_delay_s,
      jitter_s=retry_jitter_s,
    )
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
          "pool_base_url": pool_base_url,
          "request_id": request_id,
          "attempts": int(retry_attempts),
          "http_timeout_s": float(http_timeout_s),
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
          "pool_base_url": pool_base_url,
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
  pool_base_url, token, http_timeout_s, retry_attempts, retry_base_delay_s, retry_max_delay_s, retry_jitter_s = _remote_http_retry_config()
  clean_ids: list[str] = []
  seen: set[str] = set()
  for raw in list(request_ids or []):
    rid = str(raw or "").strip()
    if not rid or rid in seen:
      continue
    seen.add(rid)
    clean_ids.append(rid)
    if len(clean_ids) >= int(max(1, min(1000, int(limit)))):
      break
  query = urlparse.urlencode(
    {
      "consumer_id": str(consumer_id or ""),
      "limit": int(max(1, min(1000, int(limit)))),
      "request_id": clean_ids,
    },
    doseq=True,
  )
  url = urlparse.urljoin(pool_base_url + "/", f"asr/v1/pending-status?{query}")
  try:
    status_code, body, _attempts_used = _http_request_with_retry(
      method="GET",
      url=url,
      token=token,
      timeout_s=http_timeout_s,
      content_type="application/json",
      attempts=retry_attempts,
      backoff_base_s=retry_base_delay_s,
      backoff_max_s=retry_max_delay_s,
      jitter_s=retry_jitter_s,
    )
  except Exception:
    return []
  if int(status_code) != 200:
    return []
  rows = dict(body or {}).get("rows")
  if not isinstance(rows, list):
    return []
  return [row for row in rows if isinstance(row, dict)]


def download_remote_request_srt_to_path(
  *,
  request_id: str,
  dst_path: Path,
  allow_empty: bool = False,
) -> Path:
  rid = str(request_id or "").strip()
  if not rid:
    raise RuntimeError("Missing request_id for remote SRT fetch")
  pool_base_url = _pool_base_url()
  token = get_str("asr_pool.token", "")
  timeout_s = max(5.0, _http_timeout_s())
  safe_rid = urlparse.quote(rid, safe="")
  url = urlparse.urljoin(pool_base_url + "/", f"asr/v1/requests/{safe_rid}/artifacts/srt")
  req = urlrequest.Request(url, method="GET")
  if token:
    req.add_header("X-ASR-Token", token)
  try:
    with urlrequest.urlopen(req, timeout=float(timeout_s)) as resp:
      status_code = int(getattr(resp, "status", 200) or 200)
      data = bytes(resp.read() or b"")
      if status_code != 200:
        raise RuntimeError(f"ASR_REMOTE_SRT_FETCH_FAILED: Failed to fetch remote SRT (http={status_code})")
  except urlerror.HTTPError as e:
    status_code = int(getattr(e, "code", 500) or 500)
    body = _json_or_empty(e.read())
    code = str(body.get("code") or "ASR_REMOTE_SRT_FETCH_FAILED")
    msg = str(body.get("message") or f"Failed to fetch remote SRT (http={status_code})")
    raise RuntimeError(f"{code}: {msg}") from e
  except RuntimeError:
    raise
  except Exception as e:
    raise RuntimeError(f"ASR_REMOTE_ARTIFACT_FETCH_IO_FAILURE: {type(e).__name__}: {e}") from e
  if not data and not bool(allow_empty):
    raise RuntimeError("Remote SRT fetch returned empty payload")
  dst_path.parent.mkdir(parents=True, exist_ok=True)
  tmp = dst_path.with_suffix(dst_path.suffix + ".tmp")
  tmp.write_bytes(data)
  tmp.replace(dst_path)
  return dst_path.resolve()


def _parse_sse_event(*, event_name: str, data_lines: list[str]) -> tuple[str, dict[str, Any]]:
  raw = "\n".join(list(data_lines or [])).strip()
  if not raw:
    return str(event_name or "message").strip().lower(), {}
  try:
    parsed = json.loads(raw)
  except Exception:
    return str(event_name or "message").strip().lower(), {"raw": raw}
  return str(event_name or "message").strip().lower(), (dict(parsed) if isinstance(parsed, dict) else {"value": parsed})


def stream_remote_completions_forever(
  *,
  consumer_id: str,
  start_since_seq: int,
  stop_event: threading.Event,
  on_event: Callable[[str, dict[str, Any]], None],
) -> None:
  """
  Long-lived SSE reader for /asr/v1/completions/stream.
  Calls on_event(kind, payload) for:
    - completion
    - feed_reset
    - stream_error
  """
  cid = str(consumer_id or "").strip()
  if not cid:
    on_event(
      "stream_error",
      {
        "code": "ASR_COMPLETIONS_STREAM_CONSUMER_REQUIRED",
        "message": "consumer_id is required for stream reader",
      },
    )
    return

  pool_base_url = _pool_base_url()
  token = get_str("asr_pool.token", "")
  heartbeat_s = _stream_heartbeat_s()
  reconnect_base_s = _retry_base_delay_s()
  reconnect_max_s = max(reconnect_base_s, _retry_max_delay_s())
  reconnect_jitter_s = _retry_jitter_s()
  timeout_s = max(30.0, (float(heartbeat_s) * 3.0))
  since_seq = max(0, int(start_since_seq))
  last_feed_id = ""
  retry_index = 0

  while not stop_event.is_set():
    query = urlparse.urlencode(
      {
        "consumer_id": cid,
        "since_seq": max(0, int(since_seq)),
        "limit": 200,
        "heartbeat_s": float(heartbeat_s),
      }
    )
    stream_url = urlparse.urljoin(pool_base_url + "/", f"asr/v1/completions/stream?{query}")
    req = urlrequest.Request(stream_url, method="GET")
    req.add_header("Accept", "text/event-stream")
    if token:
      req.add_header("X-ASR-Token", token)
    try:
      with urlrequest.urlopen(req, timeout=float(timeout_s)) as resp:
        status_code = int(getattr(resp, "status", 200) or 200)
        if status_code != 200:
          on_event(
            "stream_error",
            {
              "code": "ASR_COMPLETIONS_STREAM_HTTP_ERROR",
              "status_code": int(status_code),
              "url": stream_url,
            },
          )
          raise RuntimeError(f"completions stream http={status_code}")
        retry_index = 0
        event_name = "message"
        data_lines: list[str] = []
        while not stop_event.is_set():
          raw_line = resp.readline()
          if not raw_line:
            break
          try:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
          except Exception:
            line = str(raw_line).rstrip("\r\n")
          if line == "":
            kind, payload = _parse_sse_event(event_name=event_name, data_lines=data_lines)
            event_name = "message"
            data_lines = []
            if kind == "meta":
              feed_id = str(payload.get("feed_id") or "").strip()
              next_seq_raw = payload.get("next_seq")
              next_seq = max(0, int(next_seq_raw or 0))
              feed_changed = bool(last_feed_id and feed_id and feed_id != last_feed_id)
              if feed_changed:
                on_event(
                  "feed_reset",
                  {
                    "old_feed_id": str(last_feed_id),
                    "new_feed_id": str(feed_id),
                  },
                )
              if feed_id:
                last_feed_id = feed_id
              if feed_changed:
                # Cursor must be reset for the new feed; old seq values are not comparable.
                # IMPORTANT: reset to 0 explicitly. The server's meta.next_seq can echo the
                # old client cursor when since_seq was from a different (old) feed, which
                # would otherwise skip all events on the new feed.
                since_seq = 0
                # Reconnect immediately so the server-side stream cursor also resets.
                break
              elif next_seq_raw is not None:
                since_seq = max(since_seq, next_seq)
            elif kind == "completion":
              seq = max(0, int(payload.get("seq") or 0))
              if seq > 0:
                since_seq = max(since_seq, seq + 1)
              on_event("completion", dict(payload))
            elif kind == "heartbeat":
              next_seq = max(0, int(payload.get("next_seq") or since_seq))
              feed_id = str(payload.get("feed_id") or "").strip()
              since_seq = max(since_seq, next_seq)
              if feed_id:
                last_feed_id = feed_id
            elif kind == "error":
              on_event(
                "stream_error",
                {
                  "code": str(payload.get("code") or "ASR_COMPLETIONS_STREAM_ERROR_EVENT"),
                  "message": str(payload.get("message") or "stream error event"),
                  "payload": dict(payload),
                },
              )
            continue
          if line.startswith(":"):
            continue
          if line.startswith("event:"):
            event_name = str(line[6:]).strip() or "message"
            continue
          if line.startswith("data:"):
            data_lines.append(str(line[5:]).lstrip())
            continue
    except Exception as e:
      if stop_event.is_set():
        break
      on_event(
        "stream_error",
        {
          "code": "ASR_COMPLETIONS_STREAM_IO_FAILURE",
          "message": f"{type(e).__name__}: {e}",
          "retryable": True,
        },
      )
      sleep_s = _backoff_sleep_s(
        retry_index=retry_index,
        base_s=reconnect_base_s,
        max_s=reconnect_max_s,
        jitter_s=reconnect_jitter_s,
      )
      retry_index += 1
      if sleep_s > 0.0 and not stop_event.is_set():
        stop_event.wait(timeout=float(sleep_s))

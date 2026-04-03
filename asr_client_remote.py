from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import threading

from asr_pool_api import (
  ASRAudioFile,
  ASRCompletionEvent,
  ASRCompletionFeedReset,
  ASROutputSelection,
  ASRPoolClient,
  ASRPoolClientConfig,
  ASRPoolError,
  ASRPoolRequestRejected,
  ASRRequestOptions,
  ASRRequestRouting,
  ASRSubmitRequest,
)
from asr_schema import ASR_SCHEMA_VERSION
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


def _client() -> ASRPoolClient:
  retry_base_delay_s = _retry_base_delay_s()
  return ASRPoolClient(
    ASRPoolClientConfig(
      base_url=_pool_base_url(),
      token=get_str("asr_pool.token", ""),
      http_timeout_s=_http_timeout_s(),
      retry_attempts=_retry_attempts(),
      retry_base_delay_s=retry_base_delay_s,
      retry_max_delay_s=max(retry_base_delay_s, _retry_max_delay_s()),
      retry_jitter_s=_retry_jitter_s(),
      stream_heartbeat_s=_stream_heartbeat_s(),
    )
  )


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


def _submit_request_from_payload(*, request_payload: dict[str, Any], audio_path: Path) -> ASRSubmitRequest:
  req = dict(request_payload or {})
  audio = dict(req.get("audio") or {})
  routing = dict(req.get("routing") or {})
  options = dict(req.get("options") or {})
  outputs = dict(req.get("outputs") or {})
  return ASRSubmitRequest(
    request_id=str(req.get("request_id") or "").strip(),
    consumer_id=str(req.get("consumer_id") or "").strip(),
    priority=str(req.get("priority") or "background").strip() or "background",
    audio=ASRAudioFile(
      path=audio_path,
      format=str(audio.get("format") or "wav").strip() or "wav",
      duration_ms=audio.get("duration_ms"),
      sample_rate_hz=audio.get("sample_rate_hz"),
      channels=audio.get("channels"),
    ),
    routing=ASRRequestRouting(
      fairness_key=str(routing.get("fairness_key") or "").strip(),
      slot_affinity=routing.get("slot_affinity"),
    ),
    options=ASRRequestOptions(
      language=options.get("language"),
      initial_prompt=options.get("initial_prompt"),
      align_enabled=options.get("align_enabled"),
      diarize_enabled=options.get("diarize_enabled"),
      speaker_mode=options.get("speaker_mode"),
      min_speakers=options.get("min_speakers"),
      max_speakers=options.get("max_speakers"),
      beam_size=options.get("beam_size"),
      chunk_size=options.get("chunk_size"),
      asr_backend=options.get("asr_backend"),
    ),
    outputs=ASROutputSelection(
      text=bool(outputs.get("text", False)),
      segments=bool(outputs.get("segments", False)),
      srt=bool(outputs.get("srt", False)),
      srt_inline=bool(outputs.get("srt_inline", False)),
    ),
  )


def _status_dict_with_request_id(status: Any, fallback_request_id: str) -> dict[str, Any]:
  row = status.to_dict() if status is not None else {}
  if fallback_request_id and not str(row.get("request_id") or "").strip():
    row["request_id"] = str(fallback_request_id)
  return row


def _http_status_for_submit_lifecycle(row: dict[str, Any]) -> int:
  lifecycle_state = str(row.get("state") or "").strip().lower()
  if lifecycle_state in {"completed", "failed", "cancelled"}:
    return 200
  return 202


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

  request_id = str(req.get("request_id") or "").strip()
  try:
    submit_request = _submit_request_from_payload(request_payload=req, audio_path=audio_path)
  except Exception as e:
    return {
      "ok": False,
      "request_id": str(request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code="ASR_REMOTE_REQUEST_INVALID",
        message=f"Invalid ASR request payload: {type(e).__name__}: {e}",
        retryable=False,
        details={"exc_type": type(e).__name__},
      ),
      "submit_lifecycle": {},
      "http_status": 0,
    }

  try:
    status = _client().submit_audio(submit_request)
  except ASRPoolRequestRejected as e:
    submit_lifecycle = _status_dict_with_request_id(e.request_status, request_id)
    return {
      "ok": False,
      "request_id": str(submit_lifecycle.get("request_id") or request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code=e.code,
        message=e.message,
        retryable=bool(e.retryable if e.retryable is not None else True),
        details=dict(e.details or {}),
      ),
      "submit_lifecycle": submit_lifecycle,
      "http_status": int(e.details.get("http_status") or 0),
    }
  except ASRPoolError as e:
    return {
      "ok": False,
      "request_id": str(request_id),
      "prepared_request": req,
      "error_response": _build_error_response(
        request=req,
        code=e.code,
        message=e.message,
        retryable=bool(e.retryable if e.retryable is not None else False),
        details=dict(e.details or {}),
      ),
      "submit_lifecycle": {},
      "http_status": int(e.details.get("http_status") or 0),
    }

  submit_lifecycle = _status_dict_with_request_id(status, request_id)
  rid = str(submit_lifecycle.get("request_id") or request_id).strip()
  return {
    "ok": True,
    "request_id": rid,
    "prepared_request": req,
    "submit_lifecycle": submit_lifecycle,
    "http_status": _http_status_for_submit_lifecycle(submit_lifecycle),
  }


def fetch_remote_pending_status(
  *,
  consumer_id: str,
  request_ids: list[str],
  limit: int = 200,
) -> list[dict[str, Any]]:
  try:
    rows = _client().get_request_statuses(
      consumer_id=consumer_id,
      request_ids=request_ids,
      limit=limit,
    )
  except ASRPoolError:
    return []
  return [row.to_dict() for row in rows]


def download_remote_request_srt_to_path(
  *,
  request_id: str,
  dst_path: Path,
  allow_empty: bool = False,
) -> Path:
  try:
    return _client().download_srt(
      request_id=request_id,
      dst_path=dst_path,
      allow_empty=allow_empty,
    )
  except ASRPoolError as e:
    raise RuntimeError(f"{e.code}: {e.message}") from e


def _completion_event_payload(event: ASRCompletionEvent) -> dict[str, Any]:
  payload = event.status.to_dict()
  payload["seq"] = int(event.seq)
  payload["ts_utc"] = str(event.ts_utc)
  return payload


def stream_remote_completions_forever(
  *,
  consumer_id: str,
  start_since_seq: int,
  stop_event: threading.Event,
  on_event: Callable[[str, dict[str, Any]], None],
) -> None:
  try:
    for event in _client().iter_completions(
      consumer_id=consumer_id,
      since_seq=start_since_seq,
      stop_event=stop_event,
    ):
      if isinstance(event, ASRCompletionEvent):
        on_event("completion", _completion_event_payload(event))
      elif isinstance(event, ASRCompletionFeedReset):
        on_event(
          "feed_reset",
          {
            "old_feed_id": str(event.old_feed_id),
            "new_feed_id": str(event.new_feed_id),
          },
        )
  except ASRPoolError as e:
    payload = {
      "code": str(e.code),
      "message": str(e.message),
      "retryable": (bool(e.retryable) if e.retryable is not None else None),
      "since_seq": int(max(0, int(start_since_seq))),
    }
    details = dict(e.details or {})
    if details:
      payload["details"] = details
    on_event("stream_error", payload)

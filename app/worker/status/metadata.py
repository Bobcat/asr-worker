from __future__ import annotations

from typing import Any

from app.worker.contract import normalize_speaker_mode


def _timing_value(timings: dict[str, Any], key: str) -> float | None:
  if key not in timings:
    return None
  try:
    return max(0.0, float(timings[key]))
  except Exception:
    return None


def _runtime_int(runtime_meta: dict[str, Any], key: str) -> int | None:
  if key not in runtime_meta or runtime_meta.get(key) is None:
    return None
  try:
    return max(0, int(runtime_meta.get(key)))
  except Exception:
    return None


def _runtime_float(runtime_meta: dict[str, Any], key: str) -> float | None:
  if key not in runtime_meta or runtime_meta.get(key) is None:
    return None
  try:
    return max(0.0, float(runtime_meta.get(key)))
  except Exception:
    return None


def _extended_runtime_meta_patch(
  *,
  terminal_state: str,
  response: dict[str, Any],
  request_cfg: dict[str, Any],
) -> dict[str, Any]:
  timings = dict(response.get("timings") or {})
  runtime_meta = dict(response.get("runtime") or {})
  effective_options = dict(response.get("effective_options") or {})
  speaker_mode = normalize_speaker_mode(request_cfg.get("speaker_mode", "none"))
  align_enabled = bool(request_cfg.get("align_enabled", True))
  resolved_initial_prompt = str(effective_options.get("initial_prompt") or "")
  resolved_initial_prompt_words = len([tok for tok in resolved_initial_prompt.split() if tok])
  return {
    "asr_state": str(terminal_state or ""),
    "asr_runtime": runtime_meta,
    "asr_timings": timings,
    "align_enabled": bool(effective_options.get("align_enabled", align_enabled)),
    "asr_initial_prompt_chars": len(resolved_initial_prompt),
    "asr_initial_prompt_words": max(0, resolved_initial_prompt_words),
    "asr_runner_kind": str(runtime_meta.get("runner_kind") or ""),
    "asr_runner_reused": bool(runtime_meta.get("runner_reused", False)),
    "asr_backend": str(runtime_meta.get("backend") or ""),
    "asr_device": str(runtime_meta.get("device") or ""),
    "asr_model": str(runtime_meta.get("model") or ""),
    "asr_timing_whisperx_total_s": _timing_value(timings, "total_s"),
    "asr_timing_whisperx_prepare_s": _timing_value(timings, "prepare_s"),
    "asr_timing_whisperx_transcribe_call_s": _timing_value(timings, "transcribe_call_s"),
    "asr_timing_whisperx_transcribe_s": _timing_value(timings, "transcribe_s"),
    "asr_timing_whisperx_align_s": _timing_value(timings, "align_s"),
    "asr_timing_whisperx_diarize_s": _timing_value(timings, "diarize_s"),
    "asr_timing_whisperx_finalize_s": _timing_value(timings, "finalize_s"),
    "asr_remote_submit_attempts": _runtime_int(runtime_meta, "remote_submit_attempts"),
    "asr_remote_status_attempts_total": _runtime_int(runtime_meta, "remote_status_attempts_total"),
    "asr_remote_status_http_calls": _runtime_int(runtime_meta, "remote_status_http_calls"),
    "asr_remote_cancel_attempts": _runtime_int(runtime_meta, "remote_cancel_attempts"),
    "asr_blob_fetch_ms": _runtime_float(runtime_meta, "blob_fetch_ms"),
    "speaker_mode": speaker_mode,
  }

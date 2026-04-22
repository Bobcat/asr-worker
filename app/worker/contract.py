from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.remote.asr_bridge import ASR_SCHEMA_VERSION


def normalize_speaker_mode(mode: Any) -> str:
  raw = str(mode or "auto").strip().lower()
  if raw in {"none", "off", "disabled", "no_speaker", "nospeaker", "no-speaker"}:
    return "none"
  if raw == "fixed":
    return "fixed"
  return "auto"


def _resolve_job_relpath(*, job: object, relpath: Any, field_name: str) -> Path:
  raw = str(relpath or "").strip()
  if not raw:
    raise RuntimeError(f"Missing {field_name}")
  job_dir = Path(getattr(job, "dir")).resolve()
  path = (job_dir / raw).resolve()
  try:
    path.relative_to(job_dir)
  except Exception as e:
    raise RuntimeError(f"Invalid {field_name}: {raw}") from e
  return path


def _read_worker_job_contract(job_path: Path) -> dict[str, Any]:
  raw = json.loads(job_path.read_text(encoding="utf-8"))
  if not isinstance(raw, dict):
    raise RuntimeError(f"Invalid worker job contract: {job_path}")
  return dict(raw)


def _worker_contract_sections(job_cfg: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
  return (
    dict(job_cfg.get("input") or {}),
    dict(job_cfg.get("request") or {}),
    dict(job_cfg.get("outputs") or {}),
    dict(job_cfg.get("worker_features") or {}),
  )


def _build_remote_pool_request_from_contract(*, job: object, job_cfg: dict[str, Any]) -> tuple[dict[str, Any], Path]:
  input_cfg, request_cfg, _outputs_cfg, _features_cfg = _worker_contract_sections(job_cfg)
  input_path = _resolve_job_relpath(job=job, relpath=input_cfg.get("audio_relpath"), field_name="input.audio_relpath")

  try:
    duration_ms = int(max(1, int(input_cfg.get("duration_ms") or 0)))
  except Exception as e:
    raise RuntimeError("Missing or invalid input.duration_ms") from e

  request_id = str(request_cfg.get("request_id") or getattr(job, "job_id", "")).strip()
  if not request_id:
    raise RuntimeError("Missing request.request_id")

  speaker_mode = normalize_speaker_mode(request_cfg.get("speaker_mode", "none"))

  audio: dict[str, Any] = {
    "local_path": str(input_path),
    "duration_ms": duration_ms,
  }
  audio_format = str(input_cfg.get("format") or input_path.suffix.lstrip(".") or "").strip()
  if audio_format:
    audio["format"] = audio_format
  sample_rate_hz = input_cfg.get("sample_rate_hz")
  if sample_rate_hz is not None:
    try:
      audio["sample_rate_hz"] = max(1, int(sample_rate_hz))
    except Exception:
      pass
  channels = input_cfg.get("channels")
  if channels is not None:
    try:
      audio["channels"] = max(1, int(channels))
    except Exception:
      pass

  options: dict[str, Any] = {
    "align_enabled": bool(request_cfg.get("align_enabled", True)),
    "diarize_enabled": bool(request_cfg.get("diarize_enabled", speaker_mode != "none")) and speaker_mode != "none",
    "speaker_mode": speaker_mode,
  }
  language = str(request_cfg.get("language") or "").strip()
  if language:
    options["language"] = language
  initial_prompt = str(request_cfg.get("initial_prompt") or "").strip()
  if initial_prompt:
    options["initial_prompt"] = initial_prompt
  beam_size = request_cfg.get("beam_size")
  if beam_size is not None:
    try:
      options["beam_size"] = max(1, int(beam_size))
    except Exception:
      pass
  chunk_size = request_cfg.get("chunk_size")
  if chunk_size is not None:
    try:
      options["chunk_size"] = max(1, int(chunk_size))
    except Exception:
      pass
  asr_backend = str(request_cfg.get("asr_backend") or "").strip().lower()
  if asr_backend:
    options["asr_backend"] = asr_backend
  if speaker_mode == "fixed":
    min_speakers = request_cfg.get("min_speakers")
    max_speakers = request_cfg.get("max_speakers")
    if min_speakers is not None:
      try:
        options["min_speakers"] = max(1, int(min_speakers))
      except Exception:
        pass
    if max_speakers is not None:
      try:
        options["max_speakers"] = max(1, int(max_speakers))
      except Exception:
        pass

  request_payload = {
    "schema_version": ASR_SCHEMA_VERSION,
    "request_id": request_id,
    "priority": str(request_cfg.get("priority") or "background").strip() or "background",
    "routing": dict(request_cfg.get("routing") or {}),
    "audio": audio,
    "options": options,
    "outputs": {
      "text": False,
      "segments": False,
      "srt": True,
      "srt_inline": False,
    },
  }
  return request_payload, input_path

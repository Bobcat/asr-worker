from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

from app.worker.contract import normalize_speaker_mode


AUDIO_PHASES = {"whisperx_transcribe", "whisperx_align", "whisperx_diarize"}

DEFAULTS_SECONDS = {
  "whisperx_prepare": 2.0,
  "whisperx_transcribe": 12.0,
  "whisperx_align": 8.0,
  "whisperx_diarize": 12.0,
}


@dataclass(frozen=True)
class ProgressPrediction:
  phase_expected_s: dict[str, float]
  total_expected_s: float
  confidence: float
  hints: list[str]
  sample_count: int


def _safe_float(v: Any) -> float | None:
  try:
    return float(v)
  except Exception:
    return None


def _diarization_enabled(*, speaker_mode: Any) -> bool:
  return normalize_speaker_mode(speaker_mode) != "none"


def phase_order_for_job(*, speaker_mode: str) -> list[str]:
  order = [
    "whisperx_prepare",
    "whisperx_transcribe",
    "whisperx_align",
  ]
  if _diarization_enabled(speaker_mode=speaker_mode):
    order.append("whisperx_diarize")
  return order


def _iter_done_records(runs_path: Path) -> list[dict[str, Any]]:
  if not runs_path.exists():
    return []
  rows: list[dict[str, Any]] = []
  with runs_path.open("r", encoding="utf-8") as f:
    for line in f:
      s = line.strip()
      if not s:
        continue
      try:
        obj = json.loads(s)
      except Exception:
        continue
      if str(obj.get("outcome", "")) != "done":
        continue
      if not isinstance(obj.get("phase_seconds"), dict):
        continue
      rows.append(obj)
  return rows


def _record_audio_duration_s(record: dict[str, Any]) -> int:
  try:
    value = int(record.get("audio_duration_s", 0))
  except Exception:
    value = 0
  if value > 0:
    return value
  try:
    value = int(record.get("snippet_seconds", 0))
  except Exception:
    value = 0
  return value if value > 0 else 0


def build_prediction(
  *,
  runs_path: Path,
  hardware_key: str,
  speaker_mode: str,
  audio_duration_s: int,
) -> ProgressPrediction:
  phase_order = phase_order_for_job(speaker_mode=speaker_mode)
  audio_duration_s = max(1, int(audio_duration_s))
  norm_mode = normalize_speaker_mode(speaker_mode)

  all_done = _iter_done_records(runs_path)
  candidates = []
  for r in all_done:
    if str(r.get("hardware_key", "")) != str(hardware_key):
      continue
    if normalize_speaker_mode(r.get("speaker_mode", "auto")) != norm_mode:
      continue
    candidates.append(r)

  hints: list[str] = []
  n = len(candidates)
  if n == 0:
    hints.append("cold_start")
  if n < 5:
    hints.append("low_sample_n")

  expected: dict[str, float] = {}
  used_defaults = False

  if n > 0:
    durs = [_record_audio_duration_s(r) for r in candidates]
    durs = [d for d in durs if d > 0]
    if durs and (audio_duration_s < min(durs) or audio_duration_s > max(durs)):
      hints.append("extrapolated_audio_duration")

  for phase in phase_order:
    if phase in AUDIO_PHASES:
      rates: list[float] = []
      for r in candidates:
        sec = _safe_float((r.get("phase_seconds") or {}).get(phase))
        dur = _safe_float(_record_audio_duration_s(r))
        if sec is None or dur is None or dur <= 0:
          continue
        if sec < 0:
          continue
        rates.append(sec / dur)
      if rates:
        exp = median(rates) * audio_duration_s
      else:
        exp = DEFAULTS_SECONDS.get(phase, 1.0) * (audio_duration_s / 900.0)
        used_defaults = True
      expected[phase] = max(0.0, float(exp))
      continue

    vals: list[float] = []
    for r in candidates:
      sec = _safe_float((r.get("phase_seconds") or {}).get(phase))
      if sec is None or sec < 0:
        continue
      vals.append(sec)
    if vals:
      exp = median(vals)
    else:
      exp = DEFAULTS_SECONDS.get(phase, 1.0)
      used_defaults = True
    expected[phase] = max(0.0, float(exp))

  if used_defaults:
    hints.append("phase_defaults")

  total = sum(expected.get(p, 0.0) for p in phase_order)
  confidence = min(0.95, max(0.05, n / 20.0))

  ordered_hints: list[str] = []
  for h in ("cold_start", "low_sample_n", "extrapolated_audio_duration", "phase_defaults"):
    if h in hints and h not in ordered_hints:
      ordered_hints.append(h)

  return ProgressPrediction(
    phase_expected_s={k: round(v, 6) for k, v in expected.items()},
    total_expected_s=round(max(1.0, float(total)), 6),
    confidence=round(float(confidence), 6),
    hints=ordered_hints,
    sample_count=n,
  )

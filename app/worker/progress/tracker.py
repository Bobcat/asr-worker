from __future__ import annotations

import time

from app.worker.status.io import _write_status


def _format_timings_text(rows: list[tuple[str, float]], *, total_s: float | None = None) -> str:
  cumulative = 0.0
  done_rows: list[tuple[str, float]] = []
  for name, sec in rows:
    safe = max(0.0, float(sec))
    cumulative += safe
    done_rows.append((name, safe))

  shown_total = max(0.0, float(total_s)) if total_s is not None else cumulative
  parts: list[str] = []
  for name, sec in done_rows:
    parts.append(f"{name}={sec:.2f}s")
  parts.append(f"total={shown_total:.2f}s")
  return " | ".join(parts)


def _build_progress_tracker(
  *,
  status_path,
  phase_order: list[str],
  phase_expected_s: dict[str, float],
  eta_confidence: float,
  eta_hints: list[str],
  completed_actual_seed: dict[str, float] | None = None,
):
  completed_actual: dict[str, float] = {
    str(k): max(0.0, float(v))
    for k, v in dict(completed_actual_seed or {}).items()
  }
  current_phase_key: str | None = None
  current_phase_started_t = 0.0
  current_status_phase = ""
  current_base_message = "Running..."
  last_progress = 0.0
  last_write_t = 0.0
  phase_overrun_active = False
  phase_expected_runtime: dict[str, float] = {
    p: max(0.1, float(phase_expected_s.get(p, 0.0)))
    for p in phase_order
  }
  total_expected_all = max(0.1, sum(max(0.0, float(phase_expected_runtime.get(p, 0.0))) for p in phase_order))
  hints = eta_hints
  cleaned: list[str] = []
  for raw in hints:
    h = str(raw).strip()
    if h and h not in cleaned:
      cleaned.append(h)
  hints[:] = cleaned

  def _after_current(ph: str | None) -> list[str]:
    if ph is None:
      return [p for p in phase_order if p not in completed_actual]
    if ph not in phase_order:
      return []
    i = phase_order.index(ph)
    return [p for p in phase_order[i + 1:] if p not in completed_actual]

  def _sum_completed() -> float:
    return sum(max(0.0, float(v)) for v in completed_actual.values())

  def _sum_completed_expected() -> float:
    total = 0.0
    for p in phase_order:
      if p in completed_actual:
        total += max(0.0, float(phase_expected_runtime.get(p, 0.0)))
    return total

  def _maybe_expand_phase_budget(phase_key: str | None, *, elapsed_s: float) -> None:
    nonlocal total_expected_all
    if not phase_key:
      return
    cur = max(0.1, float(phase_expected_runtime.get(phase_key, 0.1)))
    safe_elapsed = max(0.0, float(elapsed_s))
    if safe_elapsed <= (cur * 1.05):
      return
    target = max(cur, safe_elapsed * 1.10)
    cap = max(cur * 8.0, 300.0)
    nxt = min(cap, target)
    if nxt <= cur:
      return
    phase_expected_runtime[phase_key] = float(nxt)
    total_expected_all = max(
      0.1,
      sum(max(0.0, float(phase_expected_runtime.get(p, 0.0))) for p in phase_order),
    )

  def _set_phase_overrun_hint(active: bool) -> None:
    nonlocal phase_overrun_active
    if active:
      if "phase_overrun" not in hints:
        hints.append("phase_overrun")
      phase_overrun_active = True
      return
    if "phase_overrun" in hints:
      hints[:] = [h for h in hints if h != "phase_overrun"]
    phase_overrun_active = False

  def _write_eta(*, force: bool = False) -> None:
    nonlocal last_progress, last_write_t
    now = time.monotonic()
    if not force and (now - last_write_t) < 1.0:
      return

    done_actual = _sum_completed()
    elapsed_current = max(0.0, now - current_phase_started_t) if current_phase_key else 0.0
    expected_current_base = max(0.1, float(phase_expected_runtime.get(current_phase_key or "", 0.0)))
    if current_phase_key:
      _maybe_expand_phase_budget(current_phase_key, elapsed_s=elapsed_current)
      expected_current_base = max(0.1, float(phase_expected_runtime.get(current_phase_key or "", expected_current_base)))
    expected_current = expected_current_base
    remaining_keys = _after_current(current_phase_key)
    remaining_after = sum(max(0.0, float(phase_expected_runtime.get(p, 0.0))) for p in remaining_keys)

    if current_phase_key == "whisperx_transcribe":
      proxied_keys = {"whisperx_align", "whisperx_diarize", "whisperx_finalize"}
      proxy_extra = sum(max(0.0, float(phase_expected_runtime.get(p, 0.0))) for p in remaining_keys if p in proxied_keys)
      if proxy_extra > 0.0:
        expected_current = max(expected_current, expected_current + proxy_extra)
        remaining_after = sum(max(0.0, float(phase_expected_runtime.get(p, 0.0))) for p in remaining_keys if p not in proxied_keys)
    overrun_factor = 1.1
    if current_phase_key == "whisperx_transcribe":
      overrun_factor = 3.0
    overrun_now = bool(
      current_phase_key
      and expected_current > 0
      and elapsed_current > (expected_current * overrun_factor)
    )
    if current_phase_key == "whisperx_transcribe":
      overrun_now = False
    _set_phase_overrun_hint(overrun_now)

    current_projected_total = max(expected_current, elapsed_current)
    if current_phase_key:
      est_total = done_actual + current_projected_total + remaining_after
      est_elapsed = done_actual + elapsed_current
      est_remaining = max(0.0, (current_projected_total - elapsed_current) + remaining_after)
    else:
      est_total = max(0.1, done_actual + remaining_after)
      est_elapsed = done_actual
      est_remaining = max(0.0, est_total - est_elapsed)

    if current_phase_key and current_phase_key != "whisperx_transcribe" and elapsed_current > (expected_current * 1.05):
      overrun_tail = min(120.0, max(3.0, elapsed_current * 0.25))
      est_remaining = max(est_remaining, overrun_tail + remaining_after)
      est_total = max(est_total, est_elapsed + est_remaining)

    if current_phase_key is not None and est_remaining < 1.0:
      est_remaining = 1.0
      est_total = max(est_total, est_elapsed + est_remaining)

    completed_expected = _sum_completed_expected()
    if current_phase_key:
      progress_phase_expected = expected_current_base
      phase_frac = min(0.995, max(0.0, elapsed_current / progress_phase_expected))
      raw_progress = (completed_expected + (phase_frac * progress_phase_expected)) / total_expected_all
    else:
      raw_progress = completed_expected / total_expected_all

    progress_cap = 0.99
    if current_phase_key == "whisperx_transcribe":
      progress_cap = 0.90
    progress = min(progress_cap, max(last_progress, float(raw_progress)))
    last_progress = progress
    last_write_t = now

    _write_status(
      status_path,
      phase=current_status_phase or None,
      asr_progress=progress,
      asr_phase=current_phase_key or current_status_phase or None,
      message=current_base_message,
      progress_mode="predictive_v1",
      asr_eta_total_s=round(est_total, 3),
      asr_eta_remaining_s=round(est_remaining, 3),
      asr_elapsed_s=round(est_elapsed, 3),
      asr_eta_confidence=round(float(eta_confidence), 3),
      asr_eta_hints=list(hints),
    )

  def start_phase(phase_key: str, base_message: str, status_phase: str) -> None:
    nonlocal current_phase_key, current_phase_started_t, current_status_phase, current_base_message
    current_phase_key = phase_key
    current_phase_started_t = time.monotonic()
    current_status_phase = status_phase
    current_base_message = base_message
    _write_eta(force=True)

  def finish_phase(phase_key: str, actual_elapsed_s: float) -> None:
    nonlocal current_phase_key, current_phase_started_t
    safe = max(0.0, float(actual_elapsed_s))
    completed_actual[phase_key] = completed_actual.get(phase_key, 0.0) + safe
    if current_phase_key == phase_key:
      current_phase_key = None
      current_phase_started_t = 0.0
      _set_phase_overrun_hint(False)
    _write_eta(force=True)

  def heartbeat() -> None:
    _write_eta(force=False)

  def set_base_message(base_message: str, *, status_phase: str | None = None) -> None:
    nonlocal current_base_message, current_status_phase
    current_base_message = base_message
    if status_phase:
      current_status_phase = status_phase
    _write_eta(force=True)

  return start_phase, finish_phase, heartbeat, set_base_message

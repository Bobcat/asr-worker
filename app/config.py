"""Local configuration loader for asr-worker.

Sources (in order):
1. config/settings.json (tracked defaults)
2. config/local.json (optional local overrides)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")


def _repo_root() -> Path:
  return Path(__file__).resolve().parent.parent


def _load_json_file(path: Path) -> dict[str, Any]:
  if not path.exists():
    return {}
  try:
    with path.open("r", encoding="utf-8") as f:
      data = json.load(f)
    return data if isinstance(data, dict) else {}
  except Exception:
    return {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
  result = dict(base)
  for key, value in override.items():
    if str(key).startswith("_"):
      continue
    if isinstance(value, dict) and isinstance(result.get(key), dict):
      result[key] = _deep_merge(dict(result[key]), value)
    else:
      result[key] = value
  return result


def load_config() -> dict[str, Any]:
  cfg_dir = _repo_root() / "config"
  settings = _load_json_file(cfg_dir / "settings.json")
  local = _load_json_file(cfg_dir / "local.json")
  return _deep_merge(settings, local)


_CONFIG: dict[str, Any] | None = None


def get_config() -> dict[str, Any]:
  global _CONFIG
  if _CONFIG is None:
    _CONFIG = load_config()
  return _CONFIG


def reload_config() -> dict[str, Any]:
  global _CONFIG
  _CONFIG = load_config()
  return _CONFIG


def get_setting(path: str, default: T | None = None) -> T | Any:
  cfg: Any = get_config()
  for key in str(path or "").split("."):
    if not key:
      continue
    if not isinstance(cfg, dict) or key not in cfg:
      return default
    cfg = cfg[key]
  return cfg


def get_str(path: str, default: str = "") -> str:
  val = get_setting(path, default)
  return str(val) if val is not None else default


def get_int(path: str, default: int = 0, *, min_value: int | None = None) -> int:
  try:
    out = int(get_setting(path, default))
  except Exception:
    out = int(default)
  if min_value is not None:
    out = max(int(min_value), out)
  return out


def get_float(path: str, default: float = 0.0, *, min_value: float | None = None) -> float:
  try:
    out = float(get_setting(path, default))
  except Exception:
    out = float(default)
  if min_value is not None:
    out = max(float(min_value), out)
  return out

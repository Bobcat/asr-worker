from __future__ import annotations

import json
import os
import secrets
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def new_job_id() -> str:
    return f"job_{_utc_stamp()}_{secrets.token_hex(4)}"


@dataclass(frozen=True)
class QueueRoot:
    name: str
    base: Path
    inbox: Path
    running: Path
    done: Path
    error: Path


@dataclass(frozen=True)
class JobPaths:
    queue_root: QueueRoot
    job_id: str
    dir: Path
    status_path: Path
    job_path: Path
    log_path: Path


def _queue_root_from_base(base: Path, *, name: str = "") -> QueueRoot:
    resolved = Path(base).resolve()
    return QueueRoot(
        name=str(name or resolved.name),
        base=resolved,
        inbox=resolved / "inbox",
        running=resolved / "running",
        done=resolved / "done",
        error=resolved / "error",
    )


def _job_paths_for_dir(*, job_dir: Path, queue_root: QueueRoot) -> JobPaths:
    job_dir = job_dir.resolve()
    return JobPaths(
        queue_root=queue_root,
        job_id=str(job_dir.name),
        dir=job_dir,
        status_path=job_dir / "status.json",
        job_path=job_dir / "job.json",
        log_path=job_dir / "worker.log",
    )


def job_paths_from_dir(job_dir: Path, *, queue_root: QueueRoot | None = None) -> JobPaths:
    job_dir = Path(job_dir).resolve()
    root = queue_root or _queue_root_from_base(job_dir.parents[1])
    return _job_paths_for_dir(job_dir=job_dir, queue_root=root)


def _write_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def init_job_in_inbox(
    *,
    queue_root: QueueRoot,
    job_json: dict[str, Any],
    status_json: dict[str, Any],
    input_src_path: str | Path | None = None,
    input_dst_relpath: str | Path | None = None,
    move_upload_src: bool = True,
) -> JobPaths:
    queue_root.inbox.mkdir(parents=True, exist_ok=True)

    raw_job_id = str(job_json.get("job_id") or status_json.get("job_id") or "").strip()
    job_id = raw_job_id or new_job_id()
    final_dir = queue_root.inbox / job_id
    tmp_dir = queue_root.inbox / f".tmp_{job_id}"

    if tmp_dir.exists():
        raise RuntimeError(f"Temp dir already exists: {tmp_dir}")
    if final_dir.exists():
        raise RuntimeError(f"Job dir already exists: {final_dir}")

    tmp_dir.mkdir(parents=True, exist_ok=True)

    status_path = tmp_dir / "status.json"
    job_path = tmp_dir / "job.json"
    log_path = tmp_dir / "worker.log"

    status = dict(status_json)
    status["job_id"] = job_id
    job = dict(job_json)
    job["job_id"] = job_id

    _write_json_atomic(status_path, status)
    _write_json_atomic(job_path, job)
    log_path.write_text("", encoding="utf-8")

    if input_src_path is not None:
        src = Path(str(input_src_path)).resolve()
        if not src.exists():
            raise FileNotFoundError(f"Upload source missing: {src}")
        rel_raw = str(input_dst_relpath or "").strip()
        if not rel_raw:
            raise RuntimeError("Missing input_dst_relpath for queued job source")
        dst = (tmp_dir / rel_raw).resolve()
        try:
            dst.relative_to(tmp_dir)
        except Exception as e:
            raise RuntimeError(f"Invalid input_dst_relpath: {rel_raw}") from e
        dst.parent.mkdir(parents=True, exist_ok=True)
        if bool(move_upload_src):
            os.replace(src, dst)
        else:
            shutil.copy2(src, dst)

    os.replace(tmp_dir, final_dir)
    return _job_paths_for_dir(job_dir=final_dir, queue_root=queue_root)


def claim_next_job(*, queue_root: QueueRoot) -> JobPaths | None:
    queue_root.running.mkdir(parents=True, exist_ok=True)

    candidates = sorted(
        p for p in queue_root.inbox.iterdir()
        if p.is_dir() and not p.name.startswith(".tmp_")
    ) if queue_root.inbox.exists() else []
    if not candidates:
        return None

    for job_dir in candidates:
        target = queue_root.running / job_dir.name
        try:
            os.replace(job_dir, target)
        except FileNotFoundError:
            continue
        except OSError:
            continue
        return _job_paths_for_dir(job_dir=target, queue_root=queue_root)

    return None


def finish_job(job: JobPaths, *, ok: bool) -> Path:
    dst_base = job.queue_root.done if ok else job.queue_root.error
    dst_base.mkdir(parents=True, exist_ok=True)
    dst = dst_base / job.dir.name
    os.replace(job.dir, dst)
    return dst


def move_job_to_queue_inbox(job: JobPaths, *, dst_queue_root: QueueRoot) -> JobPaths:
    dst_queue_root.inbox.mkdir(parents=True, exist_ok=True)
    dst = dst_queue_root.inbox / job.dir.name
    os.replace(job.dir, dst)
    return _job_paths_for_dir(job_dir=dst, queue_root=dst_queue_root)


def nudge_inbox(queue_root: QueueRoot) -> None:
    queue_root.inbox.mkdir(parents=True, exist_ok=True)
    sentinel = queue_root.inbox / ".wake"
    sentinel.write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")


def find_job_dir(job_id: str, *, queue_roots: list[QueueRoot] | tuple[QueueRoot, ...]) -> Path | None:
    job_id = str(job_id or "").strip()
    if not job_id:
        return None
    for queue_root in queue_roots:
        for state_dir in (queue_root.inbox, queue_root.running, queue_root.done, queue_root.error):
            job_dir = state_dir / job_id
            if job_dir.exists():
                return job_dir
    return None

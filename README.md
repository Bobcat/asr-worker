# asr-worker

`asr-worker` is a file-backed ASR worker daemon on top of `asr-pool`.
It claims jobs from a queue directory, submits audio requests to the pool,
keeps job status up to date, estimates progress and ETA while work is
running, and writes final artifacts back to the job folder.

## What It Does

- watches one queue root directory on disk
- picks up job directories from `inbox` and moves them to `running`
- reads `job.json` and submits the job's audio file to `asr-pool`
- keeps `status.json` up to date while the job is waiting or running
- estimates progress and ETA from earlier completed runs
- uses streaming completion events to detect terminal completion quickly
- can download SRT artifacts into the job directory
- moves finished jobs to `done` or `error`
- exposes `/ops` and `/ops/metrics` for operators

## Code Map

If you are new to the repo, these are the key files:

### Key Files

| File | Why Open It |
|---|---|
| `app/main.py` | Thin composition root; resolves env/config, builds the runtime config, and starts the worker loop. |
| `app/worker/coordination/loop.py` | Main coordinator loop, inbox watch events, completion stream handling, scheduling, and ops snapshots. |
| `app/worker/submit.py` | Claim/refill-from-inbox, submit preparation, submit worker threading, and submit result handling. |
| `app/worker/pending.py` | Pending-status polling and interim status/progress updates while jobs are running. |
| `app/worker/runtime.py` | Shared worker runtime state and helper logic used by submit and pending flows. |
| `app/worker/finalization.py` | Terminal success/error finalization, final status patches, SRT download, and queue moves to `done` or `error`. |
| `app/remote/asr_bridge.py` | Boundary to `asr-pool`; makes explicit that ASR itself happens outside this repo and the worker bridges to it for submit, status, completions, and SRT download. |
| `app/queue/fs.py` | Queue/job directory model plus atomic filesystem moves between `inbox`, `running`, `done`, and `error`. |
| `app/worker/progress/predictor.py` and `app/worker/progress/tracker.py` | Predictive ETA/progress estimation and live status updates. |
| `app/worker/status/io.py` | `status.json` patch/write behavior and worker-facing message formatting. |
| `app/config.py` | Merged config loading from `config/settings.json` and optional `config/local.json`. |
| `app/worker/contract.py` | Worker job contract parsing, path resolution, speaker-mode normalization, and remote request construction. |

### Top-Level Layout

| Path | Role |
|---|---|
| `app/` | Application package and code entrypoints. |
| `app/worker/` | Worker package root for shared worker-domain modules. |
| `app/worker/coordination/` | Coordinator loop, event bus, inbox watch glue, and ops-window bookkeeping. |
| `app/worker/progress/` | Predictive progress and ETA logic. |
| `app/worker/status/` | Status file writes and runtime metadata patches. |
| `app/remote/` | ASR pool bridge and related remote-facing constants. |
| `app/queue/` | Filesystem queue primitives. |
| `app/ops/` | Operator HTTP endpoints. |
| `config/` | Tracked defaults plus local overrides. |
| `deploy/` | Systemd and environment examples. |

### Feature Traces

**Submit Path**

| Flow |
|---|
| `app.main`<br>`-> app.worker.coordination.loop.run_worker_loop()`<br>`-> app.worker.submit.refill_from_inbox()`<br>`-> app.worker.submit.submit_worker_loop()`<br>`-> app.worker.submit._prepare_worker_job_for_submit()` |

**Pending-Status Path**

| Flow |
|---|
| `app.main`<br>`-> app.worker.coordination.loop.run_worker_loop()`<br>`-> app.worker.pending.poll_pending_jobs()` |

**Completion / Finalization Path**

| Flow |
|---|
| `app.main`<br>`-> app.worker.coordination.loop.run_worker_loop()`<br>`-> app.worker.coordination.loop._handle_completion_event()`<br>`-> app.worker.finalization.finalize_job_terminal()` / `app.worker.finalization.finalize_job_error()` |

**Ops Path**

| Flow |
|---|
| `app.main`<br>`-> app.worker.coordination.loop.run_worker_loop()`<br>`-> app.worker.coordination.ops.build_ops_snapshot()`<br>`-> app.ops.server` |

## Runtime Model

`asr-worker` sits between a job queue on disk and `asr-pool`.
One worker process owns one queue root directory with these subdirectories:

- `inbox` for jobs waiting to be picked up
- `running` for jobs currently owned by the worker
- `done` for jobs that completed successfully
- `error` for jobs that failed validation or finished with an error

For each job directory, the worker does this:

1. reads `job.json` and resolves the input audio path
2. moves the job directory from `inbox` to `running`
3. submits the audio file to `asr-pool`
4. keeps reading current request status and completion events
5. updates `status.json`, progress, ETA, and optional artifacts such as SRT
6. moves the job directory to `done` or `error`

While a job is running, the worker updates `status.json` and can estimate
progress and ETA from earlier completed runs with similar settings.

## Worker Contract

Each job folder must contain a `job.json` file. A typical job looks like this:

```json
{
  "input": {
    "audio_relpath": "input/audio.wav",
    "duration_ms": 61234
  },
  "request": {
    "request_id": "job_123",
    "language": "nl",
    "speaker_mode": "auto",
    "priority": "background",
    "routing": {
      "slot_affinity": 0
    }
  },
  "outputs": {
    "srt_relpath": "artifacts/output.srt"
  },
  "worker_features": {
    "write_status_json": true,
    "track_pending_status": true,
    "predictive_progress": true,
    "write_timings_text": true,
    "include_runtime_meta": true,
    "download_srt": true
  }
}
```

Key fields:

- `input.audio_relpath`
  job-folder-relative path to the source audio file
- `input.duration_ms`
  required for progress and ETA estimates
- `request.*`
  ASR request data passed through to `asr-pool`, including language,
  priority, speaker mode, and optional routing
- `outputs.srt_relpath`
  required when `worker_features.download_srt=true`
- `worker_features.*`
  controls whether the worker writes `status.json`, downloads SRT,
  writes timing text, and includes extra runtime metadata

For v1 jobs, `worker_features.write_status_json` must be `true`.

## Progress Prediction

`asr-worker` can estimate progress and ETA while a job is still running.
This logic lives in the worker itself, where it can use job metadata and
earlier completed runs to produce better estimates over time.

When `worker_features.predictive_progress=true`:

- `input.duration_ms` is required
- the worker reads earlier completed runs from its progress database
- estimates improve over time as more similar jobs complete
- the current estimate is written into `status.json`

Typical `status.json` fields written by this feature:

- `progress`
- `eta_total_s`
- `eta_remaining_s`
- `elapsed_s`
- `eta_confidence`
- `timings_text` when enabled

## Status Files And Artifacts

`status.json` is the main file clients read while a job is active.
It contains the worker's current state, progress, messages, timing fields,
ASR request id, and final error or result metadata.

Example shape:

```json
{
  "state": "running",
  "phase": "whisperx_wait",
  "progress": 0.42,
  "message": "Aligning...",
  "started_at": "2026-03-22T10:15:04Z",
  "asr_request_id": "job_123",
  "progress_mode": "predictive_v1",
  "eta_total_s": 32.8,
  "eta_remaining_s": 8.3,
  "elapsed_s": 24.5,
  "eta_confidence": 0.82,
  "timings_text": "whisperx_prepare=0.18s | whisperx_transcribe=24.32s | total=24.50s",
  "srt_filename": "output.srt",
  "finished_at": null,
  "error": null
}
```

If `worker_features.download_srt=true`, the worker downloads the final SRT
artifact from `asr-pool` and writes it to `outputs.srt_relpath`.

If `worker_features.predictive_progress=true`, `status.json` also carries the
worker's current progress and ETA estimate. See `Progress Prediction`.

## Configuration

Configuration files are loaded in this order:

1. `config/settings.json`
2. `config/local.json` (optional, overrides)

Primary configuration areas:

- `asr_pool.*`
  pool base URL and token
- `asr_remote.*`
  HTTP timeout and retry settings for remote pool access
- `worker.*`
  queue base, consumer defaults, progress database, and worker identity
- `worker_events.*`
  completion heartbeat, inbox debounce, coordinator tick, and metrics log timing
- status refresh timing
  controls how often the worker reads current request status from `asr-pool`

Queue routing is usually set through environment variables in service files:

- `ASR_WORKER_QUEUE_BASE`
- `ASR_WORKER_QUEUE_NAME` (optional)
- `ASR_WORKER_MAX_OUTSTANDING` (optional override)
- `ASR_WORKER_CONSUMER_ID` (optional override)

## Observability

Each worker instance can expose:

- `GET /ops`
- `GET /ops/metrics`

Useful environment variables:

- `ASR_WORKER_OPS_ENABLED`
- `ASR_WORKER_OPS_HOST`
- `ASR_WORKER_OPS_PORT`
- `ASR_WORKER_OPS_WINDOW_S`
- `ASR_WORKER_OPS_RUNNING_STUCK_S`

If you run multiple workers on one machine, each instance should use its own
ops port.

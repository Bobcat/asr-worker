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

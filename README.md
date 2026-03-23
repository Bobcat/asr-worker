# asr-worker

`asr-worker` is a file-backed ASR convenience service on top of `asr-pool` (running locally or remotely).  
It claims jobs from a queue directory, submits to `asr-pool`, tracks progress, and writes terminal status/artifacts back to the job directory.

## What It Does

- claims jobs from one queue root (`inbox` -> `running` -> `done`/`error`)
- submits ASR requests to `asr-pool` (`/asr/v1/requests`)
- consumes completion events from the pool SSE stream
- optionally polls the `asr-pool` pending-status HTTP endpoint to update progress while a job is still running
- updates `status.json` and optionally downloads SRT artifacts

## Quick Start

```bash
sudo apt-get update
sudo apt-get install -y python3-venv

ASR_WORKER_DIR="$HOME/projects/asr-worker-dev"
mkdir -p "$ASR_WORKER_DIR"
git clone https://github.com/Bobcat/asr-worker.git "$ASR_WORKER_DIR"
cd "$ASR_WORKER_DIR"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip setuptools wheel
.venv/bin/pip install -r requirements.txt

# Minimal local queue root
mkdir -p "$ASR_WORKER_DIR/data/jobs/demo_worker"/{inbox,running,done,error}

ASR_WORKER_QUEUE_BASE="$ASR_WORKER_DIR/data/jobs/demo_worker" \
ASR_WORKER_CONSUMER_ID=worker-upload-dev@1 \
.venv/bin/python worker_daemon.py
```

By default the worker expects an `asr-pool` at `http://127.0.0.1:8090`. Override that in `config/local.json` when needed.

## Systemd Example

```bash
ASR_WORKER_DIR="$HOME/projects/asr-worker-dev"
mkdir -p ~/.config/systemd/user ~/.config/asr-worker "$ASR_WORKER_DIR/data/jobs/demo_worker"/{inbox,running,done,error}
cat > ~/.config/systemd/user/asr-worker.service <<EOF
[Unit]
Description=ASR Worker

[Service]
Type=simple
WorkingDirectory=$ASR_WORKER_DIR
Environment=ASR_WORKER_QUEUE_BASE=$ASR_WORKER_DIR/data/jobs/demo_worker
Environment=ASR_WORKER_CONSUMER_ID=worker@1
ExecStart=$ASR_WORKER_DIR/.venv/bin/python $ASR_WORKER_DIR/worker_daemon.py
Restart=always
RestartSec=2
EnvironmentFile=-%h/.config/asr-worker/asr-worker.env

[Install]
WantedBy=default.target
EOF
cp "$ASR_WORKER_DIR/deploy/env/asr-worker.dev.env.example" ~/.config/asr-worker/asr-worker.env
systemctl --user daemon-reload
systemctl --user enable --now asr-worker.service
```

## Configuration

Config load order:

1. `config/settings.json`
2. `config/local.json` (optional, gitignored)

Primary settings:

- `asr_pool.base_url`, `asr_pool.token`
- `asr_remote.*` (HTTP timeout/retries)
- `worker.*` (queue base, consumer defaults, max outstanding)
- `worker_events.*` (tick/debounce/heartbeat tuning)
- `polling_intervals.asr_remote_pending_status_poll_s`

Queue routing is normally driven by environment variables in unit files:

- `ASR_WORKER_QUEUE_BASE`
- `ASR_WORKER_QUEUE_NAME` (optional)
- `ASR_WORKER_MAX_OUTSTANDING` (optional override)
- `ASR_WORKER_CONSUMER_ID` (optional override)

Observability endpoint settings:

- `ASR_WORKER_OPS_ENABLED` (`1`/`0`, default `1`)
- `ASR_WORKER_OPS_HOST` (default `127.0.0.1`)
- `ASR_WORKER_OPS_PORT` (default `18110`)
- `ASR_WORKER_OPS_WINDOW_S` (default `300`)
- `ASR_WORKER_OPS_RUNNING_STUCK_S` (default `900`)

Each worker process exposes its own `/ops` and `/ops/metrics` on its configured host/port.  
If you run multiple workers on one machine, use a different `ASR_WORKER_OPS_PORT` per instance.

## Worker Contract Overview

Each worker job must provide a `job.json` with this shape:

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

## Job JSON Field Reference

The tables below describe the main fields most clients will use. They are not meant as a complete, exhaustive dump of every option.

### `input`

| Field | Required | Default / Rules | Effect |
|---|---|---|---|
| `audio_relpath` | Yes | Job-dir-relative path | Source audio file the worker submits to `asr-pool`. |
| `duration_ms` | Yes | Integer `>= 1` | Used for progress prediction/ETA only; does not determine transcript quality. |

### `request`

| Field | Required | Default / Rules | Effect |
|---|---|---|---|
| `request_id` | Conditionally | Falls back to worker (internally generated) `job_id` | Stable ASR request identifier. |
| `priority` | No | `background` | `asr-pool` scheduling priority. |
| `routing.slot_affinity` | No | Integer runner slot id, for example `0` | Requests a specific `asr-pool` runner slot. |
| `speaker_mode` | No | Normalized to `none` / `auto` / `fixed` | Controls diarization strategy and phase profile. |
| `align_enabled` | No | `true` | Enables/disables alignment in `asr-pool`. |
| `diarize_enabled` | No | Defaults from `speaker_mode`; forced `false` when `speaker_mode=none` | Enables/disables diarization in `asr-pool`. |
| `language` | No | Not sent when empty | Passed through to `asr-pool` options. |
| `initial_prompt` | No | Not sent when empty | Passed through to `asr-pool` and used as the initial transcription prompt. |
| `beam_size` | No | Integer `>= 1`; ignored when invalid | Passed through to `asr-pool` decoding options. |
| `min_speakers` | No | Only applied when `speaker_mode=fixed` | Passed through to `asr-pool` diarization options. |
| `max_speakers` | No | Only applied when `speaker_mode=fixed` | Passed through to `asr-pool` diarization options. |

### `outputs`

| Field | Required | Default / Rules | Effect |
|---|---|---|---|
| `srt_relpath` | Required when `download_srt=true` | Job-dir-relative path | Target path where worker stores downloaded SRT artifact. |

### `worker_features`

| Field | Required | Default / Rules | Effect |
|---|---|---|---|
| `write_status_json` | Yes | Must be `true` in file-backed v1 | Enables status projection to `status.json`. |
| `download_srt` | No | `false` | Download SRT artifact from `asr-pool` to `outputs.srt_relpath`. |
| `track_pending_status` | No | `false` | Poll interim pending status for queue/stage/message updates. |
| `predictive_progress` | No | `false`; requires `write_status_json=true` | Enables ETA/progress prediction fields in `status.json`. |
| `write_timings_text` | No | `false` | Writes human-readable `timings_text` in `status.json`. |
| `include_runtime_meta` | No | `false`; requires `write_status_json=true` | Writes extended `asr_*` runtime/timing metadata. |

Validation notes:

- `input.audio_relpath` and `input.duration_ms` are hard-required.
- If `download_srt=true`, then `outputs.srt_relpath` is required.

## ETA And Progress

When `worker_features.predictive_progress=true`:

- the worker uses `input.duration_ms` as the audio duration input for prediction
- the predictor reads historical completed runs from `worker.progress_runs_path` (`runs_v1.jsonl`)
- expected phase durations are estimated; with limited data it falls back to defaults and hints (`cold_start`, `low_sample_n`, `phase_defaults`)
- the tracker publishes live `progress`, `eta_total_s`, `eta_remaining_s`, `elapsed_s`, `eta_confidence`, `eta_hints` into `status.json`

This allows ETA/progress to refine itself as more jobs complete on the same job-profile.

## Status JSON

`status.json` is the main file clients read while a job is running.

It contains the worker's current state, progress, messages, timing information, and final ASR result metadata. Client-specific fields may exist alongside it, but the worker only manages its own status and runtime fields.

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

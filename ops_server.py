from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def _iso_utc_now() -> str:
  return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class OpsSnapshotStore:
  def __init__(self) -> None:
    self._lock = threading.Lock()
    self._snapshot: dict[str, Any] = {
      "service": "asr-worker",
      "version": "ops_v1",
      "now_utc": _iso_utc_now(),
      "window_s": 300,
      "health": "warn",
      "health_reason": "Ops snapshot not initialized yet",
      "summary": {},
      "details": {"error": "ops snapshot not initialized yet"},
    }

  def set_snapshot(self, snapshot: dict[str, Any]) -> None:
    with self._lock:
      self._snapshot = dict(snapshot or {})

  def get_snapshot(self) -> dict[str, Any]:
    with self._lock:
      return dict(self._snapshot)


def _ops_page_html() -> str:
  return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ASR Worker Operations</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fb;
      --card: #fff;
      --text: #161616;
      --muted: #5f6470;
      --line: #d8dde8;
      --ok: #157f3b;
      --warn: #9a6b00;
      --error: #b42318;
    }
    html, body { height: 100%; background: var(--bg); overflow: hidden; }
    body { margin: 0; color: var(--text); font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, sans-serif; }
    main { max-width: 980px; margin: 0 auto; padding: 16px; height: 100%; box-sizing: border-box; display: flex; flex-direction: column; }
    h1 { margin: 0 0 10px 0; font-size: 22px; }
    #meta { margin: 0 0 12px 0; color: var(--muted); font-size: 13px; }
    .cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-bottom: 12px; }
    .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 10px; }
    .card .k { color: var(--muted); font-size: 12px; margin-bottom: 4px; }
    .card .v { font-size: 18px; font-weight: 700; }
    .badge { display: inline-block; border-radius: 999px; padding: 2px 10px; font-size: 12px; font-weight: 700; }
    .badge.ok { color: #fff; background: var(--ok); }
    .badge.warn { color: #fff; background: var(--warn); }
    .badge.error { color: #fff; background: var(--error); }
    pre { background: #fff; border: 1px solid var(--line); border-radius: 10px; padding: 12px; overflow: auto; margin: 0; flex: 1; min-height: 0; }
    @media (max-width: 940px) { .cards { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
  <main>
    <h1>ASR Worker Operations</h1>
    <div id="meta">Loading...</div>
    <div class="cards">
      <div class="card"><div class="k">Inbox</div><div class="v" id="inbox_count">-</div></div>
      <div class="card"><div class="k">Running</div><div class="v" id="running_count">-</div></div>
      <div class="card"><div class="k">Done (5m)</div><div class="v" id="jobs_completed_5m">-</div></div>
      <div class="card"><div class="k">Submit Fail Rate (5m)</div><div class="v" id="submit_fail_rate_5m">-</div></div>
    </div>
    <pre id="payload"></pre>
  </main>
  <script>
    async function refresh() {
      const meta = document.getElementById("meta");
      const payload = document.getElementById("payload");
      try {
        const res = await fetch("/ops/metrics", { cache: "no-store" });
        if (!res.ok) throw new Error("HTTP " + res.status);
        const data = await res.json();
        const s = data.summary || {};
        const health = String(data.health || "warn");
        const reason = String(data.health_reason || "");
        const reasonText = reason ? (" | reason: " + reason) : "";
        meta.innerHTML = "Health: <span class='badge " + health + "'>" + health + "</span>" + reasonText + " | updated: " + new Date().toLocaleTimeString();
        document.getElementById("inbox_count").textContent = String(s.inbox_count ?? "-");
        document.getElementById("running_count").textContent = String(s.running_count ?? "-");
        document.getElementById("jobs_completed_5m").textContent = String(s.jobs_completed_5m ?? "-");
        const failRate = Number(s.submit_fail_rate_5m ?? 0);
        document.getElementById("submit_fail_rate_5m").textContent = failRate.toFixed(2);
        payload.textContent = JSON.stringify(data, null, 2);
      } catch (err) {
        meta.textContent = "Metrics unavailable: " + err;
      }
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""


class WorkerOpsServer:
  def __init__(self, *, host: str, port: int, store: OpsSnapshotStore) -> None:
    self._host = str(host or "127.0.0.1").strip() or "127.0.0.1"
    self._port = max(1, int(port))
    self._store = store
    self._server: ThreadingHTTPServer | None = None
    self._thread: threading.Thread | None = None

  def start(self) -> None:
    if self._server is not None:
      return

    store = self._store

    class _Handler(BaseHTTPRequestHandler):
      def do_GET(self) -> None:  # noqa: N802
        if self.path == "/ops/metrics":
          body = json.dumps(store.get_snapshot(), ensure_ascii=False).encode("utf-8")
          self.send_response(200)
          self.send_header("Content-Type", "application/json; charset=utf-8")
          self.send_header("Cache-Control", "no-store")
          self.send_header("Content-Length", str(len(body)))
          self.end_headers()
          self.wfile.write(body)
          return
        if self.path == "/ops":
          body = _ops_page_html().encode("utf-8")
          self.send_response(200)
          self.send_header("Content-Type", "text/html; charset=utf-8")
          self.send_header("Cache-Control", "no-store")
          self.send_header("Content-Length", str(len(body)))
          self.end_headers()
          self.wfile.write(body)
          return
        self.send_response(404)
        self.end_headers()

      def log_message(self, fmt: str, *args: Any) -> None:
        return

    try:
      self._server = ThreadingHTTPServer((self._host, self._port), _Handler)
    except Exception as exc:
      print(f"worker_daemon ops server disabled host={self._host} port={self._port}: {type(exc).__name__}: {exc}", flush=True)
      self._server = None
      return
    self._thread = threading.Thread(target=self._server.serve_forever, name="worker-ops-http", daemon=True)
    self._thread.start()
    print(f"worker_daemon ops server listening host={self._host} port={self._port}", flush=True)

  def stop(self) -> None:
    if self._server is None:
      return
    self._server.shutdown()
    self._server.server_close()
    self._server = None
    if self._thread is not None:
      self._thread.join(timeout=1.0)
      self._thread = None

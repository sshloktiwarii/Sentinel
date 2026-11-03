"""
sentinel/server.py

FastAPI application for Sentinel-AI.
Exposes:
  - WebSocket  /ws/telemetry           — live 1 s telemetry stream
  - GET        /api/telemetry         — instantaneous telemetry snapshot
  - GET        /api/history?window=…   — historical data for 1m / 5m / 1h windows
  - GET        /api/spikes             — top-5 spike events in the last 24 hours
  - GET        /api/quotas             — AI-provider quota health snapshot
  - GET        /api/engines            — local LLM engine & KV-cache telemetry
"""

import asyncio
import atexit
import hmac
import json
import os
from pathlib import Path
import time
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.responses import JSONResponse

from sentinel.config import get_config
from sentinel.database import get_history_window, get_spikes, init_db, insert_telemetry
from sentinel.engines import get_local_engines_async
from sentinel.harvester import (
    compute_thrash_danger_index,
    get_engine_pressure_snapshot,
    get_gpu_wired_limit,
    get_pageout_count,
    get_swap_usage,
    get_wired_memory_mb,
    is_apple_silicon,
)
from sentinel.quota import get_all_quotas, get_velocity_metrics

app = FastAPI(title="Sentinel-AI")

# Restrict CORS strictly to local loopback origins; wildcard ["*"] is explicitly forbidden.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:3000",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    """Ensure 401 Unauthorized returns JSON body {"error": "Unauthorized"} per interface contract."""
    if exc.status_code == 401:
        return JSONResponse(
            status_code=401,
            content={"error": "Unauthorized"},
            headers=exc.headers,
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=exc.headers,
    )


def verify_token(
    request: Request,
    authorization: str | None = Header(default=None),
) -> bool:
    """Validate Bearer token against SENTINEL_AUTH_TOKEN / SENTINEL_API_KEY.

    If no auth secret is configured, enforces loopback caller restriction.
    If an auth secret is configured, validates Bearer token using constant-time comparison.
    """
    secret = os.environ.get("SENTINEL_AUTH_TOKEN") or os.environ.get("SENTINEL_API_KEY")
    if secret:
        token = None
        if authorization:
            parts = authorization.split()
            if len(parts) == 2 and parts[0].lower() == "bearer":
                token = parts[1]
        if not token:
            token = request.query_params.get("token")
        if not token or not hmac.compare_digest(token, secret):
            raise HTTPException(
                status_code=401,
                detail="Unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return True

    # If no token is configured, enforce loopback
    client_host = request.client.host if request.client else None
    if client_host and client_host not in ("127.0.0.1", "::1", "localhost", "testclient"):
        raise HTTPException(status_code=403, detail="Forbidden: non-loopback access prohibited")

    return True


import logging
import signal
import subprocess

logger = logging.getLogger("sentinel.server")

# Track paused process PIDs for SIGSTOP/SIGCONT recovery
DEFAULT_SENTINEL_DIR = Path.home() / ".sentinel"


def _get_throttled_pids_path() -> Path:
    """Return path to persistent state file for throttled PIDs."""
    return Path(
        os.environ.get(
            "SENTINEL_THROTTLED_PIDS_PATH",
            str(DEFAULT_SENTINEL_DIR / "throttled_pids.json"),
        )
    )


def _load_throttled_pids() -> set[int]:
    """Load persisted throttled PIDs from disk."""
    path = _get_throttled_pids_path()
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {int(p) for p in data if isinstance(p, (int, str)) and str(p).isdigit()}
    except Exception as exc:
        logger.warning("Failed to load throttled PIDs from %s: %s", path, exc)
    return set()


def _save_throttled_pids(pids: set[int]) -> None:
    """Persist throttled PIDs atomically to disk."""
    path = _get_throttled_pids_path()
    try:
        if not pids:
            if path.exists():
                path.unlink(missing_ok=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(sorted(list(pids))), encoding="utf-8")
        tmp_path.replace(path)
    except Exception as exc:
        logger.warning("Failed to persist throttled PIDs to %s: %s", path, exc)


_THROTTLED_PIDS: set[int] = _load_throttled_pids()


def resume_all_throttled_processes() -> list[int]:
    """
    Iterate over all tracked and persisted throttled PIDs and send SIGCONT,
    ensuring no LLM processes remain frozen.
    Clears in-memory set and state file.
    """
    global _THROTTLED_PIDS
    pids_to_resume = set(_THROTTLED_PIDS) | _load_throttled_pids()
    resumed = []
    for pid in sorted(pids_to_resume):
        try:
            os.kill(pid, signal.SIGCONT)
            resumed.append(pid)
            logger.info("▶ RESUMED FROZEN PROCESS: Sent SIGCONT to PID %d", pid)
        except ProcessLookupError:
            pass
        except Exception as e:
            logger.error("Failed to send SIGCONT to PID %d: %s", pid, e)

    _THROTTLED_PIDS.clear()
    _save_throttled_pids(set())
    return resumed


atexit.register(resume_all_throttled_processes)


def _get_target_llm_pids() -> list[int]:
    """Find running local LLM engine processes (ollama, llama-server, vllm) using exact matching.

    Handles Linux, macOS, and BSD systems where regex alternation with -x may require -E or
    per-process matching to avoid silent pgrep failures.
    """
    pids: set[int] = set()
    current_pid = os.getpid()
    target_names = ["ollama", "llama-server", "vllm", "text-generation"]
    pattern = "|".join(target_names)

    # Primary check: exact pattern match across process names
    try:
        output = subprocess.check_output(["pgrep", "-x", pattern], text=True)
        for line in output.strip().splitlines():
            line_str = line.strip()
            if line_str and line_str.isdigit():
                pid = int(line_str)
                if pid != current_pid:
                    pids.add(pid)
    except Exception:
        # Fallback for BSD/Darwin systems where regex alternation requires -E or individual name queries
        try:
            output = subprocess.check_output(["pgrep", "-E", "-x", pattern], text=True)
            for line in output.strip().splitlines():
                line_str = line.strip()
                if line_str and line_str.isdigit():
                    pid = int(line_str)
                    if pid != current_pid:
                        pids.add(pid)
        except Exception:
            for name in target_names:
                try:
                    out = subprocess.check_output(["pgrep", "-x", name], text=True)
                    for line in out.strip().splitlines():
                        line_str = line.strip()
                        if line_str and line_str.isdigit():
                            pid = int(line_str)
                            if pid != current_pid:
                                pids.add(pid)
                except Exception:
                    pass

    return sorted(list(pids))


def apply_active_process_throttling(thrash_index: float) -> list[int]:
    """
    Active Process Throttling: If memory pressure reaches critical levels (TDI >= 0.90),
    actively pause runaway LLM inference processes via SIGSTOP to prevent macOS kernel swap freeze.
    When thrash drops back into safety (TDI < 0.75), resumes them with SIGCONT.
    Persists throttled PIDs to state set/file so no processes remain frozen across daemon restarts.
    """
    cfg = get_config()
    auto_throttle_enabled = cfg.get("auto_throttle", True)
    if not auto_throttle_enabled:
        return []

    crit_th = float(cfg.get("tdi_critical_threshold", 0.90))
    warn_th = float(cfg.get("tdi_warning_threshold", 0.75))

    actioned = []
    if thrash_index >= crit_th:
        target_pids = _get_target_llm_pids()
        newly_throttled = False
        for pid in target_pids:
            if pid not in _THROTTLED_PIDS:
                try:
                    os.kill(pid, signal.SIGSTOP)
                    _THROTTLED_PIDS.add(pid)
                    newly_throttled = True
                    actioned.append(pid)
                    logger.warning(
                        "🛑 ACTIVE THROTTLE: Paused runaway LLM process PID %d via SIGSTOP (TDI: %.2f)",
                        pid,
                        thrash_index,
                    )
                except Exception as e:
                    logger.error("Failed to SIGSTOP PID %d: %s", pid, e)
        if newly_throttled:
            _save_throttled_pids(_THROTTLED_PIDS)
    elif thrash_index < warn_th and _THROTTLED_PIDS:
        # Memory pressure normalized: resume paused processes
        for pid in list(_THROTTLED_PIDS):
            try:
                os.kill(pid, signal.SIGCONT)
                actioned.append(pid)
                logger.info("▶ ACTIVE THROTTLE RESUME: Resumed LLM process PID %d via SIGCONT", pid)
            except ProcessLookupError:
                pass
            except Exception as e:
                logger.error("Failed to SIGCONT PID %d: %s", pid, e)
            finally:
                _THROTTLED_PIDS.discard(pid)
        _save_throttled_pids(_THROTTLED_PIDS)

    return actioned


def get_alert_pill(tdi: float) -> str:
    """Map Thrash Danger Index to alert pill color indicator based on config thresholds."""
    cfg = get_config()
    crit_th = float(cfg.get("tdi_critical_threshold", 0.90))
    warn_th = float(cfg.get("tdi_warning_threshold", 0.75))
    if tdi >= crit_th:
        return "🔴"
    elif tdi >= warn_th:
        return "🟡"
    return "🟢"


def _collect_reading() -> dict:
    """Gather one telemetry snapshot and return it as a plain dict."""
    limit_mb     = get_gpu_wired_limit()
    wired_mb     = get_wired_memory_mb()
    total_swap, used_swap = get_swap_usage()
    pageouts     = get_pageout_count()
    thrash_index = compute_thrash_danger_index(wired_mb, limit_mb, used_swap)
    engine_info  = get_engine_pressure_snapshot(wired_mb)
    velocity     = get_velocity_metrics()
    throttled    = apply_active_process_throttling(thrash_index)
    ts           = time.time()
    return {
        "timestamp":        ts,
        "wired_mb":         wired_mb,
        "limit_mb":         limit_mb,
        "swap_total_mb":    total_swap,
        "swap_used_mb":     used_swap,
        "pageouts":         pageouts,
        "thrash_index":     thrash_index,
        "alert_pill":       get_alert_pill(thrash_index),
        "engine_active":    engine_info.get("engine_active", False),
        "kv_cache_mb":      engine_info.get("kv_cache_mb", 0.0),
        "kv_pressure_pct":  engine_info.get("kv_pressure_pct", 0.0),
        "velocity":         velocity,
        "throttled_pids":   list(_THROTTLED_PIDS),
        "is_apple_silicon": is_apple_silicon(),
    }

# ── Lifecycle ─────────────────────────────────────────────────────────────────

# ── Background Single-Poller & Broadcaster ───────────────────────────────────

_latest_reading: dict | None = None
_poller_task: asyncio.Task | None = None
_subscribers: set[asyncio.Queue] = set()

async def _telemetry_poller() -> None:
    """Dedicated background loop that polls system metrics exactly once per second.
    Broadcasts the reading to all connected WebSockets without per-client sampling or lock thrash."""
    global _latest_reading
    while True:
        try:
            reading = await asyncio.get_event_loop().run_in_executor(
                None, _collect_reading
            )
            _latest_reading = reading

            # Persist to DB once per second
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda r=reading: insert_telemetry(
                    timestamp    = r["timestamp"],
                    wired_mb     = r["wired_mb"],
                    swap_used_mb = r["swap_used_mb"],
                    pageouts     = r["pageouts"],
                    thrash_index = r["thrash_index"],
                ),
            )

            # Broadcast to all active websocket subscriber queues
            for q in list(_subscribers):
                try:
                    q.put_nowait(reading)
                except asyncio.QueueFull:
                    pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Telemetry poller error: %s", e)

        await asyncio.sleep(1.0)


@app.on_event("startup")
async def _startup() -> None:
    """Ensure database is initialised, recover any previously frozen processes, and launch single background poller unless disabled during tests."""
    global _poller_task
    init_db()

    # Startup recovery: resume any LLM processes left frozen by an uncoordinated daemon shutdown
    recovered = resume_all_throttled_processes()
    if recovered:
        logger.warning(
            "Startup recovery: resumed %d frozen LLM processes left from previous session: %s",
            len(recovered),
            recovered,
        )

    if os.getenv("SENTINEL_DISABLE_BACKGROUND_POLLER") == "true":
        return
    if _poller_task is None or _poller_task.done():
        _poller_task = asyncio.create_task(_telemetry_poller())


@app.on_event("shutdown")
async def _shutdown() -> None:
    """Cancel background poller cleanly and resume all throttled processes on app termination."""
    global _poller_task
    if _poller_task and not _poller_task.done():
        _poller_task.cancel()
        try:
            await _poller_task
        except asyncio.CancelledError:
            pass

    # Shutdown: resume any currently throttled PIDs so no LLM processes remain frozen
    resumed = resume_all_throttled_processes()
    if resumed:
        logger.info(
            "Shutdown: resumed %d throttled LLM processes via SIGCONT: %s",
            len(resumed),
            resumed,
        )


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/telemetry")
async def ws_telemetry(websocket: WebSocket) -> None:
    """Stream live telemetry to the connected client by consuming broadcast events from the single poller."""
    secret = os.environ.get("SENTINEL_AUTH_TOKEN") or os.environ.get("SENTINEL_API_KEY")
    if secret:
        query_params = getattr(websocket, "query_params", {})
        token = query_params.get("token") if hasattr(query_params, "get") else None
        if not token:
            headers = getattr(websocket, "headers", {})
            auth_header = headers.get("authorization") if hasattr(headers, "get") else None
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header[7:].strip()
        if not token or not hmac.compare_digest(token, secret):
            if hasattr(websocket, "close"):
                await websocket.close(code=1008)
            return

    client = getattr(websocket, "client", None)
    client_host = client.host if client and hasattr(client, "host") else None
    if not secret and client_host and client_host not in ("127.0.0.1", "::1", "localhost", "testclient"):
        if hasattr(websocket, "close"):
            await websocket.close(code=1008)
        return

    await websocket.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)

    # Immediately deliver current reading if available so client never stalls on connect
    if _latest_reading is not None:
        try:
            await websocket.send_json(_latest_reading)
        except Exception:
            pass

    try:
        while True:
            reading = await q.get()
            await websocket.send_json(reading)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    finally:
        _subscribers.discard(q)

# ── REST: history ─────────────────────────────────────────────────────────────

@app.get("/api/history")
async def api_history(
    window: Literal["1m", "5m", "1h"] = Query(
        default     = "1m",
        description = "Time window: '1m' (60 s raw), '5m' (300 s sampled), '1h' (3600 s bucketed).",
    ),
    _auth: bool = Depends(verify_token),
) -> list[dict]:
    """Return historical telemetry for the requested time window.

    Results are oldest-first so the frontend can render them directly.
    """
    try:
        rows = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_history_window(window)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return rows

# ── REST: spikes ──────────────────────────────────────────────────────────────

@app.get("/api/spikes")
async def api_spikes(_auth: bool = Depends(verify_token)) -> list[dict]:
    """Return the top-5 spike events (thrash_index >= 0.4 or swap > 0) in the last 24 h.

    Each item contains: timestamp, thrash_index, wired_mb, swap_used_mb, pageouts.
    Ordered by thrash_index descending.
    """
    rows = await asyncio.get_event_loop().run_in_executor(None, get_spikes)
    return rows

# ── REST: quotas ──────────────────────────────────────────────────────────────

@app.get("/api/quotas")
async def api_quotas(
    wrap: bool = Query(
        default=False,
        description="When true, wraps list in a dict containing top-level velocity block",
    ),
    _auth: bool = Depends(verify_token),
) -> Any:
    """Return quota health snapshot for all configured AI providers.

    Each item contains: id, provider, model, remaining_pct, tokens_left,
    resets_in, status, velocity.

    Never returns an empty list — falls back to baseline defaults when live
    provider data is unavailable.
    """
    import inspect
    if inspect.iscoroutinefunction(get_all_quotas):
        rows = await get_all_quotas()
    else:
        rows = await asyncio.get_event_loop().run_in_executor(None, get_all_quotas)
    if wrap:
        return {"quotas": rows, "velocity": get_velocity_metrics()}
    return rows

# ── REST: velocity ────────────────────────────────────────────────────────────

@app.get("/api/velocity")
async def api_velocity(_auth: bool = Depends(verify_token)) -> dict:
    """Return instantaneous token velocity and runaway agent loop detection metrics."""
    return get_velocity_metrics()

# ── REST: telemetry ───────────────────────────────────────────────────────────

@app.get("/api/telemetry")
async def api_telemetry(_auth: bool = Depends(verify_token)) -> dict:
    """Return instantaneous system telemetry snapshot with KV-cache pressure."""
    return await asyncio.get_event_loop().run_in_executor(None, _collect_reading)

# ── REST: engines ─────────────────────────────────────────────────────────────

@app.get("/api/engines")
async def api_engines(_auth: bool = Depends(verify_token)) -> dict:
    """Return active local LLM engine status, loaded models, and KV-cache breakdown."""
    wired_mb = await asyncio.get_event_loop().run_in_executor(None, get_wired_memory_mb)
    return await get_local_engines_async(wired_mb=wired_mb)


# ── REST: config ──────────────────────────────────────────────────────────────

@app.get("/api/config")
async def api_config(_auth: bool = Depends(verify_token)) -> dict:
    """Return active user configuration."""
    return get_config()


# ── Static Frontend ──────────────────────────────────────────────────────────

dist_dir = Path(__file__).parent / "web_dist"
if dist_dir.exists() and (dist_dir / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(dist_dir), html=True), name="frontend")


def start() -> None:
    """CLI entry point to launch the Sentinel-AI server."""
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Sentinel-AI Telemetry & Quota Server")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address to bind (strictly loopback, default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("SENTINEL_PORT", 8000)),
        help="Port to bind (default: 8000)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )
    args = parser.parse_args()

    # Enforce loopback binding strictly to 127.0.0.1; never 0.0.0.0
    bind_host = args.host
    if bind_host in ("0.0.0.0", "::", "", "all"):
        bind_host = "127.0.0.1"

    uvicorn.run("sentinel.server:app", host=bind_host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    start()




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
import os
from pathlib import Path
import time
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

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

import logging
import signal
import subprocess

logger = logging.getLogger("sentinel.server")

# Track paused process PIDs for SIGSTOP/SIGCONT recovery
_THROTTLED_PIDS: set[int] = set()


def _get_target_llm_pids() -> list[int]:
    """Find running local LLM engine processes (ollama, llama-server, vllm)."""
    pids = []
    try:
        output = subprocess.check_output(
            ["pgrep", "-f", "ollama|llama-server|vllm|text-generation"],
            text=True,
        )
        for line in output.strip().splitlines():
            pid = int(line.strip())
            if pid != os.getpid():
                pids.append(pid)
    except Exception:
        pass
    return pids


def apply_active_process_throttling(thrash_index: float) -> list[int]:
    """
    Active Process Throttling: If memory pressure reaches critical levels (TDI >= 0.90),
    actively pause runaway LLM inference processes via SIGSTOP to prevent macOS kernel swap freeze.
    When thrash drops back into safety (TDI < 0.75), resumes them with SIGCONT.
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
        for pid in target_pids:
            if pid not in _THROTTLED_PIDS:
                try:
                    os.kill(pid, signal.SIGSTOP)
                    _THROTTLED_PIDS.add(pid)
                    actioned.append(pid)
                    logger.warning(
                        "🛑 ACTIVE THROTTLE: Paused runaway LLM process PID %d via SIGSTOP (TDI: %.2f)",
                        pid,
                        thrash_index,
                    )
                except Exception as e:
                    logger.error("Failed to SIGSTOP PID %d: %s", pid, e)
    elif thrash_index < warn_th and _THROTTLED_PIDS:
        # Memory pressure normalized: resume paused processes
        for pid in list(_THROTTLED_PIDS):
            try:
                os.kill(pid, signal.SIGCONT)
                actioned.append(pid)
                logger.info("▶ ACTIVE THROTTLE RESUME: Resumed LLM process PID %d via SIGCONT", pid)
            except Exception as e:
                logger.error("Failed to SIGCONT PID %d: %s", pid, e)
            finally:
                _THROTTLED_PIDS.discard(pid)

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

@app.on_event("startup")
async def _startup() -> None:
    """Ensure the database is initialised before the first connection arrives."""
    init_db()

# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/telemetry")
async def ws_telemetry(websocket: WebSocket) -> None:
    """Stream live telemetry to the connected client at 1-second intervals."""
    await websocket.accept()
    try:
        while True:
            reading = await asyncio.get_event_loop().run_in_executor(
                None, _collect_reading
            )

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

            await websocket.send_json(reading)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

# ── REST: history ─────────────────────────────────────────────────────────────

@app.get("/api/history")
async def api_history(
    window: Literal["1m", "5m", "1h"] = Query(
        default     = "1m",
        description = "Time window: '1m' (60 s raw), '5m' (300 s sampled), '1h' (3600 s bucketed).",
    )
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
async def api_spikes() -> list[dict]:
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
    )
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
async def api_velocity() -> dict:
    """Return instantaneous token velocity and runaway agent loop detection metrics."""
    return get_velocity_metrics()

# ── REST: telemetry ───────────────────────────────────────────────────────────

@app.get("/api/telemetry")
async def api_telemetry() -> dict:
    """Return instantaneous system telemetry snapshot with KV-cache pressure."""
    return await asyncio.get_event_loop().run_in_executor(None, _collect_reading)

# ── REST: engines ─────────────────────────────────────────────────────────────

@app.get("/api/engines")
async def api_engines() -> dict:
    """Return active local LLM engine status, loaded models, and KV-cache breakdown."""
    wired_mb = await asyncio.get_event_loop().run_in_executor(None, get_wired_memory_mb)
    return await get_local_engines_async(wired_mb=wired_mb)


# ── REST: config ──────────────────────────────────────────────────────────────

@app.get("/api/config")
async def api_config() -> dict:
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




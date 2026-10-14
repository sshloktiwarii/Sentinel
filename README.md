# Sentinel-AI

**Zero-overhead unified memory telemetry and runaway agent loop tripwire for Apple Silicon.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Platform: macOS Apple Silicon](https://img.shields.io/badge/Platform-macOS%20Apple%20Silicon-black.svg)]()
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-brightgreen.svg)]()
[![Release: v1.0.1](https://img.shields.io/badge/Release-v1.0.1-emerald.svg)]()

Sentinel-AI is a native systems monitoring suite and tripwire engine designed specifically for Apple Silicon (M1/M2/M3/M4) local AI engineers. It continuously guards unified memory against catastrophic SSD swap death spirals during heavy local LLM inference (Ollama, LM Studio, vLLM, MLX) while monitoring AI agent token burn rates (TPS, TPM, RPM) across local proxy multiplexers like OmniRoute.

---

## System Benchmarks

Compared against traditional subprocess-scraping CLI monitors (`vm_stat`, `sysctl`, `psutil` subshells), Sentinel-AI interfaces directly with Mach microkernel primitives and foreign function C-bindings (`ctypes` into `/usr/lib/libSystem.B.dylib`):

| Metric | Mach Kernel C-Bindings (`sentinel`) | Subprocess Scraping (`vm_stat` / `sysctl`) | Improvement |
| :--- | :--- | :--- | :--- |
| **Probe Latency** | **~10 µs** | 9.8 ms – 14.5 ms | **~1,000x faster** |
| **CPU Utilization** | **< 0.02%** (background idle) | 1.8% – 3.2% (process spawn overhead) | **~99% reduction** |
| **Memory Footprint** | **~14 MB RSS** (daemon) | Spawns transient Python/shell subshells | **Stable & contiguous** |

---

## Core Architecture Highlights

- **10 µs Mach Kernel Telemetry**: Direct Darwin Mach foreign function bindings (`mach_host_self()`, `host_statistics64()`, `HOST_VM_INFO64`) bypass user-space process spawning to capture wired memory, swap pages, and pageout counters in microseconds.
- **Calibrated Thrash Danger Index (TDI)**: Derives a normalized [0.0, 1.0] risk score combining physical wired GPU pressure (70%) and dynamic swap consumption (30%). Dynamically scales against physical RAM via $\max(2048\text{ MB}, \text{RAM} \times 0.25)$.
- **Token Velocity & Runaway Detection Engine**: A high-precision 60-second sliding ring buffer tracks instantaneous Tokens Per Second (TPS), extrapolated Tokens Per Minute (TPM), and Requests Per Minute (RPM) across local inference proxies. Dispatches native macOS banners when runaway recursive agent loops are detected.
- **Zero-Node Embedded Canvas**: The rich Next.js web dashboard is statically exported directly into `sentinel/web_dist/` and served over a single port alongside WebSocket streams by FastAPI. No Node.js runtime or npm dependencies are required for end users.
- **Native Persistence (`sentinel-service`)**: Autonomous background execution via macOS `launchd` user agents (`~/Library/LaunchAgents/com.sentinel.daemon.plist`), providing zero-effort boot persistence, automatic process restarts, and log rotation.
- **Ambient Companion (`sentinel-bar`)**: Native macOS menu bar tray built with Apple HIG aesthetics, showing live TDI status pills, wired VRAM gauges, swap pageouts, and provider quota status at a glance.

For detailed architecture diagrams, mathematical formulations, and engineering designs:
- See [architecture.md](architecture.md) for full system engineering and telemetry documentation.
- See [decision.md](decision.md) for Architectural Decision Records (ADRs 001 – 009).

---

## Quickstart & Installation

### 1. Install via Pip

```bash
# Install Sentinel-AI
pip install sentinel-ai
```

### 2. Manage as a Background Service

Sentinel-AI integrates natively with macOS `launchd`:

```bash
# Install and bootstrap background LaunchAgent
sentinel-service install

# Check service status and healthcheck
sentinel-service status

# Tail background logs
sentinel-service logs

# Stop and uninstall LaunchAgent
sentinel-service uninstall
```

### 3. Launch the Menu Bar Tray

Run the ambient menu bar companion in the background:

```bash
sentinel-bar &
```

### 4. Open the Web Dashboard

Start the standalone server directly (if not running via `sentinel-service`):

```bash
sentinel
```

Then navigate to the embedded web canvas:

```bash
open http://localhost:8000
```

---

## Configuration Reference

Sentinel-AI features a zero-dependency configuration engine storing user preferences at `~/.sentinel/config.json`. The file is automatically bootstrapped on first launch with sensible defaults:

```json
{
  "proxy_url": "http://localhost:20128",
  "poll_interval_seconds": 1.0,
  "tdi_warning_threshold": 0.75,
  "tdi_critical_threshold": 0.90,
  "velocity_alert_tps": 150.0,
  "velocity_alert_rpm": 45.0,
  "notification_debounce_seconds": 60.0,
  "swap_limit_ratio": 0.25
}
```

### Configuration Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `proxy_url` | `str` | `"http://localhost:20128"` | Base URL of local proxy router (OmniRoute / LiteLLM). |
| `poll_interval_seconds` | `float` | `1.0` | Telemetry probe sampling interval in seconds. |
| `tdi_warning_threshold` | `float` | `0.75` | TDI threshold for warning indicator (warning state). |
| `tdi_critical_threshold`| `float` | `0.90` | TDI threshold for critical notification tripwire (critical state). |
| `velocity_alert_tps` | `float` | `150.0` | Token velocity threshold triggering runaway agent alerts. |
| `velocity_alert_rpm` | `float` | `45.0` | Request rate threshold triggering runaway loop warnings. |
| `notification_debounce_seconds` | `float` | `60.0` | Cooldown window between repeated velocity banner alerts. |
| `swap_limit_ratio` | `float` | `0.25` | Fraction of total physical memory used as swap ceiling. |

---

## CLI Reference

Sentinel-AI packages three official command-line entry points:

- **`sentinel`**: Starts the FastAPI telemetry backend and serves the embedded Next.js canvas on `http://127.0.0.1:8000`. Supports `--host`, `--port`, and `--reload`.
- **`sentinel-bar`**: Launches the native AppKit menu bar companion.
- **`sentinel-service`**: Controls the macOS `launchd` background daemon (`install`, `uninstall`, `status`, `logs`).

---

## Development & Testing

```bash
# Clone the repository
git clone https://github.com/sshloktiwarii/sentinel-ai.git
cd sentinel-ai

# Create virtual environment and install development dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run full test suite (172 unit & integration tests)
pytest tests/
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

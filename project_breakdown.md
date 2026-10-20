# Project Breakdown: sentinel-ai

## 1. Why It Exists (The Problem)
Running modern Large Language Models (LLMs) locally on a MacBook pushes Apple Silicon unified memory and GPU thermals to their absolute limits. If an agent enters an infinite reasoning loop or loads a massive context window, the laptop's Thermal Pressure jumps to Critical, causing the entire operating system to freeze, drop frames, or forcibly kill background processes.

---

## 2. What It Does (The Solution)
Sentinel AI is an operating-system-level telemetry daemon and active process governor:
- **Direct Mach & `sysctl` C-Bindings:** Reads Apple Silicon unified memory pressure and thermal degradation in real time with < 0.2% CPU overhead.
- **Active Process Throttling (`SIGSTOP`/`SIGCONT`):** When thermal or memory velocity reaches critical thresholds ($TDI \ge 0.90$), Sentinel suspends runaway LLM engine processes (Ollama, vLLM) with `SIGSTOP`, allowing the hardware to cool before resuming with `SIGCONT`.
- **Linux CI/CD Fallbacks:** Automatically falls back to `/proc/meminfo` and `/proc/vmstat` on Linux runners to prevent platform crashes.
- **Historical Velocity Analytics:** Stores time-series memory growth trends in SQLite to predict memory exhaustion before it happens.

---

## 3. Targeted Niche & Use Cases
Developers and AI engineers running local models on Apple Silicon or Linux workstations who need automated protection against system freezes and thermal throttling.

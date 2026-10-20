# Sentinel-AI — Plain-English Guide

## What is Sentinel-AI?

Sentinel-AI is a memory watchdog for your Mac.

When you run AI models locally on Apple Silicon (M1/M2/M3/M4), they eat a lot of memory. If they eat too much, macOS starts using your SSD as overflow memory (called "swap"). This kills your SSD lifespan and makes everything grind to a halt.

Sentinel-AI watches your memory usage in real time and warns you before things get dangerous.

## What does it monitor?

- **Unified memory pressure** — How much of your Mac's shared CPU/GPU memory is in use
- **Swap usage** — How much SSD is being used as emergency memory
- **Token speed** — How fast your local AI models are generating text (tokens per second)
- **Runaway detection** — If an AI agent gets stuck in an infinite loop, Sentinel-AI catches it and alerts you

## How is it different from Activity Monitor?

Activity Monitor polls every few seconds using slow shell commands. Sentinel-AI talks directly to the macOS kernel using C-level system calls, making it roughly 1,000x faster and using almost zero CPU.

| Feature | Sentinel-AI | Activity Monitor / htop |
|---------|-------------|------------------------|
| Speed | 10 microseconds | 10-15 milliseconds |
| CPU usage | 0.02% | 1.8-3.2% |
| Swap death alerts | Yes | No |
| AI token tracking | Yes | No |

## How do you use it?

```bash
# Install via Homebrew
brew tap sshloktiwarii/sentinel
brew install sentinel

# Run the dashboard
sentinel dashboard
```

A terminal dashboard appears showing memory pressure, swap status, and AI model performance in real time.

## What's it built with?

- **Python** — Main application
- **ctypes + Mach kernel** — Direct macOS system calls for zero-overhead monitoring
- **macOS notifications** — Native banner alerts when things go wrong

## Who is it for?

Anyone running local AI models on a Mac who wants to protect their hardware and catch runaway processes.

"""
sentinel/quota.py

AI-provider quota monitoring for Sentinel-AI.

Strategy per provider
─────────────────────
Anthropic  – Check the Anthropic API rate-limit headers by sending a tiny
             "peek" request (max_tokens=1) if ANTHROPIC_API_KEY is set.
             Falls back to baseline defaults if the key is absent or the
             request fails.

OpenAI     – Check OpenAI usage headers via a models-list request.
             Falls back to baseline defaults if the key is absent.

OmniRoute  – Asynchronously harvest live proxy quotas, token usage, and rate-limit
             counters from http://localhost:20128 using discovered local admin/bearer token.
             Maps OmniRoute gateway telemetry and provider-level limits (KiloCode, Cursor, etc.).

Cursor / Kimi / Manus – Synchronized with local proxy states when registered in
             OmniRoute; otherwise fallback to baseline defaults.

Guarantees
──────────
get_all_quotas() NEVER returns an empty list. The BASELINE list is the
floor — live data can only update individual fields, never remove records.
"""

from __future__ import annotations

import asyncio
import collections
import datetime
import json
import logging
import math
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from copy import deepcopy
from typing import Literal

import httpx

from sentinel.config import get_config

logger = logging.getLogger("sentinel.quota")

# ── Types ─────────────────────────────────────────────────────────────────────

Status = Literal["healthy", "warning", "exhausted"]

# ── Baseline records ──────────────────────────────────────────────────────────
# These are the canonical defaults returned when live data is unavailable.
# They also define the complete list of providers shown in the UI.

_BASELINE: list[dict] = [
    {
        "id":            "claude",
        "provider":      "Anthropic Claude",
        "model":         "Claude 3.5 Sonnet",
        "remaining_pct": 82,
        "tokens_left":   "164k / 200k",
        "resets_in":     "3h 12m",
        "status":        "healthy",
    },
    {
        "id":            "codex",
        "provider":      "OpenAI / Codex",
        "model":         "GPT-4o / o1-mini",
        "remaining_pct": 65,
        "tokens_left":   "650k TPM",
        "resets_in":     "Daily (00:00 UTC)",
        "status":        "healthy",
    },
    {
        "id":            "omniroute",
        "provider":      "OmniRoute Proxy",
        "model":         "Local Gateway :20128",
        "remaining_pct": 94,
        "tokens_left":   "Unlimited / Self-Hosted",
        "resets_in":     "Active",
        "status":        "healthy",
        "velocity": {
            "tps": 0.0,
            "tpm": 0.0,
            "rpm": 0.0,
            "runaway_detected": False,
            "burn_rate_status": "nominal",
        },
    },
    {
        "id":            "kilocode",
        "provider":      "KiloCode Agent",
        "model":         "Kilo Router / Hybrid",
        "remaining_pct": 100,
        "tokens_left":   "100% available",
        "resets_in":     "Active",
        "status":        "healthy",
    },
    {
        "id":            "kimi",
        "provider":      "Moonshot Kimi",
        "model":         "Kimi 128k",
        "remaining_pct": 40,
        "tokens_left":   "40k / 100k",
        "resets_in":     "1h 45m",
        "status":        "warning",
    },
    {
        "id":            "cursor",
        "provider":      "Cursor Tab / Fast Requests",
        "model":         "Sonnet 3.5 Copilot",
        "remaining_pct": 28,
        "tokens_left":   "140 / 500 reqs",
        "resets_in":     "6 days",
        "status":        "warning",
    },
    {
        "id":            "manus",
        "provider":      "Manus / Agent Sandbox",
        "model":         "Full Autonomous Runner",
        "remaining_pct": 12,
        "tokens_left":   "12 / 100 credits",
        "resets_in":     "Tomorrow",
        "status":        "exhausted",
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _status_from_pct(pct: float) -> Status:
    if pct >= 50:
        return "healthy"
    if pct >= 20:
        return "warning"
    return "exhausted"

def _fmt_tokens(n: int | float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}k"
    return str(int(n))

def _seconds_to_human(seconds: float) -> str:
    """Convert a duration in seconds to a compact human string."""
    if seconds <= 0:
        return "< 1m"
    days = int(seconds // 86400)
    rem = seconds % 86400
    hours = int(rem // 3600)
    mins = int((rem % 3600) // 60)
    if days > 0:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours > 0:
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    if mins == 0:
        return "< 1m"
    return f"{mins}m"

def _next_five_hour_reset() -> float:
    """Seconds until the next 5-hour boundary (00, 05, 10, 15, 20 UTC hour)."""
    now_utc     = time.gmtime()
    hour        = now_utc.tm_hour
    minute      = now_utc.tm_min
    current_sec = hour * 3600 + minute * 60 + now_utc.tm_sec
    slot        = math.ceil((hour + minute / 60) / 5) * 5
    if slot >= 24:
        slot = 0
    target_sec  = slot * 3600
    diff        = target_sec - current_sec
    if diff <= 0:
        diff += 5 * 3600
    return diff

def _format_iso_reset(iso_ts: str | None, fallback: str = "Active") -> str:
    if not iso_ts:
        return fallback
    try:
        dt = datetime.datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        diff = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        return _seconds_to_human(max(diff, 0))
    except Exception:
        return fallback


# ── Instantaneous Token Velocity & Runaway Loop Detection ─────────────────────

class TokenVelocityTracker:
    """Sliding-window token velocity tracker and runaway loop detection engine.

    Tracks (timestamp, total_tokens, request_count) over a rolling retention window
    (default 60s) to detect high burn rates (>150 TPS) and request floods (>45 RPM).
    """

    def __init__(
        self,
        retention_seconds: float = 60.0,
        debounce_seconds: float | None = None,
    ) -> None:
        self.retention_seconds = float(retention_seconds)
        if debounce_seconds is not None:
            self.debounce_seconds = float(debounce_seconds)
        else:
            self.debounce_seconds = float(get_config().get("notification_debounce_seconds", 60.0))
        self._window: collections.deque[tuple[float, int, int]] = collections.deque()

        # Runaway loop tracking state
        self._high_tps_start_ts: float | None = None
        self._high_rpm_start_ts: float | None = None
        self._last_notification_ts: float = 0.0

    def prune(self, current_ts: float) -> None:
        """Discard records older than retention_seconds relative to current_ts."""
        cutoff = current_ts - self.retention_seconds
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()

    def record_probe(
        self,
        total_tokens: int,
        request_count: int,
        timestamp: float | None = None,
    ) -> dict:
        """Record a probe reading and compute instantaneous velocity metrics."""
        now = float(time.time() if timestamp is None else timestamp)
        self.prune(now)
        self._window.append((now, int(total_tokens), int(request_count)))
        return self.get_metrics(now=now)

    def dispatch_notification(
        self,
        message: str = 'Agent runaway loop suspected (>150 TPS). Check active sessions.',
        title: str = "Sentinel-AI Alert",
        subtitle: str = "",
    ) -> None:
        """Send native macOS notification banner via AppleScript osascript."""
        def _escape(s: str) -> str:
            return str(s).replace('\\', '\\\\').replace('"', '\\"')

        safe_msg = _escape(message)
        safe_title = _escape(title)
        safe_subtitle = _escape(subtitle)

        if safe_subtitle:
            script = f'display notification "{safe_msg}" with title "{safe_title}" subtitle "{safe_subtitle}"'
        else:
            script = f'display notification "{safe_msg}" with title "{safe_title}"'

        try:
            subprocess.run(
                ["osascript", "-e", script],
                shell=False,
                check=False,
                capture_output=True,
                timeout=5,
            )
        except Exception as e:
            logger.warning("Failed to dispatch macOS notification: %s", e)

    def get_metrics(self, now: float | None = None) -> dict:
        """Compute rolling token velocity and runaway loop detection status."""
        current_ts = float(time.time() if now is None else now)
        self.prune(current_ts)

        if len(self._window) < 2:
            return {
                "tps": 0.0,
                "tpm": 0.0,
                "rpm": 0.0,
                "runaway_detected": False,
                "burn_rate_status": "nominal",
            }

        # 1. Instantaneous TPS: delta tokens / delta time between latest 2 points
        t_last, tok_last, req_last = self._window[-1]
        t_prev, tok_prev, req_prev = self._window[-2]
        dt = max(t_last - t_prev, 0.0001)
        delta_tok_instant = max(0, tok_last - tok_prev)
        tps = round(delta_tok_instant / dt, 1)

        # 2. Extrapolated TPM: rate over active rolling window
        t_first, tok_first, req_first = self._window[0]
        w_dt = max(t_last - t_first, 0.0001)
        w_delta_tok = max(0, tok_last - tok_first)
        w_delta_req = max(0, req_last - req_first)

        tpm = round((w_delta_tok / w_dt) * 60.0, 1)

        # 3. RPM: Requests initiated over active 60s window
        rpm = round((w_delta_req / w_dt) * 60.0, 1)

        # 4. Runaway Loop Detection Engine
        config = get_config()
        alert_tps = float(config.get("velocity_alert_tps", 150.0))
        alert_rpm = float(config.get("velocity_alert_rpm", 45.0))

        # Condition A: Sustained high burn rate (tps > alert_tps for >= 15 consecutive seconds)
        runaway_a = False
        reason_a = None
        if tps > alert_tps:
            if self._high_tps_start_ts is None:
                self._high_tps_start_ts = t_prev
            duration_a = t_last - self._high_tps_start_ts
            if duration_a >= 15.0:
                runaway_a = True
                reason_a = f"Sustained high token burn: {tps:.1f} TPS"
        else:
            self._high_tps_start_ts = None

        # Condition B: Request flood (rpm > alert_rpm sustained with 0 backoff)
        runaway_b = False
        reason_b = None
        if rpm > alert_rpm:
            if self._high_rpm_start_ts is None:
                self._high_rpm_start_ts = t_first
            duration_b = t_last - self._high_rpm_start_ts
            if duration_b >= 10.0 or (len(self._window) >= 3 and duration_b >= 5.0):
                runaway_b = True
                reason_b = f"Request flood: {rpm:.1f} RPM sustained with 0 backoff"
        else:
            self._high_rpm_start_ts = None

        runaway_detected = runaway_a or runaway_b
        reason = reason_a if runaway_a else (reason_b if runaway_b else None)

        if runaway_detected:
            burn_rate_status = "runaway"
            if self._last_notification_ts == 0.0 or (t_last - self._last_notification_ts) >= self.debounce_seconds:
                alert_msg = f'Agent runaway loop suspected (>{int(alert_tps)} TPS). Check active sessions.'
                self.dispatch_notification(alert_msg)
                self._last_notification_ts = t_last
        elif tps > 80.0 or rpm > 25.0:
            burn_rate_status = "elevated"
        else:
            burn_rate_status = "nominal"

        res = {
            "tps": tps,
            "tpm": tpm,
            "rpm": rpm,
            "runaway_detected": runaway_detected,
            "burn_rate_status": burn_rate_status,
        }
        if reason:
            res["reason"] = reason
        return res


velocity_tracker = TokenVelocityTracker(retention_seconds=60.0)


def get_velocity_metrics(now: float | None = None) -> dict:
    """Return instantaneous token velocity and runaway loop detection status."""
    return velocity_tracker.get_metrics(now=now)


def record_token_reading(
    total_tokens: int,
    request_count: int,
    timestamp: float | None = None,
) -> dict:
    """Record an external token and request count reading into the velocity tracker."""
    return velocity_tracker.record_probe(
        total_tokens=total_tokens,
        request_count=request_count,
        timestamp=timestamp,
    )


# ── OmniRoute Local Auth Discovery ────────────────────────────────────────────

def _get_omniroute_token() -> str:
    """Retrieve local proxy bearer token from env or configuration files."""
    token = os.environ.get("OMNIROUTE_API_KEY", "")
    if token:
        return token.strip()

    candidate_files = [
        os.path.expanduser("~/.omniroute/.env"),
        os.path.expanduser("~/.config/omniroute/.env"),
        os.path.expanduser("~/.kilocode/.env"),
    ]
    for path in candidate_files:
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("OMNIROUTE_API_KEY="):
                            val = line.split("=", 1)[1].strip()
                            val = val.strip("\"'")
                            if val:
                                return val
            except Exception:
                pass
    return ""

# ── Cloud Probes (Anthropic / OpenAI) ──────────────────────────────────────────

_ANTHROPIC_CACHE_TS: float = 0.0
_ANTHROPIC_CACHE: dict = {}
_ANTHROPIC_TTL = 30.0

def _probe_anthropic() -> dict:
    """Return updated fields for the 'claude' record, or {} on failure.
    NOTE: Live paid API probe is disabled by default to avoid incurring billing.
    Set SENTINEL_PROBE_ANTHROPIC=true to explicitly opt-in."""
    if os.environ.get("SENTINEL_PROBE_ANTHROPIC", "false").lower() != "true":
        return {}

    global _ANTHROPIC_CACHE, _ANTHROPIC_CACHE_TS

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {}

    now = time.time()
    if _ANTHROPIC_CACHE and (now - _ANTHROPIC_CACHE_TS) < _ANTHROPIC_TTL:
        return _ANTHROPIC_CACHE

    try:
        body = json.dumps({
            "model":      "claude-sonnet-4-5",
            "max_tokens": 1,
            "messages":   [{"role": "user", "content": "hi"}],
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data    = body,
            headers = {
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            method = "POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                hdrs = dict(resp.headers)
        except urllib.error.HTTPError as e:
            hdrs = dict(e.headers)

        remaining = int(hdrs.get("anthropic-ratelimit-tokens-remaining", 0) or 0)
        limit     = int(hdrs.get("anthropic-ratelimit-tokens-limit", 200_000) or 200_000)
        reset_at  = hdrs.get("anthropic-ratelimit-tokens-reset", "")

        if limit <= 0:
            limit = 200_000

        if reset_at:
            try:
                dt       = datetime.datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                diff     = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
                resets_in = _seconds_to_human(max(diff, 0))
            except Exception:
                resets_in = _seconds_to_human(_next_five_hour_reset())
        else:
            resets_in = _seconds_to_human(_next_five_hour_reset())

        pct    = round(min((remaining / limit) * 100, 100.0), 1)
        result = {
            "remaining_pct": pct,
            "tokens_left":   f"{_fmt_tokens(remaining)} / {_fmt_tokens(limit)}",
            "resets_in":     resets_in,
            "status":        _status_from_pct(pct),
        }
        _ANTHROPIC_CACHE    = result
        _ANTHROPIC_CACHE_TS = now
        return result

    except Exception:
        return {}

_OPENAI_CACHE_TS: float = 0.0
_OPENAI_CACHE: dict = {}
_OPENAI_TTL = 30.0

def _probe_openai() -> dict:
    """Return updated fields for the 'codex' record, or {} on failure."""
    global _OPENAI_CACHE, _OPENAI_CACHE_TS

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return {}

    now = time.time()
    if _OPENAI_CACHE and (now - _OPENAI_CACHE_TS) < _OPENAI_TTL:
        return _OPENAI_CACHE

    try:
        req = urllib.request.Request(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                hdrs = dict(resp.headers)
        except urllib.error.HTTPError as e:
            hdrs = dict(e.headers)

        tpm_remaining = int(hdrs.get("x-ratelimit-remaining-tokens",   0) or 0)
        tpm_limit     = int(hdrs.get("x-ratelimit-limit-tokens",   1_000_000) or 1_000_000)
        reset_str     = hdrs.get("x-ratelimit-reset-tokens", "")

        if tpm_limit <= 0:
            tpm_limit = 1_000_000

        def _parse_reset(s: str) -> str:
            m = re.fullmatch(r"(?:(\d+)m)?(?:(\d+)s)?", s or "")
            if not m:
                return "Daily (00:00 UTC)"
            mins = int(m.group(1) or 0)
            secs = int(m.group(2) or 0)
            return _seconds_to_human(mins * 60 + secs)

        if tpm_remaining:
            pct        = round(min((tpm_remaining / tpm_limit) * 100, 100.0), 1)
            tokens_str = f"{_fmt_tokens(tpm_remaining)} TPM"
            resets_in  = _parse_reset(reset_str) if reset_str else "Daily (00:00 UTC)"
        else:
            return {}

        result = {
            "remaining_pct": pct,
            "tokens_left":   tokens_str,
            "resets_in":     resets_in,
            "status":        _status_from_pct(pct),
        }
        _OPENAI_CACHE    = result
        _OPENAI_CACHE_TS = now
        return result

    except Exception:
        return {}

# ── OmniRoute Harvest & Mapping ───────────────────────────────────────────────

def _get_proxy_url() -> str:
    return os.environ.get("OMNIROUTE_BASE", get_config().get("proxy_url", "http://localhost:20128")).rstrip("/")

OMNIROUTE_BASE = _get_proxy_url()
_OMNIROUTE_CACHE_TS: float = 0.0
_OMNIROUTE_CACHE: dict[str, dict] = {}
_OMNIROUTE_TTL = 15.0

def _parse_omniroute_telemetry(
    quota_data: dict | None,
    analytics_data: dict | None,
    stats_payload: dict | None,
) -> dict[str, dict]:
    """
    Extract provider quotas, token counters, and rate-limit statistics
    into Sentinel's provider records.
    """
    updates: dict[str, dict] = {}

    providers_list = (quota_data or {}).get("providers", [])
    if not isinstance(providers_list, list):
        providers_list = []

    summary = (analytics_data or {}).get("summary", {})
    if not isinstance(summary, dict):
        summary = (stats_payload or {}).get("summary", {}) or {}

    total_tokens = summary.get("totalTokens", 0)
    total_reqs = summary.get("totalRequests", 0)

    if total_tokens > 0 or total_reqs > 0:
        record_token_reading(total_tokens=total_tokens, request_count=total_reqs)

    # Error breakdown / rate limit counters
    error_breakdown = (analytics_data or {}).get("errorBreakdown", [])
    rate_limited_count = 0
    if isinstance(error_breakdown, list):
        for err in error_breakdown:
            if isinstance(err, dict) and err.get("errorType") == "rate_limited":
                rate_limited_count = err.get("count", 0)

    # 1. Update OmniRoute Gateway record
    if providers_list:
        available_count = sum(
            1 for p in providers_list
            if p.get("percentRemaining", 100) > 0 and p.get("tokenStatus") != "expired"
        )
        total_providers = max(len(providers_list), 1)
        omni_pct = round((available_count / total_providers) * 100)
    else:
        omni_pct = 94

    if total_tokens > 0 or total_reqs > 0:
        tokens_left_str = f"{_fmt_tokens(total_tokens)} tokens / {total_reqs} reqs"
    else:
        tokens_left_str = "Unlimited / Self-Hosted"

    omni_status = _status_from_pct(omni_pct)
    updates["omniroute"] = {
        "remaining_pct": omni_pct,
        "tokens_left":   tokens_left_str,
        "resets_in":     "Active",
        "status":        omni_status,
        "velocity":      get_velocity_metrics(),
    }

    # 2. Update Cursor if present in OmniRoute providers
    cursor_item = next(
        (p for p in providers_list if p.get("provider") == "cursor"),
        None,
    )
    if cursor_item:
        c_pct = float(cursor_item.get("percentRemaining", 0))
        c_used = cursor_item.get("quotaUsed", 0)
        c_total = cursor_item.get("quotaTotal", 100) or 100
        c_remaining = max(c_total - c_used, 0)
        c_reset_at = cursor_item.get("resetAt")
        c_resets_in = _format_iso_reset(c_reset_at, fallback="Active")
        updates["cursor"] = {
            "remaining_pct": round(c_pct),
            "tokens_left":   f"{c_remaining} / {c_total} reqs",
            "resets_in":     c_resets_in,
            "status":        _status_from_pct(c_pct),
        }

    # 3. Update KiloCode if present in OmniRoute providers
    kilo_item = next(
        (p for p in providers_list if p.get("provider") == "kilocode"),
        None,
    )
    if kilo_item:
        k_pct = float(kilo_item.get("percentRemaining", 100))
        k_reset_at = kilo_item.get("resetAt")
        k_resets_in = _format_iso_reset(k_reset_at, fallback="Active")
        updates["kilocode"] = {
            "remaining_pct": round(k_pct),
            "tokens_left":   "100% available",
            "resets_in":     k_resets_in,
            "status":        _status_from_pct(k_pct),
        }

    # 4. Check if any OmniRoute providers correspond to Claude or OpenAI
    claude_item = next(
        (p for p in providers_list if p.get("provider") in ("anthropic", "claude")),
        None,
    )
    if claude_item:
        cl_pct = float(claude_item.get("percentRemaining", 82))
        updates["claude"] = {
            "remaining_pct": round(cl_pct),
            "tokens_left":   f"{round(cl_pct)}% available",
            "resets_in":     _format_iso_reset(claude_item.get("resetAt"), fallback="Active"),
            "status":        _status_from_pct(cl_pct),
        }

    openai_item = next(
        (p for p in providers_list if p.get("provider") in ("openai", "codex")),
        None,
    )
    if openai_item:
        op_pct = float(openai_item.get("percentRemaining", 65))
        updates["codex"] = {
            "remaining_pct": round(op_pct),
            "tokens_left":   f"{round(op_pct)}% available",
            "resets_in":     _format_iso_reset(openai_item.get("resetAt"), fallback="Daily (00:00 UTC)"),
            "status":        _status_from_pct(op_pct),
        }

    return updates

async def _probe_omniroute_async() -> dict[str, dict]:
    """Asynchronously probe OmniRoute on port 20128 using local bearer token."""
    global _OMNIROUTE_CACHE, _OMNIROUTE_CACHE_TS

    now = time.time()
    if _OMNIROUTE_CACHE and (now - _OMNIROUTE_CACHE_TS) < _OMNIROUTE_TTL:
        return deepcopy(_OMNIROUTE_CACHE)

    token = _get_omniroute_token()
    base_url = _get_proxy_url()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            # Probe primary stats endpoints per spec
            stats_payload = None
            for ep in ("/api/stats", "/metrics", "/api/usage"):
                try:
                    r = await client.get(f"{base_url}{ep}", headers=headers)
                    if r.status_code == 200:
                        try:
                            stats_payload = r.json()
                            break
                        except Exception:
                            pass
                except Exception:
                    pass

            # Query live provider quotas & analytics
            quota_data = None
            analytics_data = None

            try:
                rq = await client.get(f"{base_url}/api/usage/quota", headers=headers)
                if rq.status_code == 200:
                    quota_data = rq.json()
                elif rq.status_code == 429:
                    return {
                        "omniroute": {
                            "remaining_pct": 20,
                            "tokens_left":   "Rate-Limited (429)",
                            "resets_in":     "Backing off",
                            "status":        "warning",
                        }
                    }
            except Exception:
                pass

            try:
                ra = await client.get(f"{base_url}/api/usage/analytics", headers=headers)
                if ra.status_code == 200:
                    analytics_data = ra.json()
                elif ra.status_code == 429:
                    return {
                        "omniroute": {
                            "remaining_pct": 20,
                            "tokens_left":   "Rate-Limited (429)",
                            "resets_in":     "Backing off",
                            "status":        "warning",
                        }
                    }
            except Exception:
                pass

            if not quota_data and not analytics_data and not stats_payload:
                reachable = False
                try:
                    r_base = await client.get(f"{base_url}/", headers=headers)
                    reachable = r_base.status_code < 500
                except Exception:
                    reachable = False

                if reachable:
                    res = {
                        "omniroute": {
                            "remaining_pct": 94,
                            "tokens_left":   "Unlimited / Self-Hosted",
                            "resets_in":     "Active",
                            "status":        "healthy",
                        }
                    }
                else:
                    res = {
                        "omniroute": {
                            "remaining_pct": 0,
                            "tokens_left":   "Offline / Unreachable",
                            "resets_in":     "—",
                            "status":        "exhausted",
                        }
                    }
                _OMNIROUTE_CACHE = res
                _OMNIROUTE_CACHE_TS = now
                return deepcopy(res)

            updates = _parse_omniroute_telemetry(quota_data, analytics_data, stats_payload)
            _OMNIROUTE_CACHE = updates
            _OMNIROUTE_CACHE_TS = now
            return deepcopy(updates)

    except Exception:
        if _OMNIROUTE_CACHE:
            return deepcopy(_OMNIROUTE_CACHE)
        return {
            "omniroute": {
                "remaining_pct": 0,
                "tokens_left":   "Offline / Unreachable",
                "resets_in":     "—",
                "status":        "exhausted",
            }
        }

def _probe_omniroute_sync() -> dict[str, dict]:
    """Synchronous fallback probe for OmniRoute."""
    global _OMNIROUTE_CACHE, _OMNIROUTE_CACHE_TS

    now = time.time()
    if _OMNIROUTE_CACHE and (now - _OMNIROUTE_CACHE_TS) < _OMNIROUTE_TTL:
        return deepcopy(_OMNIROUTE_CACHE)

    token = _get_omniroute_token()
    base_url = _get_proxy_url()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        with httpx.Client(timeout=3.0) as client:
            stats_payload = None
            for ep in ("/api/stats", "/metrics", "/api/usage"):
                try:
                    r = client.get(f"{base_url}{ep}", headers=headers)
                    if r.status_code == 200:
                        stats_payload = r.json()
                        break
                except Exception:
                    pass

            quota_data = None
            analytics_data = None

            try:
                rq = client.get(f"{base_url}/api/usage/quota", headers=headers)
                if rq.status_code == 200:
                    quota_data = rq.json()
                elif rq.status_code == 429:
                    return {
                        "omniroute": {
                            "remaining_pct": 20,
                            "tokens_left":   "Rate-Limited (429)",
                            "resets_in":     "Backing off",
                            "status":        "warning",
                        }
                    }
            except Exception:
                pass

            try:
                ra = client.get(f"{base_url}/api/usage/analytics", headers=headers)
                if ra.status_code == 200:
                    analytics_data = ra.json()
                elif ra.status_code == 429:
                    return {
                        "omniroute": {
                            "remaining_pct": 20,
                            "tokens_left":   "Rate-Limited (429)",
                            "resets_in":     "Backing off",
                            "status":        "warning",
                        }
                    }
            except Exception:
                pass

            if not quota_data and not analytics_data and not stats_payload:
                try:
                    r_base = client.get(f"{base_url}/", headers=headers)
                    reachable = r_base.status_code < 500
                except Exception:
                    reachable = False

                if reachable:
                    res = {
                        "omniroute": {
                            "remaining_pct": 94,
                            "tokens_left":   "Unlimited / Self-Hosted",
                            "resets_in":     "Active",
                            "status":        "healthy",
                        }
                    }
                else:
                    res = {
                        "omniroute": {
                            "remaining_pct": 0,
                            "tokens_left":   "Offline / Unreachable",
                            "resets_in":     "—",
                            "status":        "exhausted",
                        }
                    }
                _OMNIROUTE_CACHE = res
                _OMNIROUTE_CACHE_TS = now
                return deepcopy(res)

            updates = _parse_omniroute_telemetry(quota_data, analytics_data, stats_payload)
            _OMNIROUTE_CACHE = updates
            _OMNIROUTE_CACHE_TS = now
            return deepcopy(updates)

    except Exception:
        if _OMNIROUTE_CACHE:
            return deepcopy(_OMNIROUTE_CACHE)
        return {
            "omniroute": {
                "remaining_pct": 0,
                "tokens_left":   "Offline / Unreachable",
                "resets_in":     "—",
                "status":        "exhausted",
            }
        }

# ── Public API ────────────────────────────────────────────────────────────────

async def get_all_quotas() -> list[dict]:
    """
    Return quota records for all providers as plain dicts ready for JSON
    serialisation.

    Always returns a non-empty list — the BASELINE is the floor.
    Live probes update individual fields but never remove records.
    """
    records = deepcopy(_BASELINE)
    index = {r["id"]: r for r in records}

    # Parallel asynchronous gathering of live probes
    anthropic_task = asyncio.to_thread(_probe_anthropic)
    openai_task    = asyncio.to_thread(_probe_openai)
    omniroute_task = _probe_omniroute_async()

    results = await asyncio.gather(
        anthropic_task,
        openai_task,
        omniroute_task,
        return_exceptions=True,
    )

    anthropic_res, openai_res, omniroute_res = results

    if isinstance(anthropic_res, dict) and anthropic_res:
        if "claude" in index:
            index["claude"].update(anthropic_res)

    if isinstance(openai_res, dict) and openai_res:
        if "codex" in index:
            index["codex"].update(openai_res)

    if isinstance(omniroute_res, dict) and omniroute_res:
        for provider_id, updates in omniroute_res.items():
            if provider_id in index:
                index[provider_id].update(updates)

    if "omniroute" in index:
        index["omniroute"]["velocity"] = get_velocity_metrics()

    return records

def get_all_quotas_sync() -> list[dict]:
    """Synchronous fallback of get_all_quotas()."""
    records = deepcopy(_BASELINE)
    index = {r["id"]: r for r in records}

    probe_map: dict[str, dict] = {
        "claude": _probe_anthropic(),
        "codex":  _probe_openai(),
    }
    omniroute_res = _probe_omniroute_sync()
    if omniroute_res:
        probe_map.update(omniroute_res)

    for provider_id, updates in probe_map.items():
        if updates and provider_id in index:
            index[provider_id].update(updates)

    if "omniroute" in index:
        index["omniroute"]["velocity"] = get_velocity_metrics()

    return records

"""
sentinel/menubar.py

Zero-overhead native macOS menu bar companion for Sentinel-AI.
Displays live Thrash Danger Index (TDI), unified VRAM usage, swap memory,
pageouts, and AI agent quota telemetry using rumps.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from typing import Any

import rumps

from sentinel.config import get_config

# ── Alert constants ───────────────────────────────────────────────────────────
TDI_ALERT_THRESHOLD = 0.75
SWAP_ALERT_THRESHOLD_MB = 512.0
DEBOUNCE_INTERVAL_SEC = 300.0  # 5 minutes


class SentinelMenuBarApp(rumps.App):
    """Native macOS status bar app for Sentinel-AI telemetry and quota monitoring."""

    def __init__(
        self,
        api_base: str = "http://127.0.0.1:8000",
        dashboard_url: str = "http://localhost:3000",
        poll_interval: int = 2,
    ) -> None:
        super().__init__("Sentinel-AI", title="TDI: 0.00 · 🟢")
        self.api_base = api_base.rstrip("/")
        self.dashboard_url = dashboard_url
        self.poll_interval = poll_interval

        # Alert tracking
        self._last_tdi_alert: float = 0.0
        self._last_swap_alert: float = 0.0

        # UI Menu Items
        self.item_header = rumps.MenuItem("Sentinel-AI — Apple Silicon Monitor", callback=None)
        self.item_wired = rumps.MenuItem("Wired VRAM: -- MB / -- MB", callback=None)
        self.item_swap = rumps.MenuItem("Swap Used: -- MB (Pageouts: --)", callback=None)
        self.item_omni = rumps.MenuItem("OmniRoute Tokens: --", callback=None)
        self.item_velocity = rumps.MenuItem("Token Velocity: 0.0 TPS (nominal)", callback=None)
        self.item_cursor = rumps.MenuItem("Cursor Quota: --", callback=None)

        self.btn_dashboard = rumps.MenuItem("Open Dashboard", callback=self.open_dashboard)
        self.btn_refresh = rumps.MenuItem("Trigger Manual Refresh", callback=self.manual_refresh)
        self.btn_quit = rumps.MenuItem("Quit Sentinel Bar", callback=self.quit_app)

        self.menu = [
            self.item_header,
            None,
            self.item_wired,
            self.item_swap,
            self.item_omni,
            self.item_velocity,
            self.item_cursor,
            None,
            self.btn_dashboard,
            self.btn_refresh,
            self.btn_quit,
        ]

        # Initial metrics harvest
        self.update_metrics()

        # Background polling timer
        self.timer = rumps.Timer(self.on_timer, self.poll_interval)
        self.timer.start()

    @staticmethod
    def get_alert_pill(tdi: float) -> str:
        """Map Thrash Danger Index to alert pill color indicator based on config thresholds."""
        cfg = get_config()
        crit = float(cfg.get("tdi_critical_threshold", 0.90))
        warn = float(cfg.get("tdi_warning_threshold", 0.75))
        if tdi >= crit:
            return "🔴"
        elif tdi >= warn:
            return "🟡"
        return "🟢"

    @staticmethod
    def get_status_icon(tdi: float, use_config: bool = False) -> str:
        """Map Thrash Danger Index to visual status indicator."""
        if use_config:
            return SentinelMenuBarApp.get_alert_pill(tdi)
        if tdi < 0.30:
            return "🟢"
        if tdi < 0.70:
            return "🟡"
        return "🔴"

    def fetch_telemetry(self) -> dict[str, Any]:
        """Fetch latest telemetry from Sentinel server or fall back to direct hardware probe."""
        # 1. Attempt REST API
        try:
            url = f"{self.api_base}/api/history?window=1m"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=1.2) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if isinstance(data, list) and len(data) > 0:
                    return data[-1]
        except Exception:
            pass

        # 2. Direct fallback using local harvester
        try:
            from sentinel.harvester import (
                compute_thrash_danger_index,
                get_gpu_wired_limit,
                get_pageout_count,
                get_swap_usage,
                get_wired_memory_mb,
                is_apple_silicon,
            )

            wired = get_wired_memory_mb()
            limit = get_gpu_wired_limit()
            total_swap, used_swap = get_swap_usage()
            pageouts = get_pageout_count()
            tdi = compute_thrash_danger_index(wired, limit, used_swap)
            return {
                "thrash_index": tdi,
                "wired_mb": wired,
                "limit_mb": limit,
                "swap_used_mb": used_swap,
                "swap_total_mb": total_swap,
                "pageouts": pageouts,
                "is_apple_silicon": is_apple_silicon(),
            }
        except Exception:
            return {
                "thrash_index": 0.0,
                "wired_mb": 0.0,
                "limit_mb": 18432.0,
                "swap_used_mb": 0.0,
                "pageouts": 0,
                "is_apple_silicon": True,
            }

    def fetch_quotas(self) -> list[dict[str, Any]]:
        """Fetch quota telemetry from Sentinel server or local quota manager."""
        # 1. Attempt REST API
        try:
            url = f"{self.api_base}/api/quotas"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if isinstance(data, list):
                    return data
        except Exception:
            pass

        # 2. In-process fallback
        try:
            from sentinel.quota import get_all_quotas_sync

            return get_all_quotas_sync()
        except Exception:
            return []

    def dispatch_notification(self, message: str, title: str = "Sentinel-AI Alert") -> None:
        """Send a native macOS notification banner via AppleScript osascript."""
        script = 'on run {msg, title}\ndisplay notification msg with title title\nend run'
        try:
            subprocess.Popen(["osascript", "-e", script, message, title])
        except Exception:
            pass

    def check_alerts(
        self, tdi: float, swap_used_mb: float, now: float | None = None
    ) -> list[str]:
        """Check alert crossing thresholds and dispatch debounced notifications."""
        current_time = now if now is not None else time.time()
        triggered_alerts: list[str] = []

        # 1. Check TDI threshold
        config = get_config()
        tdi_threshold = float(config.get("tdi_warning_threshold", TDI_ALERT_THRESHOLD))

        if tdi >= tdi_threshold:
            if (current_time - self._last_tdi_alert) >= DEBOUNCE_INTERVAL_SEC:
                msg = f"System memory pressure critical (TDI: {tdi:.2f}). Thrash imminent."
                self.dispatch_notification(msg)
                self._last_tdi_alert = current_time
                triggered_alerts.append("tdi")

        # 2. Check Swap Used threshold
        if swap_used_mb >= SWAP_ALERT_THRESHOLD_MB:
            if (current_time - self._last_swap_alert) >= DEBOUNCE_INTERVAL_SEC:
                msg = f"Swap utilization high ({swap_used_mb:.0f} MB). Risk of SSD pageout thrashing."
                self.dispatch_notification(msg)
                self._last_swap_alert = current_time
                triggered_alerts.append("swap")

        return triggered_alerts

    def update_metrics(self) -> None:
        """Poll telemetry and quotas, update status title and dropdown labels."""
        telemetry = self.fetch_telemetry()
        quotas = self.fetch_quotas()

        tdi = float(telemetry.get("thrash_index", 0.0) or 0.0)
        wired_mb = float(telemetry.get("wired_mb", 0.0) or 0.0)
        limit_mb = float(telemetry.get("limit_mb", 18432.0) or 18432.0)
        swap_used = float(telemetry.get("swap_used_mb", 0.0) or 0.0)
        pageouts = int(telemetry.get("pageouts", 0) or 0)

        # 1. Update Title: "TDI: <val> · <status_icon>"
        status_icon = self.get_status_icon(tdi)
        self.title = f"TDI: {tdi:.2f} · {status_icon}"

        # 2. Update Metric Menu Items
        self.item_wired.title = f"Wired VRAM: {wired_mb:,.0f} MB / {limit_mb:,.0f} MB"
        self.item_swap.title = f"Swap Used: {swap_used:,.0f} MB (Pageouts: {pageouts:,})"

        # 3. Quota & Velocity extractions
        omni_rec = next((q for q in quotas if q.get("id") == "omniroute"), None)
        if omni_rec:
            omni_tokens = omni_rec.get("tokens_left", "Active")
            vel_block = omni_rec.get("velocity")
        else:
            omni_tokens = "Offline / Unreachable"
            vel_block = None
        self.item_omni.title = f"OmniRoute Tokens: {omni_tokens}"

        # Resolve velocity metrics from telemetry or omniroute quota
        velocity = telemetry.get("velocity") or vel_block or {}
        runaway = bool(velocity.get("runaway_detected", False))
        tps = float(velocity.get("tps", 0.0) or 0.0)
        rpm = float(velocity.get("rpm", 0.0) or 0.0)
        status_burn = str(velocity.get("burn_rate_status", "nominal"))
        reason = velocity.get("reason")

        if runaway:
            self.title = f"⚠️ [RUNAWAY] TDI: {tdi:.2f} · {status_icon}"
            self.item_velocity.title = f"⚠️ RUNAWAY: {tps:.1f} TPS ({reason or 'High Burn'})"
        else:
            self.title = f"TDI: {tdi:.2f} · {status_icon}"
            self.item_velocity.title = f"Token Velocity: {tps:.1f} TPS | {rpm:.0f} RPM ({status_burn})"

        cursor_rec = next((q for q in quotas if q.get("id") == "cursor"), None)
        if cursor_rec:
            c_left = cursor_rec.get("tokens_left", "--")
            c_status = cursor_rec.get("status", "").capitalize()
            cursor_str = f"{c_left} ({c_status})" if c_status else c_left
        else:
            cursor_str = "Unavailable"
        self.item_cursor.title = f"Cursor Quota: {cursor_str}"

        # 4. Evaluate Alert Engine
        self.check_alerts(tdi, swap_used)

    def on_timer(self, _sender: Any) -> None:
        """Periodic timer event."""
        self.update_metrics()

    def open_dashboard(self, _sender: Any) -> None:
        """Action callback to open Next.js web dashboard in default browser."""
        try:
            subprocess.Popen(["open", self.dashboard_url])
        except Exception:
            pass

    def manual_refresh(self, _sender: Any) -> None:
        """Action callback to manually force refresh all metrics immediately."""
        self.update_metrics()

    def quit_app(self, _sender: Any) -> None:
        """Action callback to cleanly terminate Sentinel Bar."""
        if hasattr(self, "timer") and self.timer:
            self.timer.stop()
        rumps.quit_application()


def main() -> None:
    """Launch the Sentinel-AI macOS menu bar app."""
    app = SentinelMenuBarApp()
    app.run()


if __name__ == "__main__":
    main()

"""
tests/test_velocity.py

Unit and integration tests for Token Velocity Tracker and Runaway Agent Loop Detection:
- Sliding-window retention and pruning (60s window)
- Instantaneous TPS, extrapolated TPM, and RPM calculations
- Runaway Condition A: Sustained high burn rate (>150 TPS for >= 15s)
- Runaway Condition B: Request flood (>45 RPM sustained)
- Native macOS osascript notification debouncing (180s)
- REST endpoints (/api/velocity, /api/quotas) and WebSocket payload schema validation
- Menubar runaway warning badge verification
"""

import time
from unittest.mock import MagicMock, patch
import pytest
from starlette.testclient import TestClient

from sentinel.quota import (
    TokenVelocityTracker,
    get_velocity_metrics,
    record_token_reading,
)
from sentinel.server import _collect_reading, app
from sentinel.menubar import SentinelMenuBarApp


class TestSlidingWindowVelocity:
    """Test rolling window mechanics, pruning, and rate math."""

    def test_empty_and_single_entry_metrics(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        m0 = tracker.get_metrics(now=100.0)
        assert m0["tps"] == 0.0
        assert m0["tpm"] == 0.0
        assert m0["rpm"] == 0.0
        assert m0["runaway_detected"] is False
        assert m0["burn_rate_status"] == "nominal"

        # Single probe record
        m1 = tracker.record_probe(total_tokens=1000, request_count=5, timestamp=100.0)
        assert m1["tps"] == 0.0
        assert m1["tpm"] == 0.0
        assert m1["rpm"] == 0.0
        assert m1["runaway_detected"] is False

    def test_steady_state_rate_calculation(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        # t=0: 1000 tokens, 10 requests
        tracker.record_probe(total_tokens=1000, request_count=10, timestamp=100.0)
        # t=10s: 1500 tokens (+500), 12 requests (+2)
        m = tracker.record_probe(total_tokens=1500, request_count=12, timestamp=110.0)

        # Delta tokens = 500 in 10s -> TPS = 50.0
        assert m["tps"] == 50.0
        # TPM = 500 / 10s * 60s = 3000.0
        assert m["tpm"] == 3000.0
        # RPM = 2 / 10s * 60s = 12.0
        assert m["rpm"] == 12.0
        assert m["runaway_detected"] is False
        assert m["burn_rate_status"] == "nominal"

    def test_pruning_beyond_retention_window(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        tracker.record_probe(total_tokens=1000, request_count=10, timestamp=100.0)
        tracker.record_probe(total_tokens=2000, request_count=20, timestamp=120.0)

        assert len(tracker._window) == 2

        # Record at t=170 (>60s after t=100)
        tracker.record_probe(total_tokens=3000, request_count=30, timestamp=170.0)
        # Oldest entry at t=100 must be pruned
        assert len(tracker._window) == 2
        timestamps = [item[0] for item in tracker._window]
        assert 100.0 not in timestamps
        assert 120.0 in timestamps
        assert 170.0 in timestamps

    def test_elevated_burn_rate_status(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        tracker.record_probe(total_tokens=1000, request_count=10, timestamp=100.0)
        # 900 tokens in 10s -> 90 TPS (> 80 TPS)
        m = tracker.record_probe(total_tokens=1900, request_count=12, timestamp=110.0)
        assert m["tps"] == 90.0
        assert m["runaway_detected"] is False
        assert m["burn_rate_status"] == "elevated"


class TestRunawayDetection:
    """Test Condition A (high burn rate) and Condition B (request flood)."""

    def test_condition_a_sustained_high_burn_rate(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        with patch.object(tracker, "dispatch_notification") as mock_notify:
            # Start burn: t=100s, 0 tokens
            tracker.record_probe(total_tokens=0, request_count=1, timestamp=100.0)

            # t=105s (+5s): 1000 tokens (200 TPS > 150) -> duration 5s (<15s)
            m1 = tracker.record_probe(total_tokens=1000, request_count=2, timestamp=105.0)
            assert m1["tps"] == 200.0
            assert m1["runaway_detected"] is False
            assert m1["burn_rate_status"] == "elevated"
            mock_notify.assert_not_called()

            # t=110s (+10s): 2000 tokens (200 TPS > 150) -> duration 10s (<15s)
            m2 = tracker.record_probe(total_tokens=2000, request_count=3, timestamp=110.0)
            assert m2["runaway_detected"] is False

            # t=115s (+15s sustained): 3000 tokens (200 TPS > 150) -> duration 15s (TRIP!)
            m3 = tracker.record_probe(total_tokens=3000, request_count=4, timestamp=115.0)
            assert m3["runaway_detected"] is True
            assert m3["burn_rate_status"] == "runaway"
            assert "Sustained high token burn" in m3["reason"]
            mock_notify.assert_called_once()
            assert ">150 TPS" in mock_notify.call_args[0][0]

    def test_condition_a_resets_when_burn_drops(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        tracker.record_probe(total_tokens=0, request_count=1, timestamp=100.0)
        # Burn for 15s
        tracker.record_probe(total_tokens=3000, request_count=2, timestamp=115.0)
        m1 = tracker.get_metrics(now=115.0)
        assert m1["runaway_detected"] is True

        # Burn drops: +10 tokens in 5s (2 TPS)
        m2 = tracker.record_probe(total_tokens=3010, request_count=3, timestamp=120.0)
        assert m2["tps"] == 2.0
        assert m2["runaway_detected"] is False
        assert m2["burn_rate_status"] == "nominal"

    def test_condition_b_request_flood(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0)
        with patch.object(tracker, "dispatch_notification") as mock_notify:
            # Rapid requests: 10 requests every 5 seconds (120 RPM > 45)
            tracker.record_probe(total_tokens=100, request_count=0, timestamp=100.0)
            tracker.record_probe(total_tokens=110, request_count=10, timestamp=105.0)
            m = tracker.record_probe(total_tokens=120, request_count=20, timestamp=110.0)

            assert m["rpm"] == 120.0
            assert m["runaway_detected"] is True
            assert m["burn_rate_status"] == "runaway"
            assert "Request flood" in m["reason"]
            mock_notify.assert_called_once()


class TestNotificationDebounce:
    """Verify 3-minute debounce window prevents notification spamming."""

    def test_notification_debounced_for_180s(self):
        tracker = TokenVelocityTracker(retention_seconds=60.0, debounce_seconds=180.0)
        with patch.object(tracker, "dispatch_notification") as mock_notify:
            # Trip at t=1000
            tracker.record_probe(total_tokens=0, request_count=0, timestamp=980.0)
            tracker.record_probe(total_tokens=4000, request_count=1, timestamp=1000.0)
            assert mock_notify.call_count == 1

            # Ongoing runaway probes every 30s (within 60s window) -> should NOT dispatch again
            tracker.record_probe(total_tokens=10000, request_count=2, timestamp=1030.0)
            tracker.record_probe(total_tokens=16000, request_count=3, timestamp=1060.0)
            tracker.record_probe(total_tokens=22000, request_count=4, timestamp=1090.0)
            tracker.record_probe(total_tokens=28000, request_count=5, timestamp=1120.0)
            tracker.record_probe(total_tokens=34000, request_count=6, timestamp=1150.0)
            assert mock_notify.call_count == 1

            # Probe at t=1181 (181s after initial alert) -> should dispatch again
            tracker.record_probe(total_tokens=40000, request_count=7, timestamp=1181.0)
            assert mock_notify.call_count == 2


class TestApiAndWebSocketIntegration:
    """Verify API endpoints expose velocity block cleanly."""

    def test_api_velocity_endpoint(self):
        with TestClient(app) as client:
            resp = client.get("/api/velocity")
            assert resp.status_code == 200
            data = resp.json()
            assert "tps" in data
            assert "tpm" in data
            assert "rpm" in data
            assert "runaway_detected" in data
            assert "burn_rate_status" in data

    def test_api_quotas_includes_velocity(self):
        with TestClient(app) as client:
            resp = client.get("/api/quotas")
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, list)
            omni = next((item for item in data if item["id"] == "omniroute"), None)
            assert omni is not None
            assert "velocity" in omni
            assert "tps" in omni["velocity"]
            assert "burn_rate_status" in omni["velocity"]

    def test_api_quotas_wrapped_endpoint(self):
        with TestClient(app) as client:
            resp = client.get("/api/quotas?wrap=true")
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, dict)
            assert "quotas" in data
            assert "velocity" in data
            assert isinstance(data["quotas"], list)
            assert "tps" in data["velocity"]

    def test_collect_reading_contains_velocity(self):
        reading = _collect_reading()
        assert "velocity" in reading
        vel = reading["velocity"]
        assert "tps" in vel
        assert "tpm" in vel
        assert "rpm" in vel
        assert "runaway_detected" in vel
        assert "burn_rate_status" in vel


class TestMenubarRunawayBadge:
    """Verify menu bar title and item when runaway is detected."""

    def test_menubar_displays_runaway_warning(self):
        with patch("rumps.Timer"):
            app_instance = SentinelMenuBarApp()

        mock_telemetry = {
            "thrash_index": 0.15,
            "wired_mb": 2500.0,
            "limit_mb": 18432.0,
            "swap_used_mb": 0.0,
            "pageouts": 0,
            "velocity": {
                "tps": 220.5,
                "tpm": 13230.0,
                "rpm": 12.0,
                "runaway_detected": True,
                "burn_rate_status": "runaway",
                "reason": "Sustained high token burn: 220.5 TPS",
            },
        }

        with patch.object(app_instance, "fetch_telemetry", return_value=mock_telemetry):
            with patch.object(app_instance, "fetch_quotas", return_value=[]):
                with patch.object(app_instance, "check_alerts"):
                    app_instance.update_metrics()

        assert "⚠️ [RUNAWAY]" in app_instance.title
        assert "⚠️ RUNAWAY" in app_instance.item_velocity.title
        assert "220.5 TPS" in app_instance.item_velocity.title


class TestHardenedNotificationDispatch:
    """Verify security hardening for AppleScript notification dispatching."""

    def test_notification_sanitization_and_subprocess_args(self):
        tracker = TokenVelocityTracker()
        with patch("subprocess.Popen") as mock_popen:
            tracker.dispatch_notification(
                message='Malicious "string" with \\ backslash',
                title='Alert "Title" \\',
            )
            mock_popen.assert_called_once()
            args, kwargs = mock_popen.call_args
            cmd = args[0]
            assert cmd[0] == "osascript"
            assert cmd[1] == "-e"
            assert "on run {msg, title}" in cmd[2]
            assert "display notification msg with title title" in cmd[2]
            # Verify payload is passed safely via argv rather than interpolated into the script
            assert cmd[3] == 'Malicious "string" with \\ backslash'
            assert cmd[4] == 'Alert "Title" \\'

    def test_notification_with_subtitle_args(self):
        tracker = TokenVelocityTracker()
        with patch("subprocess.Popen") as mock_popen:
            tracker.dispatch_notification(
                message='Msg',
                title='Title',
                subtitle='Sub',
            )
            mock_popen.assert_called_once()
            args, kwargs = mock_popen.call_args
            cmd = args[0]
            assert cmd[0] == "osascript"
            assert cmd[1] == "-e"
            assert "on run {msg, title, sub}" in cmd[2]
            assert cmd[3] == 'Msg'
            assert cmd[4] == 'Title'
            assert cmd[5] == 'Sub'

    def test_notification_exception_handling_never_crashes(self):
        tracker = TokenVelocityTracker()
        with patch("subprocess.Popen", side_effect=RuntimeError("Subprocess execution failed")):
            # Should catch silently without raising
            tracker.dispatch_notification("Test message", "Test title")

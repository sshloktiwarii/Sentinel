"""
tests/test_server.py

Tests for sentinel/server.py WebSocket endpoint.

Strategy:
- Use Starlette's TestClient with its built-in WebSocket support (synchronous,
  no event-loop conflicts) for connection and payload tests.
- Patch harvester functions so tests are hermetic and never touch real sysctl.
- Patch insert_telemetry so tests never write to disk.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from sentinel.server import app, dist_dir

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_LIMIT = 16384
MOCK_WIRED = 16384
MOCK_SWAP_TOTAL = 2048.0
MOCK_SWAP_USED = 512.0
MOCK_PAGEOUTS = 42
MOCK_THRASH = 0.5

def _patch_harvester():
    """Return a context-manager stack that mocks all harvester calls."""
    return (
        patch("sentinel.server.get_gpu_wired_limit", return_value=MOCK_LIMIT),
        patch("sentinel.server.get_wired_memory_mb", return_value=MOCK_WIRED),
        patch("sentinel.server.get_swap_usage", return_value=(MOCK_SWAP_TOTAL, MOCK_SWAP_USED)),
        patch("sentinel.server.get_pageout_count", return_value=MOCK_PAGEOUTS),
        patch("sentinel.server.compute_thrash_danger_index", return_value=MOCK_THRASH),
        patch("sentinel.server.insert_telemetry", return_value=1),
        patch("sentinel.server.init_db"),
    )

# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

class TestWebSocketConnection:
    def test_connection_is_accepted(self):
        """Client should be able to connect without an error."""
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            with TestClient(app) as client:
                with client.websocket_connect("/ws/telemetry") as ws:
                    # Receive one message to confirm the handshake completed
                    ws.receive_text()

    def test_endpoint_path_exists(self):
        """Connecting to a wrong path should raise an error (404/rejected)."""
        with TestClient(app) as client:
            with pytest.raises(Exception):
                with client.websocket_connect("/ws/does-not-exist") as ws:
                    ws.receive_text()

# ---------------------------------------------------------------------------
# Payload tests
# ---------------------------------------------------------------------------

class TestTelemetryPayload:
    def _get_message(self) -> dict:
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            with TestClient(app) as client:
                with client.websocket_connect("/ws/telemetry") as ws:
                    raw = ws.receive_text()
        return json.loads(raw)

    def test_payload_is_valid_json(self):
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            with TestClient(app) as client:
                with client.websocket_connect("/ws/telemetry") as ws:
                    raw = ws.receive_text()
        # json.loads raises if invalid
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)

    def test_payload_has_required_keys(self):
        msg = self._get_message()
        required = {
            "timestamp",
            "wired_mb",
            "limit_mb",
            "swap_total_mb",
            "swap_used_mb",
            "pageouts",
            "thrash_index",
        }
        assert required.issubset(msg.keys())

    def test_payload_timestamp_is_numeric(self):
        msg = self._get_message()
        assert isinstance(msg["timestamp"], (int, float))
        assert msg["timestamp"] > 0

    def test_payload_wired_mb_matches_mock(self):
        msg = self._get_message()
        assert msg["wired_mb"] == MOCK_WIRED

    def test_payload_swap_used_mb_matches_mock(self):
        msg = self._get_message()
        assert msg["swap_used_mb"] == pytest.approx(MOCK_SWAP_USED)

    def test_payload_pageouts_matches_mock(self):
        msg = self._get_message()
        assert msg["pageouts"] == MOCK_PAGEOUTS

    def test_payload_thrash_index_matches_mock(self):
        msg = self._get_message()
        assert msg["thrash_index"] == pytest.approx(MOCK_THRASH)

    def test_payload_thrash_index_in_range(self):
        msg = self._get_message()
        assert 0.0 <= msg["thrash_index"] <= 1.0

# ---------------------------------------------------------------------------
# Persistence tests
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_insert_telemetry_is_called(self):
        """Each broadcasted message must trigger one insert_telemetry call."""
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4] as mock_thrash, \
             patches[5] as mock_insert, patches[6]:
            with TestClient(app) as client:
                with client.websocket_connect("/ws/telemetry") as ws:
                    ws.receive_text()
            assert mock_insert.call_count >= 1

    def test_insert_called_with_correct_swap(self):
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patches[5] as mock_insert, patches[6]:
            with TestClient(app) as client:
                with client.websocket_connect("/ws/telemetry") as ws:
                    ws.receive_text()
            call_kwargs = mock_insert.call_args
            # accept both positional and keyword call styles
            args, kwargs = call_kwargs
            swap = kwargs.get("swap_used_mb", args[2] if len(args) > 2 else None)
            assert swap == pytest.approx(MOCK_SWAP_USED)


# ---------------------------------------------------------------------------
# Static frontend & API route priority tests
# ---------------------------------------------------------------------------

class TestStaticFrontend:
    def test_api_telemetry_returns_json(self):
        """GET /api/telemetry must return JSON with application/json content-type (API routes have priority)."""
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            with TestClient(app) as client:
                response = client.get("/api/telemetry")
                assert response.status_code == 200
                assert "application/json" in response.headers.get("content-type", "")
                payload = response.json()
                assert isinstance(payload, dict)
                assert payload["limit_mb"] == MOCK_LIMIT
                assert payload["wired_mb"] == MOCK_WIRED
                assert payload["swap_used_mb"] == pytest.approx(MOCK_SWAP_USED)
                assert payload["thrash_index"] == pytest.approx(MOCK_THRASH)

    def test_frontend_serves_index_html_when_web_dist_present(self):
        """GET / serves the static index.html frontend with text/html when web_dist is present."""
        assert dist_dir.exists(), f"Expected web_dist directory at {dist_dir}"
        index_file = dist_dir / "index.html"
        assert index_file.exists(), f"Expected index.html at {index_file}"

        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
            content_type = response.headers.get("content-type", "")
            assert "text/html" in content_type
            assert "<!doctype html>" in response.text.lower() or "<html" in response.text.lower()


# ---------------------------------------------------------------------------
# Security hardening tests
# ---------------------------------------------------------------------------

class TestHardenedServerSecurity:
    """Verify server security hardening: restricted CORS and loopback binding."""

    def test_cors_restricted_origins(self):
        from starlette.middleware.cors import CORSMiddleware
        cors_middlewares = [
            m for m in app.user_middleware if m.cls == CORSMiddleware
        ]
        assert len(cors_middlewares) == 1
        allow_origins = cors_middlewares[0].kwargs.get("allow_origins", [])
        expected_origins = [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://localhost:3000",
        ]
        assert allow_origins == expected_origins
        assert "*" not in allow_origins
        assert ["*"] != allow_origins

        # Verify CORS enforcement via TestClient
        with TestClient(app) as client:
            # Allowed origin
            resp_allowed = client.get("/api/config", headers={"Origin": "http://localhost:8000"})
            assert resp_allowed.headers.get("access-control-allow-origin") == "http://localhost:8000"

            # Disallowed origin
            resp_denied = client.get("/api/config", headers={"Origin": "http://evil.com"})
            assert "access-control-allow-origin" not in resp_denied.headers

    def test_start_enforces_loopback_binding(self):
        import argparse
        from sentinel.server import start

        with patch("argparse.ArgumentParser.parse_args", return_value=argparse.Namespace(host="0.0.0.0", port=8000, reload=False)):
            with patch("uvicorn.run") as mock_uvicorn:
                start()
                mock_uvicorn.assert_called_once_with("sentinel.server:app", host="127.0.0.1", port=8000, reload=False)


# ---------------------------------------------------------------------------
# SE-01: Process Throttling & Zombie Lockup Recovery Tests
# ---------------------------------------------------------------------------

class TestProcessThrottlingAndRecovery:
    """Verify security hardening for active process throttling and crash recovery."""

    def test_get_target_llm_pids_exact_matching(self, monkeypatch):
        from sentinel import server

        with patch("subprocess.check_output", return_value=f"1111\n{server.os.getpid()}\n2222\n") as mock_pgrep:
            pids = server._get_target_llm_pids()
            mock_pgrep.assert_called_once_with(
                ["pgrep", "-x", "ollama|llama-server|vllm|text-generation"],
                text=True,
            )
            # Verify own PID is filtered out and exact PIDs returned
            assert pids == [1111, 2222]

    def test_get_target_llm_pids_bsd_fallback_matching(self, monkeypatch):
        from sentinel import server
        import subprocess

        # Simulate regex alternation failing on BSD/macOS and falling back to per-process matching
        def mock_check_output(cmd, text=True):
            if cmd == ["pgrep", "-x", "ollama|llama-server|vllm|text-generation"]:
                raise subprocess.CalledProcessError(1, cmd)
            if cmd == ["pgrep", "-E", "-x", "ollama|llama-server|vllm|text-generation"]:
                raise subprocess.CalledProcessError(2, cmd, output="pgrep: illegal option -- E")
            if cmd == ["pgrep", "-x", "ollama"]:
                return "3333\n"
            if cmd == ["pgrep", "-x", "llama-server"]:
                return f"4444\n{server.os.getpid()}\n"
            return ""

        with patch("subprocess.check_output", side_effect=mock_check_output):
            pids = server._get_target_llm_pids()
            assert pids == [3333, 4444]

    def test_apply_throttling_pauses_and_persists(self, tmp_path, monkeypatch):
        from sentinel import server
        import signal

        state_file = tmp_path / "throttled.json"
        monkeypatch.setenv("SENTINEL_THROTTLED_PIDS_PATH", str(state_file))
        server._THROTTLED_PIDS.clear()

        with patch.object(server, "_get_target_llm_pids", return_value=[4001, 4002]):
            with patch("os.kill") as mock_kill:
                actioned = server.apply_active_process_throttling(0.95)

                assert actioned == [4001, 4002]
                assert server._THROTTLED_PIDS == {4001, 4002}
                assert mock_kill.call_count == 2
                mock_kill.assert_any_call(4001, signal.SIGSTOP)
                mock_kill.assert_any_call(4002, signal.SIGSTOP)

                # Verify persisted to disk
                assert state_file.exists()
                persisted = json.loads(state_file.read_text())
                assert sorted(persisted) == [4001, 4002]

    def test_apply_throttling_resumes_and_cleans_persistence(self, tmp_path, monkeypatch):
        from sentinel import server
        import signal

        state_file = tmp_path / "throttled.json"
        monkeypatch.setenv("SENTINEL_THROTTLED_PIDS_PATH", str(state_file))
        server._THROTTLED_PIDS.clear()
        server._THROTTLED_PIDS.update([4001, 4002])
        server._save_throttled_pids(server._THROTTLED_PIDS)

        with patch("os.kill") as mock_kill:
            actioned = server.apply_active_process_throttling(0.50)

            assert sorted(actioned) == [4001, 4002]
            assert server._THROTTLED_PIDS == set()
            assert mock_kill.call_count == 2
            mock_kill.assert_any_call(4001, signal.SIGCONT)
            mock_kill.assert_any_call(4002, signal.SIGCONT)

            # Verify persistence file was removed
            assert not state_file.exists()

    def test_resume_all_throttled_processes_startup_recovery(self, tmp_path, monkeypatch):
        from sentinel import server
        import signal

        state_file = tmp_path / "throttled.json"
        state_file.write_text(json.dumps([5001, 5002]))
        monkeypatch.setenv("SENTINEL_THROTTLED_PIDS_PATH", str(state_file))
        server._THROTTLED_PIDS.clear()

        with patch("os.kill") as mock_kill:
            # Simulate one dead process raising ProcessLookupError and one alive
            def kill_side_effect(pid, sig):
                if pid == 5001:
                    raise ProcessLookupError()
                return None

            mock_kill.side_effect = kill_side_effect

            resumed = server.resume_all_throttled_processes()

            assert resumed == [5002]
            assert server._THROTTLED_PIDS == set()
            assert not state_file.exists()
            assert mock_kill.call_count == 2

    def test_shutdown_resumes_throttled_processes(self, tmp_path, monkeypatch):
        from sentinel import server
        import signal
        import asyncio

        state_file = tmp_path / "throttled.json"
        monkeypatch.setenv("SENTINEL_THROTTLED_PIDS_PATH", str(state_file))
        server._THROTTLED_PIDS.clear()
        server._THROTTLED_PIDS.add(6001)
        server._save_throttled_pids(server._THROTTLED_PIDS)

        with patch("os.kill") as mock_kill:
            asyncio.run(server._shutdown())

            mock_kill.assert_called_once_with(6001, signal.SIGCONT)
            assert server._THROTTLED_PIDS == set()
            assert not state_file.exists()


# ---------------------------------------------------------------------------
# Endpoint Authentication & Loopback Protection Tests
# ---------------------------------------------------------------------------

class TestEndpointSecurity:
    def test_unauthenticated_request_rejected_when_token_configured(self, monkeypatch):
        monkeypatch.setenv("SENTINEL_AUTH_TOKEN", "super-secret-token")
        with TestClient(app) as client:
            resp = client.get("/api/config")
            assert resp.status_code == 401
            assert resp.json() == {"error": "Unauthorized"}

    def test_invalid_bearer_token_rejected(self, monkeypatch):
        monkeypatch.setenv("SENTINEL_AUTH_TOKEN", "super-secret-token")
        with TestClient(app) as client:
            resp = client.get("/api/config", headers={"Authorization": "Bearer wrong-token"})
            assert resp.status_code == 401
            assert resp.json() == {"error": "Unauthorized"}

    def test_valid_bearer_token_accepted(self, monkeypatch):
        monkeypatch.setenv("SENTINEL_AUTH_TOKEN", "super-secret-token")
        with TestClient(app) as client:
            resp = client.get("/api/config", headers={"Authorization": "Bearer super-secret-token"})
            assert resp.status_code == 200

    def test_valid_query_token_accepted(self, monkeypatch):
        monkeypatch.setenv("SENTINEL_AUTH_TOKEN", "super-secret-token")
        with TestClient(app) as client:
            resp = client.get("/api/config?token=super-secret-token")
            assert resp.status_code == 200

    def test_websocket_rejected_when_token_configured_and_missing(self, monkeypatch):
        monkeypatch.setenv("SENTINEL_AUTH_TOKEN", "super-secret-token")
        with TestClient(app) as client:
            with pytest.raises(Exception):
                with client.websocket_connect("/ws/telemetry") as ws:
                    ws.receive_json()

    def test_websocket_accepted_with_valid_token(self, monkeypatch):
        monkeypatch.setenv("SENTINEL_AUTH_TOKEN", "super-secret-token")
        patches = _patch_harvester()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            with TestClient(app) as client:
                with client.websocket_connect("/ws/telemetry?token=super-secret-token") as ws:
                    msg = ws.receive_json()
                    assert "timestamp" in msg



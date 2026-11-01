"""
Tests/test_history_api.py

Unit tests for the /api/history and /api/spikes REST endpoints.
Uses FastAPI's TestClient and an isolated SQLite database.
"""

import time
import pytest
from fastapi.testclient import TestClient

import sentinel.database as db_module
from sentinel.database import init_db, insert_telemetry
from sentinel.server import app

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def db_path(tmp_path):
    """Return a fresh, initialised database path for each test."""
    path = str(tmp_path / "test_api.db")
    init_db(path=path)
    return path

@pytest.fixture()
def client(db_path, monkeypatch):
    """
    TestClient with harvester calls patched out and the database redirected
    to a temp file so tests are fully isolated from the live sentinel.db.

    We patch sentinel.database._DB_PATH so that _resolve() picks up the temp
    path at call time — this works regardless of how functions were imported.
    """
    # Redirect every database call to the temp db
    monkeypatch.setattr(db_module, "_DB_PATH", db_path)
    monkeypatch.setenv("SENTINEL_DISABLE_BACKGROUND_POLLER", "true")

    # Patch harvester so startup doesn't fail on macOS-only syscalls
    monkeypatch.setattr("sentinel.harvester.get_gpu_wired_limit",   lambda: 18_432)
    monkeypatch.setattr("sentinel.harvester.get_wired_memory_mb",   lambda: 4_096.0)
    monkeypatch.setattr("sentinel.harvester.get_swap_usage",        lambda: (2_048.0, 512.0))
    monkeypatch.setattr("sentinel.harvester.get_pageout_count",     lambda: 42)
    monkeypatch.setattr(
        "sentinel.harvester.compute_thrash_danger_index",
        lambda w, l, s: 0.2,
    )

    with TestClient(app) as c:
        yield c

# ── Helper ────────────────────────────────────────────────────────────────────

def _insert(db_path, *, timestamp, wired_mb=4096, swap_used_mb=0.0,
            pageouts=10, thrash_index=0.1):
    return insert_telemetry(
        timestamp=timestamp,
        wired_mb=wired_mb,
        swap_used_mb=swap_used_mb,
        pageouts=pageouts,
        thrash_index=thrash_index,
        path=db_path,
    )

# ══════════════════════════════════════════════════════════════════════════════
# /api/history  – 1m window
# ══════════════════════════════════════════════════════════════════════════════

class TestHistoryWindow1m:

    def test_status_200(self, client):
        resp = client.get("/api/history?window=1m")
        assert resp.status_code == 200

    def test_returns_list(self, client):
        resp = client.get("/api/history?window=1m")
        assert isinstance(resp.json(), list)

    def test_empty_when_no_rows(self, client):
        resp = client.get("/api/history?window=1m")
        assert resp.json() == []

    def test_only_returns_rows_within_60s(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 30)   # within window
        _insert(db_path, timestamp=now - 90)   # outside window

        resp = client.get("/api/history?window=1m")
        rows = resp.json()
        assert len(rows) == 1
        assert abs(rows[0]["timestamp"] - (now - 30)) < 1.0

    def test_ordered_oldest_first(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 50)
        _insert(db_path, timestamp=now - 20)
        _insert(db_path, timestamp=now - 10)

        rows = client.get("/api/history?window=1m").json()
        timestamps = [r["timestamp"] for r in rows]
        assert timestamps == sorted(timestamps)

    def test_max_60_rows(self, client, db_path):
        now = time.time()
        for i in range(80):
            _insert(db_path, timestamp=now - i)

        rows = client.get("/api/history?window=1m").json()
        assert len(rows) <= 60

    def test_row_has_required_keys(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 5)

        row = client.get("/api/history?window=1m").json()[0]
        for key in ("timestamp", "wired_mb", "swap_used_mb", "pageouts", "thrash_index"):
            assert key in row, f"Missing key: {key}"

# ══════════════════════════════════════════════════════════════════════════════
# /api/history  – 5m window
# ══════════════════════════════════════════════════════════════════════════════

class TestHistoryWindow5m:

    def test_status_200(self, client):
        resp = client.get("/api/history?window=5m")
        assert resp.status_code == 200

    def test_excludes_rows_older_than_300s(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 100)   # inside
        _insert(db_path, timestamp=now - 400)   # outside

        rows = client.get("/api/history?window=5m").json()
        for r in rows:
            assert r["timestamp"] >= now - 300 - 1  # small tolerance

    def test_ordered_oldest_first(self, client, db_path):
        now = time.time()
        for i in range(30):
            _insert(db_path, timestamp=now - i * 9)

        rows = client.get("/api/history?window=5m").json()
        timestamps = [r["timestamp"] for r in rows]
        assert timestamps == sorted(timestamps)

    def test_max_60_rows(self, client, db_path):
        now = time.time()
        # Insert one row per second for the last 300 s (300 rows → sampled down)
        for i in range(300):
            _insert(db_path, timestamp=now - i)

        rows = client.get("/api/history?window=5m").json()
        assert len(rows) <= 60

    def test_row_has_required_keys(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 60)

        rows = client.get("/api/history?window=5m").json()
        if rows:
            row = rows[0]
            for key in ("timestamp", "wired_mb", "swap_used_mb", "pageouts", "thrash_index"):
                assert key in row

# ══════════════════════════════════════════════════════════════════════════════
# /api/history  – 1h window
# ══════════════════════════════════════════════════════════════════════════════

class TestHistoryWindow1h:

    def test_status_200(self, client):
        resp = client.get("/api/history?window=1h")
        assert resp.status_code == 200

    def test_excludes_rows_older_than_3600s(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 1800)   # inside
        _insert(db_path, timestamp=now - 4000)   # outside

        rows = client.get("/api/history?window=1h").json()
        for r in rows:
            assert r["timestamp"] >= now - 3600 - 1

    def test_aggregated_values_are_averages(self, client, db_path):
        """Two rows in the same 60-second bucket should produce one averaged row."""
        now = time.time()
        bucket_start = now - 1800
        _insert(db_path, timestamp=bucket_start + 10, wired_mb=2000, thrash_index=0.2)
        _insert(db_path, timestamp=bucket_start + 20, wired_mb=4000, thrash_index=0.4)

        rows = client.get("/api/history?window=1h").json()
        # Both rows fall in the same 60-second bucket so we get exactly one row
        assert len(rows) == 1
        assert abs(rows[0]["wired_mb"]     - 3000.0) < 1.0
        assert abs(rows[0]["thrash_index"] - 0.3)    < 0.001

    def test_max_120_rows(self, client, db_path):
        now = time.time()
        for i in range(3600):
            _insert(db_path, timestamp=now - i)

        rows = client.get("/api/history?window=1h").json()
        assert len(rows) <= 120

    def test_no_bucket_key_in_response(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 600)

        rows = client.get("/api/history?window=1h").json()
        if rows:
            assert "bucket" not in rows[0]

# ══════════════════════════════════════════════════════════════════════════════
# /api/history  – invalid window
# ══════════════════════════════════════════════════════════════════════════════

class TestHistoryWindowInvalid:

    def test_unknown_window_returns_422(self, client):
        # FastAPI validates the Literal type and returns 422 before our code runs
        resp = client.get("/api/history?window=2h")
        assert resp.status_code == 422

    def test_missing_window_defaults_to_1m(self, client):
        resp = client.get("/api/history")
        assert resp.status_code == 200

# ══════════════════════════════════════════════════════════════════════════════
# /api/spikes
# ══════════════════════════════════════════════════════════════════════════════

class TestSpikesEndpoint:

    def test_status_200(self, client):
        assert client.get("/api/spikes").status_code == 200

    def test_returns_list(self, client):
        assert isinstance(client.get("/api/spikes").json(), list)

    def test_empty_when_no_spikes(self, client, db_path):
        # Insert a row with thrash_index < 0.4 and swap == 0 — should not appear
        now = time.time()
        _insert(db_path, timestamp=now - 100, thrash_index=0.1, swap_used_mb=0.0)
        assert client.get("/api/spikes").json() == []

    def test_includes_high_thrash_rows(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 100, thrash_index=0.8, swap_used_mb=0.0)

        rows = client.get("/api/spikes").json()
        assert len(rows) == 1
        assert abs(rows[0]["thrash_index"] - 0.8) < 0.001

    def test_includes_nonzero_swap_rows(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 100, thrash_index=0.1, swap_used_mb=512.0)

        rows = client.get("/api/spikes").json()
        assert len(rows) == 1
        assert rows[0]["swap_used_mb"] == 512.0

    def test_excludes_rows_older_than_24h(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 90_000, thrash_index=0.9, swap_used_mb=1024.0)

        rows = client.get("/api/spikes").json()
        assert rows == []

    def test_max_5_results(self, client, db_path):
        now = time.time()
        for i in range(10):
            _insert(
                db_path,
                timestamp=now - i * 60,
                thrash_index=round(0.5 + i * 0.04, 3),
                swap_used_mb=float(i * 100),
            )

        rows = client.get("/api/spikes").json()
        assert len(rows) <= 5

    def test_ordered_by_thrash_index_descending(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 60,  thrash_index=0.5)
        _insert(db_path, timestamp=now - 120, thrash_index=0.9)
        _insert(db_path, timestamp=now - 180, thrash_index=0.7)

        rows = client.get("/api/spikes").json()
        indices = [r["thrash_index"] for r in rows]
        assert indices == sorted(indices, reverse=True)

    def test_row_has_required_keys(self, client, db_path):
        now = time.time()
        _insert(db_path, timestamp=now - 60, thrash_index=0.6, wired_mb=8192, swap_used_mb=256.0)

        row = client.get("/api/spikes").json()[0]
        for key in ("timestamp", "thrash_index", "wired_mb", "swap_used_mb", "pageouts"):
            assert key in row, f"Missing key: {key}"

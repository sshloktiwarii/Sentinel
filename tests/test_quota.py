"""
tests/test_quota.py

Unit tests for sentinel/quota.py and /api/quotas endpoint.
Tests authenticated proxy harvesting, token discovery, schema conformity,
and graceful fallback caching.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from sentinel.quota import (
    _BASELINE,
    _get_omniroute_token,
    _parse_omniroute_telemetry,
    _seconds_to_human,
    get_all_quotas,
    get_all_quotas_sync,
)
from sentinel.server import app

REQUIRED_KEYS = {"id", "provider", "model", "remaining_pct", "tokens_left", "resets_in", "status"}
VALID_STATUSES = {"healthy", "warning", "exhausted"}


class TestTokenDiscovery:
    def test_token_from_env(self, monkeypatch):
        monkeypatch.setenv("OMNIROUTE_API_KEY", "sk-test-env-token-1234")
        assert _get_omniroute_token() == "sk-test-env-token-1234"

    def test_token_from_file(self, monkeypatch, tmp_path):
        monkeypatch.delenv("OMNIROUTE_API_KEY", raising=False)
        fake_env = tmp_path / ".env"
        fake_env.write_text("STORAGE_KEY=abc\nOMNIROUTE_API_KEY=sk-file-token-9999\n")

        import sentinel.quota as sq
        with patch.object(sq, "os") as mock_os:
            mock_os.environ = {}
            mock_os.path.expanduser.side_effect = lambda p: str(fake_env) if ".omniroute" in p else "/nonexistent"
            mock_os.path.isfile.side_effect = lambda p: p == str(fake_env)
            assert sq._get_omniroute_token() == "sk-file-token-9999"



class TestTelemetryParsing:
    def test_parse_omniroute_and_providers(self):
        quota_data = {
            "providers": [
                {
                    "provider": "cursor",
                    "quotaUsed": 100,
                    "quotaTotal": 100,
                    "percentRemaining": 0,
                    "resetAt": "2026-10-15T14:20:42.433Z",
                    "tokenStatus": "valid",
                },
                {
                    "provider": "kilocode",
                    "quotaUsed": 0,
                    "quotaTotal": None,
                    "percentRemaining": 100,
                    "resetAt": None,
                    "tokenStatus": "valid",
                },
                {
                    "provider": "ollama",
                    "quotaUsed": 0,
                    "quotaTotal": None,
                    "percentRemaining": 100,
                    "resetAt": None,
                    "tokenStatus": "valid",
                },
            ]
        }
        analytics_data = {
            "summary": {
                "totalTokens": 402414,
                "totalRequests": 330,
                "successfulRequests": 84,
            },
            "errorBreakdown": [
                {"errorType": "rate_limited", "count": 53},
            ],
        }

        updates = _parse_omniroute_telemetry(quota_data, analytics_data, None)

        assert "omniroute" in updates
        omni = updates["omniroute"]
        assert omni["tokens_left"] == "402k tokens / 330 reqs"
        assert omni["remaining_pct"] == 67  # 2 of 3 available
        assert omni["status"] == "healthy"

        assert "cursor" in updates
        cur = updates["cursor"]
        assert cur["remaining_pct"] == 0
        assert cur["tokens_left"] == "0 / 100 reqs"
        assert cur["status"] == "exhausted"

        assert "kilocode" in updates
        kilo = updates["kilocode"]
        assert kilo["remaining_pct"] == 100
        assert kilo["status"] == "healthy"

    def test_parse_empty_payloads(self):
        updates = _parse_omniroute_telemetry(None, None, None)
        assert "omniroute" in updates
        assert updates["omniroute"]["remaining_pct"] == 0
        assert "Offline" in updates["omniroute"]["tokens_left"] or "Unreachable" in updates["omniroute"]["tokens_left"]


class TestPublicQuotasApi:
    @pytest.mark.asyncio
    async def test_get_all_quotas_returns_valid_schema(self):
        quotas = await get_all_quotas()
        assert isinstance(quotas, list)
        assert len(quotas) >= len(_BASELINE)

        ids = [q["id"] for q in quotas]
        assert "omniroute" in ids
        assert "kilocode" in ids
        assert "cursor" in ids
        assert "claude" in ids
        assert "codex" in ids

        for q in quotas:
            assert REQUIRED_KEYS.issubset(q.keys())
            assert 0 <= q["remaining_pct"] <= 100
            assert q["status"] in VALID_STATUSES
            assert isinstance(q["tokens_left"], str)
            assert isinstance(q["resets_in"], str)

    def test_get_all_quotas_sync_matches(self):
        quotas = get_all_quotas_sync()
        assert isinstance(quotas, list)
        assert len(quotas) >= len(_BASELINE)
        for q in quotas:
            assert REQUIRED_KEYS.issubset(q.keys())

    def test_rest_endpoint_returns_live_quotas(self):
        with TestClient(app) as client:
            resp = client.get("/api/quotas")
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, list)
            assert len(data) >= 6

            omni = next((item for item in data if item["id"] == "omniroute"), None)
            assert omni is not None
            assert "tokens" in omni["tokens_left"] or "Unlimited" in omni["tokens_left"] or "Offline" in omni["tokens_left"]


class TestGracefulFallback:
    @pytest.mark.asyncio
    async def test_fallback_when_proxy_down(self, monkeypatch):
        # Point to unreachable port
        monkeypatch.setenv("OMNIROUTE_BASE", "http://localhost:59999")
        # Clear cache
        import sentinel.quota as sq
        sq._OMNIROUTE_CACHE = {}
        sq._OMNIROUTE_CACHE_TS = 0.0

        quotas = await sq.get_all_quotas()
        assert isinstance(quotas, list)
        assert len(quotas) > 0

        omni = next(q for q in quotas if q["id"] == "omniroute")
        assert omni["status"] == "exhausted"
        assert "Offline" in omni["tokens_left"] or "Unreachable" in omni["tokens_left"]

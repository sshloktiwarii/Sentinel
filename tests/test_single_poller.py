import asyncio
import time
from unittest.mock import patch
import pytest
from starlette.testclient import TestClient

import sentinel.server as srv

@pytest.mark.asyncio
async def test_single_background_poller_coalesces_multiple_tabs():
    """Verify that multiple concurrent WebSocket clients connecting at different times
    receive live telemetry while _collect_reading is strictly executed by the single background loop
    at 1-second cadence, preventing pgrep storms regardless of client count."""
    collect_count = 0
    real_collect = srv._collect_reading

    def counted_collect():
        nonlocal collect_count
        collect_count += 1
        return real_collect()

    with patch("sentinel.server._collect_reading", side_effect=counted_collect):
        # Start the background poller task directly
        poller_task = asyncio.create_task(srv._telemetry_poller())

        class MockWS:
            def __init__(self, name):
                self.name = name
                self.queue = asyncio.Queue()
            async def accept(self):
                pass
            async def send_json(self, data):
                await self.queue.put(data)

        ws1 = MockWS("tab-1")
        ws2 = MockWS("tab-2")
        ws3 = MockWS("tab-3")

        # Tab 1 connects at t=0
        task1 = asyncio.create_task(srv.ws_telemetry(ws1))
        await asyncio.sleep(0.3)

        # Tab 2 connects at t=0.3s (offset phase)
        task2 = asyncio.create_task(srv.ws_telemetry(ws2))
        await asyncio.sleep(0.4)

        # Tab 3 connects at t=0.7s (offset phase)
        task3 = asyncio.create_task(srv.ws_telemetry(ws3))

        # Let the poller run for 2.2 seconds total
        await asyncio.sleep(1.5)

        # Stop poller and clients
        poller_task.cancel()
        task1.cancel()
        task2.cancel()
        task3.cancel()
        await asyncio.gather(poller_task, task1, task2, task3, return_exceptions=True)

        # Clean up subscribers
        srv._subscribers.clear()

        # In ~2.2 seconds, the poller executes approximately 2-3 times regardless of 3 active tabs
        print(f"Total collections during 3-tab test: {collect_count}")
        assert collect_count in (2, 3), f"Expected 2-3 single-loop polls, got {collect_count}"

        # All 3 tabs must have received broadcasts
        assert not ws1.queue.empty(), "Tab 1 received no telemetry"
        assert not ws2.queue.empty(), "Tab 2 received no telemetry"
        assert not ws3.queue.empty(), "Tab 3 received no telemetry"


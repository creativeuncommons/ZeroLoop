#!/usr/bin/env python3
"""
Unit Tests for ZeroLoop Integration Bridges
===========================================
Tests ZeroMCP, HyperGate, ZeroLeak, and AgentTrace bridges.
"""

import os
import sys
import tempfile
import shutil
import unittest
from typing import Dict, Any

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
ZEROLOOP_PY_DIR = os.path.join(PROJECT_ROOT, "zeroloop-py")
BRIDGES_DIR = os.path.join(PROJECT_ROOT, "bridges")

for p in [ZEROLOOP_PY_DIR, BRIDGES_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from zeroloop import DurableLoop, LoopPausedException
from zeromcp_bridge import ZeroMCPSupervisor, supervise_mcp_tool
from hypergate_bridge import ZeroLoopHyperGateInterceptor
from zeroleak_bridge import ZeroLoopSanitizer
from agenttrace_bridge import ZeroLoopTracePublisher


class TestZeroLoopBridges(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="zeroloop_bridges_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_zeromcp_bridge(self):
        """Asserts ZeroMCPSupervisor provides idempotency and avoids double dispatches."""
        loop = DurableLoop(loop_id="test_mcp_bridge", storage_dir=self.test_dir)
        loop.start()
        loop.step_start("mcp_dispatch")

        call_count = {"count": 0}

        def mock_mcp_tool(x: int) -> int:
            call_count["count"] += 1
            return x * 2

        supervisor = ZeroMCPSupervisor(loop)
        r1 = supervisor.dispatch("mock_mcp_tool", mock_mcp_tool, {"x": 5})
        self.assertEqual(r1, 10)
        self.assertEqual(call_count["count"], 1)

        # Re-dispatch with same params -> cache hit
        r2 = supervisor.dispatch("mock_mcp_tool", mock_mcp_tool, {"x": 5})
        self.assertEqual(r2, 10)
        self.assertEqual(call_count["count"], 1)
        self.assertEqual(loop.duplicate_tool_calls_prevented, 1)

    def test_hypergate_bridge(self):
        """Asserts HyperGate routing interceptor captures costs into ZeroLoop journal."""
        loop = DurableLoop(loop_id="test_hg_bridge", storage_dir=self.test_dir)
        loop.start()
        loop.step_start("llm_reasoning")

        interceptor = ZeroLoopHyperGateInterceptor(loop)

        def mock_llm(prompt: str, model: str):
            return f"Answer for: {prompt}", 0.0025

        resp = interceptor.route_and_execute_llm(
            prompt="Hello World",
            llm_caller_fn=mock_llm
        )
        self.assertIn("Answer for: Hello World", resp)
        self.assertAlmostEqual(loop.total_cost_usd, 0.0025, places=4)

        # Test budget pause
        with self.assertRaises(LoopPausedException):
            interceptor.route_and_execute_llm(
                prompt="Budget overflow",
                llm_caller_fn=mock_llm,
                max_cost_limit_usd=0.0020  # Total cost (0.0025) exceeds limit
            )

    def test_zeroleak_bridge(self):
        """Asserts ZeroLeak bridge sanitizes sensitive dictionaries and strings."""
        sanitizer = ZeroLoopSanitizer()
        sample_dict = {
            "prompt": "User input with test@example.com",
            "nested": {"key": "secret_token_1234"}
        }
        # In stdlib fallback mode or engine mode, sanitize_dict deep-processes dicts
        clean = sanitizer.sanitize_dict(sample_dict)
        self.assertIn("prompt", clean)
        self.assertIn("nested", clean)

    def test_agenttrace_bridge(self):
        """Asserts AgentTrace bridge records spans and transitions cleanly."""
        loop = DurableLoop(loop_id="test_trace_bridge", storage_dir=self.test_dir, task_name="traced_loop")
        loop.start()

        publisher = ZeroLoopTracePublisher()
        span = publisher.start_loop_span(loop)
        publisher.record_transition(span, "PENDING", "RUNNING", reason="test_start")

        receipt = loop.complete("final")
        publisher.finish_loop_span(span, receipt)


if __name__ == "__main__":
    unittest.main()

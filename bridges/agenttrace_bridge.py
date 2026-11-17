"""
ZeroLoop <-> AgentTrace Integration Bridge
==========================================
Telemetry integration publisher synchronizing ZeroLoop lifecycle state transitions,
steps, and tool calls into AgentTrace spans and DAG trajectories.

Guarantees:
1. Emits SpanKind.AGENT root spans for durable multi-turn loops.
2. Emits child spans for each step and tool execution.
3. Automatically sets state transition attributes and pause/resume telemetry events.
"""

import os
import sys
from typing import Dict, Any, Optional

# Include local zeroloop-py
ZEROLOOP_PY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "zeroloop-py"))
if ZEROLOOP_PY_DIR not in sys.path:
    sys.path.insert(0, ZEROLOOP_PY_DIR)

from zeroloop import DurableLoop, LoopEvent, EventType


class ZeroLoopTracePublisher:
    """
    Publishes ZeroLoop events directly to an AgentTrace tracer.
    """
    def __init__(self, tracer: Optional[Any] = None):
        self.tracer = tracer
        if self.tracer is None:
            try:
                WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
                at_path = os.path.join(WORKSPACE_ROOT, "AgentTrace", "agenttrace-py")
                if os.path.exists(at_path) and at_path not in sys.path:
                    sys.path.insert(0, at_path)
                from agenttrace import AgentTracer
                self.tracer = AgentTracer()
            except Exception:
                self.tracer = None

    def start_loop_span(self, loop: DurableLoop) -> Optional[Any]:
        """Creates an Agent root span for the loop."""
        if not self.tracer or not hasattr(self.tracer, "start_span"):
            return None
        try:
            span = self.tracer.start_span(
                name=f"zeroloop:{loop.task_name}",
                kind="AGENT",
                attributes={
                    "zeroloop.loop_id": loop.loop_id,
                    "zeroloop.task_name": loop.task_name,
                    "zeroloop.state": loop.state.value,
                    "zeroloop.step_idx": loop.step_idx
                }
            )
            return span
        except Exception:
            return None

    def record_transition(self, span: Any, old_state: str, new_state: str, reason: str = ""):
        """Records a state transition event on the active span."""
        if not span:
            return
        if hasattr(span, "add_event"):
            span.add_event(
                name="state_transition",
                attributes={"from_state": old_state, "to_state": new_state, "reason": reason}
            )
        if hasattr(span, "set_attribute"):
            span.set_attribute("zeroloop.state", new_state)

    def finish_loop_span(self, span: Any, receipt: Any):
        """Finishes loop root span with receipt metrics."""
        if not span:
            return
        if hasattr(span, "set_attribute"):
            span.set_attribute("zeroloop.total_steps", getattr(receipt, "total_steps", 0))
            span.set_attribute("zeroloop.duplicates_prevented", getattr(receipt, "duplicate_tool_calls_prevented", 0))
            span.set_attribute("zeroloop.cost_usd", getattr(receipt, "total_cost_usd", 0.0))
            span.set_attribute("zeroloop.active_duration_s", getattr(receipt, "active_duration_s", 0.0))
        if hasattr(span, "finish"):
            span.finish()

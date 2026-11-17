"""
ZeroLoop <-> ZeroMCP Integration Bridge
=======================================
Tool supervisor wrapper providing deterministic idempotency protection
and crash recovery for ZeroMCP tool invocations.

Guarantees:
1. Every ZeroMCP tool dispatch is governed by an idempotency token:
   hash(step_idx, tool_name, sanitized_inputs).
2. If an agent process crashes or restarts, ZeroMCP is not re-invoked;
   the cached output is replayed from ZeroLoop's SQLite WAL journal.
3. Mutating tools (database updates, payments, API dispatches) are executed
   EXACTLY ONCE across any number of rehydration restarts.
"""

import os
import sys
import functools
from typing import Dict, Any, Callable, Optional, Union

# Include local zeroloop-py
ZEROLOOP_PY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "zeroloop-py"))
if ZEROLOOP_PY_DIR not in sys.path:
    sys.path.insert(0, ZEROLOOP_PY_DIR)

from zeroloop import DurableLoop, compute_idempotency_key


class ZeroMCPSupervisor:
    """
    Supervises tool dispatches to ZeroMCP tools within a DurableLoop context.
    """
    def __init__(self, loop: DurableLoop):
        self.loop = loop

    def dispatch(
        self,
        tool_name: str,
        tool_fn: Callable[..., Any],
        params: Dict[str, Any],
        sanitize_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    ) -> Any:
        """
        Executes a ZeroMCP tool through ZeroLoop's Idempotency Gate.
        """
        return self.loop.execute_tool(
            tool_name=tool_name,
            tool_fn=tool_fn,
            inputs=params,
            sanitize_inputs_fn=sanitize_fn
        )


def supervise_mcp_tool(
    handler: Optional[Callable[..., Any]] = None,
    *,
    tool_name: Optional[str] = None
):
    """
    Decorator for ZeroMCP tool handlers that binds them to the active ZeroLoop.
    
    Usage:
        @supervise_mcp_tool
        def transfer_funds(account_id: str, amount: float, loop: Optional[DurableLoop] = None):
            ...
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        actual_name = tool_name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            loop: Optional[DurableLoop] = kwargs.pop("_loop", None)
            if loop is not None and isinstance(loop, DurableLoop):
                return loop.execute_tool(
                    tool_name=actual_name,
                    tool_fn=lambda **kw: fn(*args, **kw),
                    inputs=kwargs
                )
            # Direct invocation without active loop
            return fn(*args, **kwargs)
        return wrapper

    if handler is not None:
        return decorator(handler)
    return decorator

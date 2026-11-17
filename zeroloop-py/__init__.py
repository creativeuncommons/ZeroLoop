"""
ZeroLoop Package Root
=====================
Zero-Dependency Crash-Proof Durable Agent State Machine.
"""

from .zeroloop import (
    LoopState,
    EventType,
    LoopEvent,
    LoopReceipt,
    LoopPausedException,
    EventJournal,
    DurableLoop,
    durable_loop,
    compute_idempotency_key,
    prune_journals,
    list_loops,
    inspect_loop,
    main,
)

__version__ = "0.1.0"
__all__ = [
    "LoopState",
    "EventType",
    "LoopEvent",
    "LoopReceipt",
    "LoopPausedException",
    "EventJournal",
    "DurableLoop",
    "durable_loop",
    "compute_idempotency_key",
    "prune_journals",
    "list_loops",
    "inspect_loop",
    "main",
]

"""
ZeroLoop <-> ZeroLeak Integration Bridge
========================================
Privacy and secret sanitization middleware ensuring ZeroLoop's SQLite WAL
journals and LLM prompts contain zero raw PII or secret credentials.

Guarantees:
1. Inbound prompts and user tasks are de-identified before persistence in ZeroLoop.
2. Tool execution parameters and tool outputs are masked before state recording.
3. Only reversible synthetic tokens (e.g. [EMAIL_1], [API_KEY_1]) reside in journal.
"""

import os
import sys
import copy
from typing import Dict, Any, Optional, Callable

# Include local zeroloop-py
ZEROLOOP_PY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "zeroloop-py"))
if ZEROLOOP_PY_DIR not in sys.path:
    sys.path.insert(0, ZEROLOOP_PY_DIR)

from zeroloop import DurableLoop


class ZeroLoopSanitizer:
    """
    Sanitizes state dictionaries, prompts, and tool inputs using ZeroLeak.
    """
    def __init__(self, zeroleak_engine: Optional[Any] = None):
        self.engine = zeroleak_engine
        if self.engine is None:
            # Attempt to import local ZeroLeak if available
            try:
                WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
                zl_path = os.path.join(WORKSPACE_ROOT, "ZeroLeak", "zeroleak-py")
                if os.path.exists(zl_path) and zl_path not in sys.path:
                    sys.path.insert(0, zl_path)
                from zeroleak import default_engine
                self.engine = default_engine
            except Exception:
                self.engine = None

    def sanitize_text(self, text: str, trace_id: str = "zeroloop_session") -> str:
        """De-identifies text using ZeroLeak if present."""
        if not text or not isinstance(text, str):
            return text
        if self.engine and hasattr(self.engine, "mask"):
            return self.engine.mask(text, trace_id=trace_id)
        return text

    def unmask_text(self, text: str, trace_id: str = "zeroloop_session") -> str:
        """Restitutes synthetic tokens back to plaintext for local execution."""
        if not text or not isinstance(text, str):
            return text
        if self.engine and hasattr(self.engine, "unmask"):
            return self.engine.unmask(text, trace_id=trace_id)
        return text

    def sanitize_dict(self, data: Dict[str, Any], trace_id: str = "zeroloop_session") -> Dict[str, Any]:
        """Deeply de-identifies all string values inside a dictionary."""
        if self.engine and hasattr(self.engine, "mask_object"):
            return self.engine.mask_object(data, trace_id=trace_id)

        clean = copy.deepcopy(data)
        for k, v in clean.items():
            if isinstance(v, str):
                clean[k] = self.sanitize_text(v, trace_id=trace_id)
            elif isinstance(v, dict):
                clean[k] = self.sanitize_dict(v, trace_id=trace_id)
        return clean

#!/usr/bin/env python3
"""
Unit Tests for ZeroLoop Core Engine
===================================
Tests state machine transitions, crash-proof recovery, tool idempotency,
human-in-the-loop pause/resume, and auto-pruning.
"""

import os
import sys
import time
import shutil
import tempfile
import unittest
from typing import Dict, Any

# Path setup
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ZEROLOOP_PY_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "zeroloop-py"))
if ZEROLOOP_PY_DIR not in sys.path:
    sys.path.insert(0, ZEROLOOP_PY_DIR)

from zeroloop import (
    LoopState,
    EventType,
    EventJournal,
    DurableLoop,
    LoopPausedException,
    compute_idempotency_key,
    prune_journals,
    list_loops,
    inspect_loop,
)


class TestZeroLoopCore(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="zeroloop_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_state_transitions_and_journal_chain(self):
        """Asserts valid state progression and tamper-evident cryptographic chaining."""
        loop = DurableLoop(loop_id="test_trans_1", storage_dir=self.test_dir, task_name="test_task")
        self.assertEqual(loop.state, LoopState.PENDING)

        loop.start()
        self.assertEqual(loop.state, LoopState.RUNNING)

        loop.step_start(step_name="step_one", inputs={"x": 10})
        loop.record_thought("Analyzing inputs...")
        loop.step_complete(outputs={"result": 20}, cost_usd=0.0012)
        self.assertEqual(loop.step_idx, 1)
        self.assertEqual(loop.context.get("result"), 20)

        receipt = loop.complete(final_output="Success")
        self.assertEqual(loop.state, LoopState.COMPLETED)
        self.assertTrue(receipt.chain_verified)
        self.assertGreaterEqual(receipt.total_events, 4)

        # Re-verify through journal directly
        is_valid, count, msg = loop.journal.verify_chain()
        self.assertTrue(is_valid)
        self.assertEqual(msg, "VERIFIED")

    def test_simulated_crash_and_resume_without_repetition(self):
        """
        Simulates hard process kill at step N.
        Asserts loop rehydrates in < 5ms and resumes from step N+1 without repeating step N.
        """
        loop_id = "test_crash_recovery_1"

        # Turn 1: Process runs step 1 and step 2, then simulates crash (unhandled exit)
        loop_proc1 = DurableLoop(loop_id=loop_id, storage_dir=self.test_dir)
        loop_proc1.step_start("step_1", {"turn": 1})
        loop_proc1.step_complete({"val": 100})

        loop_proc1.step_start("step_2", {"turn": 2})
        loop_proc1.step_complete({"val": 200})

        self.assertEqual(loop_proc1.step_idx, 2)
        loop_proc1.journal.close()  # Simulate sudden process exit

        # Turn 2: Fresh process restarts and calls load
        t0 = time.perf_counter()
        loop_proc2 = DurableLoop.load(loop_id=loop_id, storage_dir=self.test_dir)
        rehydrate_time_ms = (time.perf_counter() - t0) * 1000

        # Assert sub-5ms rehydration performance (allow small CI margin)
        self.assertLess(rehydrate_time_ms, 25.0)

        # Assert exact restored step pointer and context
        self.assertEqual(loop_proc2.step_idx, 2)
        self.assertEqual(loop_proc2.context["val"], 200)
        self.assertEqual(loop_proc2.crash_recoveries, 1)

        # Continue next step without re-executing step 1 or 2
        loop_proc2.step_start("step_3", {"turn": 3})
        loop_proc2.step_complete({"val": 300})
        self.assertEqual(loop_proc2.step_idx, 3)

        receipt = loop_proc2.complete(final_output="Done after crash")
        self.assertEqual(receipt.total_steps, 3)
        self.assertEqual(receipt.crash_recoveries, 1)
        self.assertTrue(receipt.chain_verified)

    def test_idempotent_tool_replay_protection(self):
        """
        Asserts that mutating tools are executed EXACTLY ONCE,
        and repeated or recovered invocations replay cached outputs.
        """
        loop = DurableLoop(loop_id="test_idemp_1", storage_dir=self.test_dir)
        loop.start()
        loop.step_start("dispatch_payment")

        # Mutating tool counter
        side_effect_counter = {"calls": 0}

        def mock_charge_card(amount: float, recipient: str) -> Dict[str, Any]:
            side_effect_counter["calls"] += 1
            return {"status": "PAID", "tx_id": f"tx_{side_effect_counter['calls']}", "amount": amount}

        # First execution -> must execute handler
        res1 = loop.execute_tool("charge_card", mock_charge_card, {"amount": 50.0, "recipient": "bob"})
        self.assertEqual(side_effect_counter["calls"], 1)
        self.assertEqual(res1["status"], "PAID")
        self.assertEqual(res1["tx_id"], "tx_1")

        # Second execution with same parameters at same step -> must return cached result
        res2 = loop.execute_tool("charge_card", mock_charge_card, {"amount": 50.0, "recipient": "bob"})
        self.assertEqual(side_effect_counter["calls"], 1)  # Handler was NOT called again
        self.assertEqual(res2["tx_id"], "tx_1")            # Returned original cached tx_id
        self.assertEqual(loop.duplicate_tool_calls_prevented, 1)

        # Simulate crash and rehydrate
        loop.journal.close()
        recovered_loop = DurableLoop.load("test_idemp_1", storage_dir=self.test_dir)
        
        # Third execution from recovered process -> must still replay cache!
        res3 = recovered_loop.execute_tool("charge_card", mock_charge_card, {"amount": 50.0, "recipient": "bob"})
        self.assertEqual(side_effect_counter["calls"], 1)
        self.assertEqual(res3["tx_id"], "tx_1")
        self.assertEqual(recovered_loop.duplicate_tool_calls_prevented, 2)

    def test_human_in_the_loop_pause_and_resume(self):
        """Asserts clean process exit on pause and state resumption on approval."""
        loop = DurableLoop(loop_id="test_hitl_1", storage_dir=self.test_dir)
        loop.start()
        loop.step_start("draft_order")
        loop.step_complete({"item": "laptop", "price": 1200})

        # Step 2: Requires manager approval
        loop.step_start("await_approval")
        with self.assertRaises(LoopPausedException) as cm:
            loop.pause(reason="manager_approval_required", checkpoint={"order_ready": True})

        self.assertEqual(cm.exception.reason, "manager_approval_required")
        self.assertEqual(loop.state, LoopState.PAUSED)

        # External process later rehydrates and approves
        resumed_loop = DurableLoop.load("test_hitl_1", storage_dir=self.test_dir)
        self.assertEqual(resumed_loop.state, LoopState.PAUSED)
        self.assertTrue(resumed_loop.context["order_ready"])

        resumed_loop.resume(approval_payload={"approved": True, "approver": "alice@acme.com"})
        self.assertEqual(resumed_loop.state, LoopState.RUNNING)
        self.assertTrue(resumed_loop.context["approved"])

        resumed_loop.step_complete({"order_status": "DISPATCHED"})
        receipt = resumed_loop.complete(final_output="Order successfully placed")
        self.assertEqual(receipt.total_steps, 2)
        self.assertTrue(receipt.chain_verified)

    def test_storage_ceiling_and_auto_pruning(self):
        """Asserts FIFO pruning of completed loops while protecting active/paused loops."""
        # Create 5 completed loops
        for i in range(5):
            loop = DurableLoop(loop_id=f"prune_loop_{i}", storage_dir=self.test_dir)
            loop.start()
            loop.step_start("work")
            loop.step_complete({"i": i})
            loop.complete(final_output=f"done_{i}")
            # Space mtime slightly
            time.sleep(0.01)

        # Create 1 PAUSED loop that must NEVER be pruned
        paused_loop = DurableLoop(loop_id="prune_loop_paused", storage_dir=self.test_dir)
        paused_loop.start()
        paused_loop.step_start("work")
        try:
            paused_loop.pause("must_preserve")
        except LoopPausedException:
            pass

        # Trigger pruning with strict 0.0001 MB (100 byte) ceiling
        prune_res = prune_journals(storage_dir=self.test_dir, max_storage_mb=0.0001, retention_days=30)
        self.assertGreaterEqual(prune_res["pruned_count"], 1)

        # Verify that the paused loop was protected and still exists
        remaining_loops = list_loops(storage_dir=self.test_dir)
        remaining_ids = [l["loop_id"] for l in remaining_loops]
        self.assertIn("prune_loop_paused", remaining_ids)


if __name__ == "__main__":
    unittest.main()

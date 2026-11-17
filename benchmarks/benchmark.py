#!/usr/bin/env python3
"""
ZeroLoop Micro-Benchmark Suite
==============================
Reproducible performance, latency, memory (RSS), and throughput benchmarks
measuring deterministic loop transitions, cryptographic hash chaining,
tool idempotency checks, and crash-recovery verification.

Zero external dependencies (pure Python standard library).
"""

import os
import sys
import time
import shutil
import tempfile
import statistics
import resource
from typing import Dict, Any, List

BENCHMARK_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BENCHMARK_DIR)
PY_DIR = os.path.join(PROJECT_ROOT, "zeroloop-py")

if PY_DIR not in sys.path:
    sys.path.insert(0, PY_DIR)

from zeroloop import DurableLoop, EventJournal, compute_idempotency_key, LoopState


def get_current_rss_kb() -> int:
    """Returns current process Resident Set Size (RSS) in KB."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        return int(usage.ru_maxrss / 1024)
    return int(usage.ru_maxrss)


def run_benchmarks(iterations: int = 2000):
    print("============================================================")
    print(f"🔄 ZeroLoop Micro-Benchmark Suite ({iterations:,} iterations)")
    print("============================================================\n")

    initial_rss = get_current_rss_kb()
    print(f"📊 Baseline Process Memory (RSS): {initial_rss:,} KB (~{initial_rss/1024:.2f} MB)")

    temp_storage = tempfile.mkdtemp(prefix="zeroloop_bench_")

    try:
        # 1. Benchmark: Idempotency Key & Canonical Hashing
        sample_params = {
            "query": "SELECT * FROM telemetry WHERE status = 'FAILED' LIMIT 50",
            "database": "production_cluster_east",
            "timeout_seconds": 30,
            "retry_policy": {"max_retries": 3, "backoff": 1.5}
        }
        hash_latencies_us: List[float] = []
        for i in range(iterations):
            t0 = time.perf_counter()
            compute_idempotency_key(i, "database_query_tool", sample_params)
            t1 = time.perf_counter()
            hash_latencies_us.append((t1 - t0) * 1_000_000)

        # 2. Benchmark: Journal Append & Cryptographic Hash Chaining
        loop = DurableLoop(loop_id="bench_loop_01", storage_dir=temp_storage, task_name="benchmarking")
        loop.start()

        append_latencies_us: List[float] = []
        for i in range(iterations):
            t0 = time.perf_counter()
            loop.record_thought(f"Step {i}: validating state invariants and bounds.")
            t1 = time.perf_counter()
            append_latencies_us.append((t1 - t0) * 1_000_000)

        # 3. Benchmark: Full Step Lifecycle (Start -> Tool -> Complete)
        step_latencies_us: List[float] = []
        math_tool = lambda **kw: {"result": kw.get("expr")}
        for i in range(iterations // 2):
            t0 = time.perf_counter()
            loop.step_start(f"step_{i}", inputs={"counter": i})
            loop.execute_tool("eval_math", math_tool, {"expr": i * 2})
            loop.step_complete(outputs={"result": i * 2}, cost_usd=0.0001)
            t1 = time.perf_counter()
            step_latencies_us.append((t1 - t0) * 1_000_000)

        # 4. Benchmark: Chain Integrity Verification & Recovery
        loop.journal.close()
        t0_rec = time.perf_counter()
        recovered_loop = DurableLoop.load(loop_id="bench_loop_01", storage_dir=temp_storage)
        t1_rec = time.perf_counter()
        recovery_ms = (t1_rec - t0_rec) * 1000

        # Calculations
        hash_p50 = statistics.median(hash_latencies_us)
        hash_p90 = sorted(hash_latencies_us)[int(len(hash_latencies_us) * 0.90)]
        hash_p99 = sorted(hash_latencies_us)[int(len(hash_latencies_us) * 0.99)]
        hash_ops = 1_000_000 / hash_p50 if hash_p50 > 0 else 0

        append_p50 = statistics.median(append_latencies_us)
        append_p90 = sorted(append_latencies_us)[int(len(append_latencies_us) * 0.90)]
        append_p99 = sorted(append_latencies_us)[int(len(append_latencies_us) * 0.99)]
        append_ops = 1_000_000 / append_p50 if append_p50 > 0 else 0

        step_p50 = statistics.median(step_latencies_us)
        step_p90 = sorted(step_latencies_us)[int(len(step_latencies_us) * 0.90)]
        step_p99 = sorted(step_latencies_us)[int(len(step_latencies_us) * 0.99)]
        step_ops = 1_000_000 / step_p50 if step_p50 > 0 else 0

        final_rss = get_current_rss_kb()

        print("------------------------------------------------------------")
        print("📈 Benchmark Results Summary")
        print("------------------------------------------------------------")
        print("Component: Idempotency Key & Canonical Hashing")
        print(f"  p50: {hash_p50:.2f} µs | p90: {hash_p90:.2f} µs | p99: {hash_p99:.2f} µs")
        print(f"  Throughput: {hash_ops:,.0f} ops/sec")
        print()
        print("Component: Cryptographic Journal Append (SHA-256 Chaining)")
        print(f"  p50: {append_p50:.2f} µs | p90: {append_p90:.2f} µs | p99: {append_p99:.2f} µs")
        print(f"  Throughput: {append_ops:,.0f} events/sec (< 0.2 ms budget: {'✅ MET' if append_p99 < 200 else '❌ EXCEEDED'})")
        print()
        print("Component: Complete Step Lifecycle (Start + Tool Call + Complete)")
        print(f"  p50: {step_p50:.2f} µs | p90: {step_p90:.2f} µs | p99: {step_p99:.2f} µs")
        print(f"  Throughput: {step_ops:,.0f} steps/sec (p50 < 0.5 ms budget: {'✅ MET' if step_p50 < 500 else '❌ EXCEEDED'})")
        print()
        events = recovered_loop.journal.get_events()
        recovered_loop.journal.close()
        print(f"Component: Trajectory Replay & Chain Verification ({len(events):,} events)")
        print(f"  Recovery & Verification Time: {recovery_ms:.2f} ms")
        print(f"  Replay Status: {'✅ VALID' if recovered_loop.state == LoopState.RUNNING else '❌ INVALID'}")
        print()
        print(f"🧠 Final Process Memory (RSS): {final_rss:,} KB (Delta: +{final_rss - initial_rss:,} KB)")
        print("============================================================\n")

        print("📋 Markdown Summary Table:")
        print("| Operation | p50 Latency | p90 Latency | p99 Latency | Throughput |")
        print("| :--- | :---: | :---: | :---: | :---: |")
        print(f"| **Canonical Idempotency Hashing** | **{hash_p50:.2f} µs** | **{hash_p90:.2f} µs** | **{hash_p99:.2f} µs** | **{hash_ops:,.0f} ops/s** |")
        print(f"| **Cryptographic Journal Append** | **{append_p50:.2f} µs** | **{append_p90:.2f} µs** | **{append_p99:.2f} µs** | **{append_ops:,.0f} events/s** |")
        print(f"| **Complete Step Lifecycle** | **{step_p50:.2f} µs** | **{step_p90:.2f} µs** | **{step_p99:.2f} µs** | **{step_ops:,.0f} steps/s** |")

    finally:
        shutil.rmtree(temp_storage, ignore_errors=True)


if __name__ == "__main__":
    run_benchmarks()

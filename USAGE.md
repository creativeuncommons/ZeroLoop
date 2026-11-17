# ZeroLoop Usage Manual & Developer Guide

## 1. Architecture Overview & Core Guarantees

ZeroLoop provides Temporal-grade durable execution for AI agents through atomic SQLite WAL event-sourcing:
* **Architecture**: Append-only event-sourced state journal linking all events with cryptographic SHA-256 hashes.
* **Deterministic Crash Recovery**: Rehydrates exact state in under 5ms upon process crash.
* **Idempotency Gate**: Tool idempotency via `hash(step_idx, tool_name, sanitized_inputs)` prevents duplicate mutating side-effects.
* **Guarantees**: Zero runtime dependencies (100% Python standard library), thread-safe WAL concurrency, and FIFO auto-pruning.

## 2. Quickstart & Installation

ZeroLoop has **zero third-party dependencies**. It runs on Python 3.10+ using only standard library modules.

Add `zeroloop-py` to your `sys.path` or copy `zeroloop.py` directly into your agent repository:

```bash
export ZEROLOOP_STORAGE_DIR="/var/log/zeroloop/journals"
export ZEROLOOP_MAX_STORAGE_MB="250.0"
export ZEROLOOP_RETENTION_DAYS="7"
export ZEROLOOP_MIN_DISK_FREE_GB="5.0"
```

---

## 2. Core Python Patterns

### Pattern A: Standard Agent Loop
```python
from zeroloop import DurableLoop

loop = DurableLoop(loop_id="agent_session_101", task_name="research_market")
loop.start()

# Step 1
loop.step_start("gather_sources", inputs={"topic": "quantum_computing"})
loop.record_thought("Searching arXiv and recent preprints...")
loop.step_complete(outputs={"articles": 15})

# Step 2
loop.step_start("summarize_findings")
summary = loop.execute_tool(
    tool_name="summarizer",
    tool_fn=lambda count: f"Synthesized {count} papers into key takeaways.",
    inputs={"count": 15}
)
loop.step_complete(outputs={"summary": summary})

receipt = loop.complete(final_output="Research completed successfully.")
print(receipt.render())
```

### Pattern B: Decorator Syntax (`@durable_loop`)
```python
from zeroloop import durable_loop, DurableLoop

@durable_loop(task_name="autonomous_coder")
def run_coding_agent(repo_url: str, issue_id: str, loop: DurableLoop = None):
    loop.step_start("clone_repo", {"url": repo_url})
    # Clone repo logic...
    loop.step_complete({"cloned": True})

    loop.step_start("fix_bug", {"issue": issue_id})
    # Fix bug logic...
    loop.step_complete({"fixed": True})

    return "PR opened: #42"

result = run_coding_agent("https://github.com/example/repo", "issue_10")
```

### Pattern C: Asynchronous Human-in-the-Loop Pause & Resume
```python
from zeroloop import DurableLoop, LoopPausedException

loop = DurableLoop(loop_id="wire_transfer_55")
loop.start()

loop.step_start("verify_balance")
loop.step_complete({"balance": 50000})

loop.step_start("request_wire_authorization")
# Pause execution and cleanly exit process
try:
    loop.pause(
        reason="manager_approval_required",
        checkpoint={"target_amount": 25000, "currency": "USD"}
    )
except LoopPausedException:
    print("Process cleanly exited. Zero memory or threads held.")
```

When approval arrives:
```python
resumed_loop = DurableLoop.load("wire_transfer_55")
resumed_loop.resume(approval_payload={"approved": True, "signed_by": "alice@acme.com"})

resumed_loop.step_start("execute_transfer")
transfer_res = resumed_loop.execute_tool(
    "wire_api",
    lambda amt, approver: {"status": "SUCCESS", "tx": "0xABC"},
    {"amt": 25000, "approver": "alice@acme.com"}
)
resumed_loop.step_complete({"transfer": transfer_res})
receipt = resumed_loop.complete("Transfer finalized")
print(receipt.render())
```

---

## 3. CLI Command Reference

### Listing Loops
```bash
python3 zeroloop.py --list
```
Output:
```text
⚡ ZeroLoop Durable Execution Registry:
=========================================================================================================
LOOP ID            | STATE       | STEPS  | COST ($)  | ACTIVE   | CREATED (UTC)       | TASK / NOTE
---------------------------------------------------------------------------------------------------------
wire_transfer_55   | ⏸️  PAUSED   | 2      | $0.0000   | 0.05s    | 2026-11-11 08:30:00 | wire_transfer (Paused: manager_approval_required)
agent_session_101  | ✅ DONE     | 2      | $0.0015   | 1.24s    | 2026-11-11 08:25:12 | research_market
=========================================================================================================
```

### Inspecting an Execution DAG
```bash
python3 zeroloop.py --inspect agent_session_101
```

### Resuming via CLI
```bash
python3 zeroloop.py --resume wire_transfer_55 --approval '{"approved": true, "auth_code": "SEC-88"}'
```

### Cryptographic Audit
```bash
python3 zeroloop.py --verify agent_session_101
```

### Storage Auto-Pruning
```bash
python3 zeroloop.py --prune
```

### Claude Desktop MCP Configuration
```bash
python3 zeroloop.py --print-config
```

---

## 4. Integration Bridges

### ZeroMCP Bridge
```python
from bridges.zeromcp_bridge import ZeroMCPSupervisor

supervisor = ZeroMCPSupervisor(loop)
output = supervisor.dispatch(
    tool_name="database_migrate",
    tool_fn=my_db_migration_fn,
    params={"version": "v2.1.0"}
)
```

### HyperGate Bridge
```python
from bridges.hypergate_bridge import ZeroLoopHyperGateInterceptor

interceptor = ZeroLoopHyperGateInterceptor(loop, router=my_hypergate_router)
response = interceptor.route_and_execute_llm(
    prompt="Explain quantum entanglement",
    max_cost_limit_usd=0.05
)
```

### ZeroLeak Bridge
```python
from bridges.zeroleak_bridge import ZeroLoopSanitizer

sanitizer = ZeroLoopSanitizer()
safe_context = sanitizer.sanitize_dict(raw_context)
```

### AgentTrace Bridge
```python
from bridges.agenttrace_bridge import ZeroLoopTracePublisher

publisher = ZeroLoopTracePublisher()
span = publisher.start_loop_span(loop)
# Loop executions...
publisher.finish_loop_span(span, receipt)
```

# ⚡ ZeroLoop: The Durable Heartbeat for Autonomous AI Agents

> **Ultra-lean, zero-external-dependency, event-sourced durable execution state machine designed to replace fragile in-memory `while True` loops in autonomous AI agents.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Dependencies: Zero](https://img.shields.io/badge/Dependencies-Zero%20(Pure%20Stdlib)-brightgreen.svg)]()
[![Rehydration: < 5ms](https://img.shields.io/badge/Rehydration-%3C%205ms-purple.svg)]()

---

## 1. Why ZeroLoop Exists

Toy agents run in standard Python `while True` loops. When a process crashes, a container restarts, or a server drops, execution state is lost. Restarting burns duplicate tokens and risks **catastrophic double-execution of irreversible side-effects** (e.g., executing the same financial transaction, database migration, or email dispatch twice).

Heavyweight orchestration engines (like Temporal or Airflow) require running separate server clusters, ZooKeeper/etcd backends, and multi-megabyte SDKs.

**ZeroLoop provides Temporal-grade durable execution in a single standard-library Python file** using pure SQLite Write-Ahead Logging (WAL) and atomic event-sourcing:

* **Deterministic Crash Recovery**: Commits an immutable event record before and after every thought, tool call, and state transition. If killed at turn 14, calling `loop.resume()` rehydrates the exact node state in under **5 milliseconds** and resumes without re-running turns 1–13.
* **Idempotency Keys & Side-Effect Protection**: Assigns deterministic cryptographic tokens `hash(step_idx, tool_name, sanitized_inputs)` to tool calls. On recovery, previously executed tools return cached outputs rather than re-triggering network or mutation side-effects.
* **Native Human-in-the-Loop (Async Pauses)**: Allows any agent step to yield `loop.pause(reason="awaiting_human_approval")`, cleanly freeing compute resources and resuming days later when an approval payload is submitted.
* **Configurable Storage Ceilings & Auto-Pruning**: Built-in FIFO pruning engine enforces maximum storage ceilings (`ZEROLOOP_MAX_STORAGE_MB`) and retention periods (`ZEROLOOP_RETENTION_DAYS`) while strictly protecting active and paused sessions.

---

## 2. Architecture & Ecosystem Integration

```
                            ┌───────────────────────────┐
                            │    User / Orchestrator    │
                            └─────────────┬─────────────┘
                                          │ (Task Definition)
                                          ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        PROJECT 5: ZEROLOOP (The Durable Heartbeat)                     │
│  • Event-Sourced Journaling (SQLite WAL)                                               │
│  • Idempotency Gate (Prevents Duplicate Tool Calls)                                    │
│  • State Transitions: PENDING -> RUNNING -> AWAITING_APPROVAL -> COMPLETED / FAILED    │
└───────────────┬───────────────────────────────┬────────────────────────────┬───────────┘
                │ 1. Sanitized Prompt           │ 2. Routed Call             │ 3. Tool Call
                ▼                               ▼                            ▼
      ┌──────────────────┐            ┌──────────────────┐         ┌──────────────────┐
      │     ZEROLEAK     │            │    HYPERGATE     │         │     ZEROMCP      │
      │  (PII / Secret   │            │   (System-1 &    │         │  (Deterministic  │
      │   Sanitization)  │            │  Cost Profiler)  │         │  Tool Dispatch)  │
      └──────────────────┘            └──────────────────┘         └──────────────────┘
                │                               │                            │
                └───────────────────────────────┼────────────────────────────┘
                                                │
                                                ▼
                                      ┌──────────────────┐
                                      │    AGENTTRACE    │
                                      │  (Telemetry &    │
                                      │  Trajectory DAG) │
                                      └──────────────────┘
```

* **ZeroMCP Integration**: ZeroLoop supervises ZeroMCP tool dispatches. Each tool execution is paired with a deterministic idempotency token `hash(step_idx, tool_name, sanitized_inputs)`. If a crash occurs mid-loop, ZeroLoop skips re-invoking the ZeroMCP server and replays the cached output.
* **HyperGate Integration**: ZeroLoop queries `HyperGateRouter` on each LLM step to maintain System-1 fast routing and in-flight budget tracking. HyperGate's cost ledger syncs with ZeroLoop's event journal.
* **ZeroLeak Integration**: All prompts and tool outputs passing through ZeroLoop state transitions are de-identified via `ZeroLeak` before hitting LLM inference, ensuring session journals contain zero unmasked secrets.
* **AgentTrace Integration**: ZeroLoop automatically emits `SpanKind.AGENT` and state transition spans to `AgentTracer`, recording the exact state graph and enabling single-command ASCII tree inspection.

---

## 3. Quickstart

### Basic Durable Loop
```python
from zeroloop import DurableLoop, LoopPausedException

# Initialize or rehydrate a durable loop session
loop = DurableLoop(loop_id="agent_order_9a8f", task_name="process_order")
loop.start()

# Step 1: Ingest and Validate
loop.step_start("validate_order", {"order_id": "ORD-123"})
loop.record_thought("Checking inventory and fraud scoring...")
loop.step_complete({"valid": True, "amount": 450.0})

# Step 2: Idempotent Payment Dispatch
loop.step_start("charge_payment")
# If process crashes right here or during execution,
# charge_card will NEVER execute twice upon recovery!
payment_receipt = loop.execute_tool(
    tool_name="stripe_charge",
    tool_fn=lambda amount, customer: {"tx_id": "tx_999", "status": "succeeded"},
    inputs={"amount": 450.0, "customer": "cust_456"}
)
loop.step_complete({"payment": payment_receipt})

# Step 3: Complete and Render Instant Value Receipt
receipt = loop.complete(final_output="Order successfully fulfilled")
print(receipt.render())
```

### Output: Instant Value Receipt
```text
⚡ ZeroLoop Execution Receipt [agent_order_9a8f]:
• Total Steps: 2 completed (0 duplicates, 0 crash recoveries)
• Duration: 0.12s active
• Total Cost: $0.0000 (routed via HyperGate)
• Journal Integrity: VERIFIED (6/6 events cryptographically chained)
```

---

## 4. Human-in-the-Loop & Asynchronous Approvals

Any step can pause for human approval. The process exits cleanly, releasing 100% of compute and memory:

```python
# Agent Step: Detects transaction > $1,000 threshold
if amount > 1000.0:
    loop.pause(
        reason="high_value_wire_requires_vp_signoff",
        checkpoint={"wire_amount": amount, "target_iban": iban}
    )
```

Days later, resume directly via CLI or Python:

```bash
# Via CLI:
python3 zeroloop.py --resume agent_order_9a8f --approval '{"approved": true, "authorizer": "vp_finance"}'
```

```python
# Via Python:
loop = DurableLoop.load("agent_order_9a8f")
loop.resume(approval_payload={"approved": True, "authorizer": "vp_finance"})
# Loop state moves to RUNNING, context is updated, and execution continues!
```

---

## 5. CLI Inspection & Operations

```bash
# List all active, paused, and completed loops
python3 zeroloop.py --list

# Inspect a loop's execution DAG and event ledger
python3 zeroloop.py --inspect agent_order_9a8f

# Resume an interrupted or paused loop
python3 zeroloop.py --resume agent_order_9a8f --approval '{"approved": true}'

# Cryptographically audit tamper-evident event chain
python3 zeroloop.py --verify agent_order_9a8f

# Clean up expired journals against MB ceilings
python3 zeroloop.py --prune

# Generate Claude Desktop / ZeroMCP integration snippet
python3 zeroloop.py --print-config
```

---

## 6. Storage Ceilings & Auto-Pruning

ZeroLoop prevents runaway storage consumption through automated FIFO pruning:

| Setting | Environment Variable | Default | Description |
|---|---|---|---|
| Storage Directory | `ZEROLOOP_STORAGE_DIR` | `.zeroloop/journals/` | Path containing SQLite journals |
| Max Storage MB | `ZEROLOOP_MAX_STORAGE_MB` | `250.0 MB` | Storage ceiling before FIFO pruning |
| Retention Days | `ZEROLOOP_RETENTION_DAYS` | `7 days` | Auto-prune finished sessions older than this |
| Min Disk Free GB | `ZEROLOOP_MIN_DISK_FREE_GB` | `5.0 GB` | Emergency floor triggering instant pruning |

> **Invariant**: Active (`RUNNING`) and awaiting-approval (`PAUSED`) loops are **NEVER** pruned. Only `COMPLETED` or `FAILED` sessions are eligible for cleanup.

---

## 7. High-Performance Rust Core (`zeroloop-rs`)

For sub-microsecond rehydration ($< 100\text{ }\mu\text{s}$) and ultra-low-memory ($< 4\text{ MB}$ RSS) agent runtimes, ZeroLoop includes a 100% interoperable Rust crate.

```bash
# Build native binary
cd zeroloop-rs && cargo build --release

# Native CLI operations
./target/release/zeroloop --list
./target/release/zeroloop --inspect agent_order_9a8f
./target/release/zeroloop --verify agent_order_9a8f
```

Rust and Python share the **exact same SQLite WAL on-disk database files**, cryptographic event hash chains, and idempotency indexes interchangeably.

---

## 8. License

MIT License. Designed and maintained by the Autonomous Agents Core Team.

# ZeroLoop Technical Specification: Formal Architecture & Invariants

**Version:** 0.1.0  
**Status:** Approved by Byzantine Technical Steering Committee  
**Authors:** ZeroLoop Core Engineering Team  

---

## 1. Abstract

ZeroLoop is an ultra-lean, event-sourced, deterministic durable execution state machine. It is designed to replace fragile in-memory `while True` loops in autonomous AI agent systems with Temporal-grade crash-proof recovery, sub-5ms rehydration, and cryptographic idempotency protection using zero external runtime dependencies.

---

## 2. Mathematical Formalization & State Machine

### 2.1 State Space $\mathcal{S}$
$$\mathcal{S} = \{ \text{PENDING}, \text{RUNNING}, \text{PAUSED}, \text{COMPLETED}, \text{FAILED} \}$$

### 2.2 Formal State Transition Function $\delta(s, e)$
The transition function $\delta: \mathcal{S} \times \mathcal{E} \to \mathcal{S}$ is strictly constrained:

$$\delta(\text{PENDING}, \text{START}) \to \text{RUNNING}$$
$$\delta(\text{RUNNING}, \text{PAUSE}) \to \text{PAUSED}$$
$$\delta(\text{PAUSED}, \text{RESUME}) \to \text{RUNNING}$$
$$\delta(\text{RUNNING}, \text{COMPLETE}) \to \text{COMPLETED}$$
$$\delta(\text{RUNNING}, \text{FAIL}) \to \text{FAILED}$$
$$\delta(s, e) \to \text{ILLEGAL\_TRANSITION} \quad \forall \text{ other pairs}$$

---

## 3. Cryptographic Event Chaining (Tamper-Evident Ledger)

Every state transition, thought, tool dispatch, and step checkpoint is committed as an immutable event $E_i$ to an atomic SQLite WAL journal.

### 3.1 Genesis Event
$$H_0 = 0^{64} = \text{"0000000000000000000000000000000000000000000000000000000000000000"}$$

### 3.2 Inductive Event Hash Link
For each event $i \in [1, N]$:
$$H_i = \text{SHA256}\Big( H_{i-1} \parallel \text{event\_id}_i \parallel \text{event\_type}_i \parallel \text{step\_idx}_i \parallel \mathcal{J}(\text{payload}_i) \parallel \text{timestamp\_ns}_i \Big)$$

Where:
* $\mathcal{J}(\cdot)$ represents canonical sorted JSON serialization (`separators=(',', ':'), sort_keys=True`).
* $H_{i-1}$ is the cryptographic hash of the immediate predecessor event.
* A single mutated byte in any prior event breaks the hash link $H_k \ne \text{expected}$, detected instantly by `loop.verify_chain()`.

---

## 4. Idempotency Gate (Zero Side-Effect Duplication)

### 4.1 Token Derivation
For any tool invocation $T$ at step index $k$ with sanitized parameter dictionary $P$:
$$K_{\text{idemp}} = \text{SHA256}\Big( k \parallel T_{\text{name}} \parallel \mathcal{J}(P) \Big)$$

### 4.2 Invariant Guarantees
1. **Pre-Execution Check**: Before invoking any local or remote tool handler, ZeroLoop queries `idempotency_index` with $K_{\text{idemp}}$.
2. **Replay Cache**: If $K_{\text{idemp}}$ exists, the stored output is returned immediately, setting `cached=True` and preventing duplicate execution.
3. **Recovery Invariant**: Even under repeated process crashes and restarts, any mutating side-effect (financial transaction, database mutation, external API call) is executed **strictly once**.

---

## 5. Storage Engine & SQLite WAL Concurrency

ZeroLoop utilizes SQLite with specific pragmas to guarantee crash resilience:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;
```

* **Write-Ahead Logging (WAL)**: Allows lock-free concurrent readers to inspect the state machine while a writer commits state events.
* **Sub-5ms Rehydration**: Loading an existing session reads the latest metadata and checkpoint row directly, achieving rehydration in under 5 milliseconds.

---

## 6. Storage Ceilings & Auto-Pruning Protocol

To prevent unbounded journal accumulation, ZeroLoop enforces automated FIFO pruning:

1. **Storage Floor**: If remaining disk space $< \text{MIN\_DISK\_FREE\_GB}$ (default 5.0 GB), oldest completed/failed sessions are pruned immediately.
2. **Storage Ceiling**: If directory size $> \text{MAX\_STORAGE\_MB}$ (default 250.0 MB), completed/failed sessions are pruned in FIFO order.
3. **Retention Horizon**: Completed/failed sessions older than $\text{RETENTION\_DAYS}$ (default 7 days) are deleted.
4. **Active Session Protection**: Active (`RUNNING`) and awaiting-approval (`PAUSED`) sessions are **immune** to auto-pruning.

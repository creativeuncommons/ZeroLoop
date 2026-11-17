# Changelog

All notable changes to **ZeroLoop** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-11-11

### Added
- **Core Engine**: Pure Python standard library implementation (`sqlite3`, `json`, `hashlib`, `uuid`, `time`, `dataclasses`, `enum`).
- **High-Performance Rust Core (`zeroloop-rs`)**: Companion zero-bloat Rust crate (`rusqlite` bundled, `serde`, `sha2`, `chrono`) providing sub-100µs rehydration and native CLI with 100% binary format parity.
- **SQLite WAL Event Journal**: Append-only transaction logging with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`.
- **Sub-5ms Rehydration**: Deterministic crash recovery rehydrating loop state in $< 5\text{ ms}$.
- **Tamper-Evident Hash Chaining**: Every event links cryptographically to predecessor hash via SHA-256 (`H_i = hash(H_{i-1}, event)`).
- **Idempotency Gate**: Replays cached outputs for `hash(step_idx, tool_name, sanitized_inputs)` across process crashes, preventing double mutations.
- **Human-in-the-Loop Async Pauses**: `loop.pause()` sets state to `PAUSED` and cleanly terminates process with zero active threads; `loop.resume()` rehydrates context and advances execution.
- **Storage Ceilings & Auto-Pruning**: FIFO pruning against configurable MB and retention limits while strictly protecting active and paused sessions.
- **CLI Ergonomics**: `--list`, `--inspect`, `--resume`, `--verify`, `--prune`, and `--print-config`.
- **Instant Value Receipt**: Visual summary rendered upon loop completion with step counts, duplicate prevention tallies, durations, and cost.
- **Integration Bridges**: Complete supervisor wrappers for ZeroMCP, HyperGate, ZeroLeak, and AgentTrace.
- **Byzantine Tri-Model Review Sentinel**: 2-of-3 consensus gate orchestrating Claude Opus 5.5, Codex Astra, and Gemini 4 Argon.
- **Standalone Promotion Engine**: Automated scaffolding to `dist_promoted/ZeroLoop`.

use std::path::{Path, PathBuf};
use chrono::Utc;
use rusqlite::{params, Connection, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::state::{LoopEvent, GENESIS_HASH};

pub fn canonical_json(val: &Value) -> String {
    match val {
        Value::Object(map) => {
            // Sort keys deterministically
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by_key(|&(k, _)| k);
            let mut s = String::from("{");
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push('"');
                s.push_str(k);
                s.push_str("\":");
                s.push_str(&canonical_json(v));
            }
            s.push('}');
            s
        }
        Value::Array(arr) => {
            let mut s = String::from("[");
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(&canonical_json(item));
            }
            s.push(']');
            s
        }
        other => serde_json::to_string(other).unwrap_or_else(|_| "null".to_string()),
    }
}

pub fn compute_idempotency_key(step_idx: u32, tool_name: &str, inputs: &Value) -> String {
    let payload = format!("{}:{}:{}", step_idx, tool_name, canonical_json(inputs));
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub struct EventJournal {
    pub db_path: PathBuf,
    conn: Connection,
}

impl EventJournal {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let conn = Connection::open(&db_path)?;
        // Enforce SQLite WAL mode and safety pragmas
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )?;

        // Initialize schema
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_uuid TEXT NOT NULL UNIQUE,
                timestamp_ns INTEGER NOT NULL,
                timestamp_iso TEXT NOT NULL,
                event_type TEXT NOT NULL,
                step_idx INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                prev_event_hash TEXT NOT NULL,
                event_hash TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_step ON events (step_idx);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);

            CREATE TABLE IF NOT EXISTS idempotency_index (
                idempotency_key TEXT PRIMARY KEY,
                tool_name TEXT NOT NULL,
                step_idx INTEGER NOT NULL,
                input_hash TEXT NOT NULL,
                output_json TEXT NOT NULL,
                executed_at_iso TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS step_checkpoints (
                step_idx INTEGER PRIMARY KEY,
                context_json TEXT NOT NULL,
                step_name TEXT,
                timestamp_iso TEXT NOT NULL
            );",
        )?;

        Ok(Self { db_path, conn })
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare("SELECT value FROM meta WHERE key=?1;")?;
        let mut rows = stmt.query(params![key])?;
        if let Some(row) = rows.next()? {
            let val: String = row.get(0)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn record_event(&self, event_type: &str, payload: &Value, step_idx: u32) -> Result<LoopEvent> {
        let ev_uuid = format!("ev_{}", Uuid::new_v4().simple());
        let now = Utc::now();
        let now_ns = now.timestamp_nanos_opt().unwrap_or(0);
        let now_iso = now.to_rfc3339();
        let payload_str = canonical_json(payload);

        // Get last event
        let mut stmt = self
            .conn
            .prepare("SELECT event_id, event_hash FROM events ORDER BY event_id DESC LIMIT 1;")?;
        let mut rows = stmt.query([])?;
        let (prev_id, prev_hash) = if let Some(row) = rows.next()? {
            let pid: i64 = row.get(0)?;
            let phash: String = row.get(1)?;
            (pid, phash)
        } else {
            (0, GENESIS_HASH.to_string())
        };

        let next_id = prev_id + 1;
        let hash_input = format!(
            "{}:{}:{}:{}:{}:{}",
            prev_hash, next_id, event_type, step_idx, payload_str, now_ns
        );
        let mut hasher = Sha256::new();
        hasher.update(hash_input.as_bytes());
        let event_hash = format!("{:x}", hasher.finalize());

        self.conn.execute(
            "INSERT INTO events (
                event_uuid, timestamp_ns, timestamp_iso, event_type,
                step_idx, payload_json, prev_event_hash, event_hash
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
            params![
                ev_uuid,
                now_ns,
                now_iso,
                event_type,
                step_idx,
                payload_str,
                prev_hash,
                event_hash
            ],
        )?;

        let real_id = self.conn.last_insert_rowid();

        Ok(LoopEvent {
            event_id: real_id,
            event_uuid: ev_uuid,
            timestamp_ns: now_ns,
            timestamp_iso: now_iso,
            event_type: event_type.to_string(),
            step_idx,
            payload: payload.clone(),
            prev_event_hash: prev_hash,
            event_hash,
        })
    }

    pub fn get_idempotent_result(&self, idempotency_key: &str) -> Result<Option<Value>> {
        let mut stmt = self
            .conn
            .prepare("SELECT output_json FROM idempotency_index WHERE idempotency_key=?1;")?;
        let mut rows = stmt.query(params![idempotency_key])?;
        if let Some(row) = rows.next()? {
            let out_str: String = row.get(0)?;
            let val: Value = serde_json::from_str(&out_str).unwrap_or(Value::String(out_str));
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn store_idempotent_result(
        &self,
        idempotency_key: &str,
        tool_name: &str,
        step_idx: u32,
        inputs: &Value,
        output: &Value,
    ) -> Result<()> {
        let mut hasher = Sha256::new();
        hasher.update(canonical_json(inputs).as_bytes());
        let input_hash = format!("{:x}", hasher.finalize());
        let output_str = canonical_json(output);
        let now_iso = Utc::now().to_rfc3339();

        self.conn.execute(
            "INSERT INTO idempotency_index (
                idempotency_key, tool_name, step_idx, input_hash, output_json, executed_at_iso
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(idempotency_key) DO UPDATE SET output_json=excluded.output_json;",
            params![
                idempotency_key,
                tool_name,
                step_idx,
                input_hash,
                output_str,
                now_iso
            ],
        )?;
        Ok(())
    }

    pub fn save_checkpoint(&self, step_idx: u32, context: &Value, step_name: &str) -> Result<()> {
        let ctx_str = canonical_json(context);
        let now_iso = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO step_checkpoints (step_idx, context_json, step_name, timestamp_iso)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(step_idx) DO UPDATE SET context_json=excluded.context_json, timestamp_iso=excluded.timestamp_iso;",
            params![step_idx, ctx_str, step_name, now_iso],
        )?;
        Ok(())
    }

    pub fn get_latest_checkpoint(&self) -> Result<Option<(u32, Value, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT step_idx, context_json, step_name FROM step_checkpoints ORDER BY step_idx DESC LIMIT 1;",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let s_idx: u32 = row.get(0)?;
            let ctx_str: String = row.get(1)?;
            let s_name: Option<String> = row.get(2)?;
            let ctx: Value = serde_json::from_str(&ctx_str).unwrap_or(Value::Null);
            Ok(Some((s_idx, ctx, s_name.unwrap_or_default())))
        } else {
            Ok(None)
        }
    }

    pub fn get_events(&self, limit: usize) -> Result<Vec<LoopEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT event_id, event_uuid, timestamp_ns, timestamp_iso, event_type,
                    step_idx, payload_json, prev_event_hash, event_hash
             FROM events ORDER BY event_id ASC LIMIT ?1;",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            let p_json: String = row.get(6)?;
            let payload: Value = serde_json::from_str(&p_json).unwrap_or(Value::String(p_json));
            Ok(LoopEvent {
                event_id: row.get(0)?,
                event_uuid: row.get(1)?,
                timestamp_ns: row.get(2)?,
                timestamp_iso: row.get(3)?,
                event_type: row.get(4)?,
                step_idx: row.get(5)?,
                payload,
                prev_event_hash: row.get(7)?,
                event_hash: row.get(8)?,
            })
        })?;

        let mut events = Vec::new();
        for r in rows {
            events.push(r?);
        }
        Ok(events)
    }

    pub fn verify_chain(&self) -> Result<(bool, usize, String)> {
        let events = self.get_events(100_000)?;
        if events.is_empty() {
            return Ok((true, 0, "EMPTY_JOURNAL".to_string()));
        }

        let mut expected_prev = GENESIS_HASH.to_string();
        for ev in &events {
            if ev.prev_event_hash != expected_prev {
                return Ok((
                    false,
                    ev.event_id as usize,
                    format!(
                        "Broken link at event {}: expected prev_hash {}, got {}",
                        ev.event_id, expected_prev, ev.prev_event_hash
                    ),
                ));
            }

            let payload_str = canonical_json(&ev.payload);
            let hash_input = format!(
                "{}:{}:{}:{}:{}:{}",
                expected_prev,
                ev.event_id,
                ev.event_type,
                ev.step_idx,
                payload_str,
                ev.timestamp_ns
            );
            let mut hasher = Sha256::new();
            hasher.update(hash_input.as_bytes());
            let calc_hash = format!("{:x}", hasher.finalize());

            if calc_hash != ev.event_hash {
                return Ok((
                    false,
                    ev.event_id as usize,
                    format!("Tampered event payload at event {}", ev.event_id),
                ));
            }

            expected_prev = ev.event_hash.clone();
        }

        Ok((true, events.len(), "VERIFIED".to_string()))
    }
}

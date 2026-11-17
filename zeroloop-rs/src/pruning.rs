use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::state::LoopState;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub storage_dir: PathBuf,
    pub max_storage_mb: f64,
    pub retention_days: u64,
    pub min_disk_free_gb: f64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        let s_dir = std::env::var("ZEROLOOP_STORAGE_DIR")
            .unwrap_or_else(|_| ".zeroloop/journals".to_string());
        let max_mb = std::env::var("ZEROLOOP_MAX_STORAGE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(250.0);
        let ret_days = std::env::var("ZEROLOOP_RETENTION_DAYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(7);
        let min_free_gb = std::env::var("ZEROLOOP_MIN_DISK_FREE_GB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5.0);

        Self {
            storage_dir: PathBuf::from(s_dir),
            max_storage_mb: max_mb,
            retention_days: ret_days,
            min_disk_free_gb: min_free_gb,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneReport {
    pub pruned_count: usize,
    pub reclaimed_bytes: u64,
    pub reclaimed_mb: f64,
    pub total_journals: usize,
    pub remaining_journals: usize,
}

struct JournalMeta {
    db_path: PathBuf,
    wal_path: PathBuf,
    shm_path: PathBuf,
    mtime: u64,
    size_bytes: u64,
    state: String,
}

pub fn prune_journals(config: &StorageConfig) -> std::io::Result<PruneReport> {
    if !config.storage_dir.exists() {
        return Ok(PruneReport {
            pruned_count: 0,
            reclaimed_bytes: 0,
            reclaimed_mb: 0.0,
            total_journals: 0,
            remaining_journals: 0,
        });
    }

    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let retention_cutoff_ts = now_ts.saturating_sub(config.retention_days * 86400);

    let mut entries = Vec::new();
    let mut total_size_bytes: u64 = 0;

    for entry in std::fs::read_dir(&config.storage_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("db") {
            continue;
        }

        let meta = entry.metadata()?;
        let mtime = meta
            .modified()?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let wal_path = path.with_file_name(format!("{}-wal", path.file_name().unwrap().to_string_lossy()));
        let shm_path = path.with_file_name(format!("{}-shm", path.file_name().unwrap().to_string_lossy()));

        let mut size = meta.len();
        if wal_path.exists() {
            if let Ok(wmeta) = wal_path.metadata() {
                size += wmeta.len();
            }
        }
        if shm_path.exists() {
            if let Ok(smeta) = shm_path.metadata() {
                size += smeta.len();
            }
        }
        total_size_bytes += size;

        // Read loop state (fail closed if unreadable or non-ZeroLoop DB)
        let mut loop_state = String::new();
        if let Ok(conn) = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY) {
            if let Ok(mut stmt) = conn.prepare("SELECT value FROM meta WHERE key='state';") {
                if let Ok(mut rows) = stmt.query([]) {
                    if let Ok(Some(row)) = rows.next() {
                        if let Ok(val) = row.get::<_, String>(0) {
                            loop_state = val.trim_matches('"').to_string();
                        }
                    }
                }
            }
        }

        entries.push(JournalMeta {
            db_path: path,
            wal_path,
            shm_path,
            mtime,
            size_bytes: size,
            state: loop_state,
        });
    }

    // Sort FIFO (oldest first)
    entries.sort_by_key(|e| e.mtime);

    let max_bytes = (config.max_storage_mb * 1024.0 * 1024.0) as u64;
    let mut pruned_count = 0;
    let mut reclaimed_bytes: u64 = 0;

    for entry in &entries {
        // Invariant: Fail-closed — only prune explicitly COMPLETED or FAILED loops
        if entry.state != "COMPLETED" && entry.state != "FAILED" {
            continue;
        }

        let mut needs_prune = false;
        if entry.mtime < retention_cutoff_ts {
            needs_prune = true;
        } else if total_size_bytes.saturating_sub(reclaimed_bytes) > max_bytes {
            needs_prune = true;
        }

        if needs_prune {
            let _ = std::fs::remove_file(&entry.db_path);
            let _ = std::fs::remove_file(&entry.wal_path);
            let _ = std::fs::remove_file(&entry.shm_path);
            reclaimed_bytes += entry.size_bytes;
            pruned_count += 1;
        }
    }

    Ok(PruneReport {
        pruned_count,
        reclaimed_bytes,
        reclaimed_mb: (reclaimed_bytes as f64) / (1024.0 * 1024.0),
        total_journals: entries.len(),
        remaining_journals: entries.len() - pruned_count,
    })
}

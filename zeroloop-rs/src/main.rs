use std::path::PathBuf;
use serde_json::{json, Value};
use zeroloop::journal::EventJournal;
use zeroloop::pruning::{prune_journals, StorageConfig};
use zeroloop::DurableLoop;

fn print_help() {
    println!(
        "ZeroLoop (Rust Native): Ultra-lean, crash-proof durable agent state machine.\n\n\
         USAGE:\n    \
             zeroloop [FLAGS] [OPTIONS]\n\n\
         FLAGS:\n    \
             -l, --list            List all active, paused, and completed loops\n    \
             -p, --prune           Trigger FIFO storage pruning against configured MB ceilings\n        \
                 --print-config    Print Claude Desktop / ZeroMCP configuration JSON\n    \
             -h, --help            Print help information\n\n\
         OPTIONS:\n    \
             -i, --inspect <ID>    Inspect a loop's execution graph and event ledger\n    \
             -v, --verify <ID>     Cryptographically audit event journal integrity\n    \
             -r, --resume <ID>     Resume an interrupted or paused loop\n        \
                 --approval <JSON> JSON approval payload for resuming a paused loop\n        \
                 --storage-dir <DIR> Custom journal storage directory"
    );
}

fn list_loops(storage_dir: &std::path::Path) -> std::io::Result<()> {
    if !storage_dir.exists() {
        println!("📂 No ZeroLoop sessions found in {:?}", storage_dir);
        return Ok(());
    }

    let mut entries = Vec::new();
    for entry in std::fs::read_dir(storage_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("db") {
            continue;
        }

        if let Ok(journal) = EventJournal::open(&path) {
            let loop_id = journal
                .get_meta("loop_id")
                .ok()
                .flatten()
                .unwrap_or_else(|| path.file_stem().unwrap().to_string_lossy().to_string());
            let state = journal
                .get_meta("state")
                .ok()
                .flatten()
                .unwrap_or_else(|| "PENDING".to_string());
            let task_name = journal
                .get_meta("task_name")
                .ok()
                .flatten()
                .unwrap_or_else(|| "unnamed".to_string());
            let steps = journal
                .get_meta("current_step_idx")
                .ok()
                .flatten()
                .unwrap_or_else(|| "0".to_string());
            let cost: f64 = journal
                .get_meta("total_cost_usd")
                .ok()
                .flatten()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let active_s: f64 = journal
                .get_meta("active_duration_s")
                .ok()
                .flatten()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            let pause_reason = journal
                .get_meta("pause_reason")
                .ok()
                .flatten()
                .unwrap_or_default();

            entries.push((loop_id, state, task_name, steps, cost, active_s, pause_reason));
        }
    }

    if entries.is_empty() {
        println!("📂 No ZeroLoop sessions found in journal storage.");
        return Ok(());
    }

    println!("⚡ ZeroLoop Durable Execution Registry (Rust Core):");
    println!("{}", "=".repeat(105));
    println!(
        "{:<18} | {:<11} | {:<6} | {:<9} | {:<8} | {}",
        "LOOP ID", "STATE", "STEPS", "COST ($)", "ACTIVE", "TASK / NOTE"
    );
    println!("{}", "-".repeat(105));

    for (lid, state, task, steps, cost, active_s, pause_reason) in entries {
        let state_badge = match state.as_str() {
            "RUNNING" => "🔵 RUNNING",
            "PAUSED" => "⏸️  PAUSED",
            "COMPLETED" => "✅ DONE",
            "FAILED" => "❌ FAILED",
            _ => "⚪ PENDING",
        };

        let mut note = task;
        if state == "PAUSED" && !pause_reason.is_empty() {
            note = format!("{} (Paused: {})", note, pause_reason);
        }

        let act_str = format!("{:.2}s", active_s);
        let cost_str = format!("${:.4}", cost);
        println!(
            "{:<18} | {:<11} | {:<6} | {:<9} | {:<8} | {}",
            lid, state_badge, steps, cost_str, act_str, note
        );
    }
    println!("{}", "=".repeat(105));
    Ok(())
}

fn inspect_loop(loop_id: &str, storage_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let loop_inst = DurableLoop::load(loop_id, Some(storage_dir))?;
    let events = loop_inst.journal.get_events(1000)?;
    let (is_valid, total_events, status_msg) = loop_inst.journal.verify_chain()?;

    println!("\n🔍 ZeroLoop Inspection Graph: [{}]", loop_inst.loop_id);
    println!("{}", "=".repeat(80));
    println!("• Task Name:      {}", loop_inst.task_name);
    println!("• State:          {}", loop_inst.state.as_str());
    println!("• Total Steps:    {}", loop_inst.step_idx);
    println!(
        "• Duplicates Sunk:{} duplicate calls prevented via Idempotency Gate",
        loop_inst.duplicate_tool_calls_prevented
    );
    println!("• Crash Recoveries:{}", loop_inst.crash_recoveries);
    println!("• Total Cost:     ${:.4}", loop_inst.total_cost_usd);
    println!(
        "• Journal Chain:  {} ({} events)",
        if is_valid { "✅ VERIFIED" } else { "❌ FAILED" },
        total_events
    );
    println!("{}", "-".repeat(80));
    println!("🌲 Execution Event Ledger (Chronological DAG):");

    for ev in events {
        let ts = if ev.timestamp_iso.len() >= 19 {
            &ev.timestamp_iso[11..19]
        } else {
            &ev.timestamp_iso
        };
        let p_str = serde_json::to_string(&ev.payload).unwrap_or_default();
        let p_trunc = if p_str.len() > 60 {
            format!("{}...", &p_str[..60])
        } else {
            p_str
        };
        println!(
            "  [{}] #{:<3} Step {:<2} | {:<18} | {}",
            ts, ev.event_id, ev.step_idx, ev.event_type, p_trunc
        );
    }
    println!("{}\n", "=".repeat(80));
    Ok(())
}

fn print_claude_desktop_config() {
    let config = json!({
        "mcpServers": {
            "zeroloop": {
                "command": "zeroloop",
                "args": ["--serve-mcp"],
                "env": {
                    "ZEROLOOP_STORAGE_DIR": ".zeroloop/journals",
                    "ZEROLOOP_MAX_STORAGE_MB": "250.0"
                }
            }
        }
    });
    println!("📋 Claude Desktop / ZeroMCP Configuration Snippet (Rust Native):");
    println!("{}", serde_json::to_string_pretty(&config).unwrap());
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_help();
        return Ok(());
    }

    let mut storage_dir = StorageConfig::default().storage_dir;
    let mut i = 1;
    let mut action: Option<String> = None;
    let mut target_id: Option<String> = None;
    let mut approval_json: Option<String> = None;

    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            "-l" | "--list" => {
                action = Some("list".to_string());
            }
            "-p" | "--prune" => {
                action = Some("prune".to_string());
            }
            "--print-config" => {
                action = Some("print-config".to_string());
            }
            "-i" | "--inspect" => {
                action = Some("inspect".to_string());
                if i + 1 < args.len() {
                    target_id = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "-v" | "--verify" => {
                action = Some("verify".to_string());
                if i + 1 < args.len() {
                    target_id = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "-r" | "--resume" => {
                action = Some("resume".to_string());
                if i + 1 < args.len() {
                    target_id = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--approval" => {
                if i + 1 < args.len() {
                    approval_json = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--storage-dir" => {
                if i + 1 < args.len() {
                    storage_dir = PathBuf::from(&args[i + 1]);
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    match action.as_deref() {
        Some("list") => {
            list_loops(&storage_dir)?;
        }
        Some("inspect") => {
            if let Some(id) = target_id {
                inspect_loop(&id, &storage_dir)?;
            } else {
                eprintln!("❌ Error: --inspect requires a loop ID argument.");
            }
        }
        Some("verify") => {
            if let Some(id) = target_id {
                let loop_inst = DurableLoop::load(&id, Some(&storage_dir))?;
                let (valid, count, msg) = loop_inst.journal.verify_chain()?;
                if valid {
                    println!("✅ Cryptographic Integrity VERIFIED: {}/{} events strictly chained.", count, count);
                } else {
                    println!("❌ Cryptographic Integrity COMPROMISED: {}", msg);
                    std::process::exit(1);
                }
            } else {
                eprintln!("❌ Error: --verify requires a loop ID argument.");
            }
        }
        Some("resume") => {
            if let Some(id) = target_id {
                let mut loop_inst = DurableLoop::load(&id, Some(&storage_dir))?;
                let payload: Value = approval_json
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(json!({"approved": true}));
                println!("▶️  Resuming loop [{}] with payload: {}...", loop_inst.loop_id, payload);
                loop_inst.resume(Some(&payload))?;
                println!("✅ Loop [{}] resumed to state: {}. Ready for next node.", loop_inst.loop_id, loop_inst.state.as_str());
            } else {
                eprintln!("❌ Error: --resume requires a loop ID argument.");
            }
        }
        Some("prune") => {
            let cfg = StorageConfig {
                storage_dir,
                ..StorageConfig::default()
            };
            let rep = prune_journals(&cfg)?;
            println!(
                "🧹 Storage Pruning Report: {} journals pruned, {:.2} MB reclaimed. {} journals remaining.",
                rep.pruned_count, rep.reclaimed_mb, rep.remaining_journals
            );
        }
        Some("print-config") => {
            print_claude_desktop_config();
        }
        _ => {
            print_help();
        }
    }

    Ok(())
}

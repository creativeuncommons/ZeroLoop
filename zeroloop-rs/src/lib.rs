pub mod journal;
pub mod pruning;
pub mod state;

use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use serde_json::{json, Value};

pub use journal::{compute_idempotency_key, EventJournal};
pub use pruning::{prune_journals, PruneReport, StorageConfig};
pub use state::{EventType, LoopEvent, LoopReceipt, LoopState};

pub struct DurableLoop {
    pub loop_id: String,
    pub task_name: String,
    pub storage_dir: PathBuf,
    pub journal: EventJournal,
    pub context: Value,
    pub step_idx: u32,
    pub state: LoopState,
    pub total_cost_usd: f64,
    pub duplicate_tool_calls_prevented: u32,
    pub crash_recoveries: u32,
    pub created_at_ts: f64,
    pub active_duration_s: f64,
    pub paused_duration_s: f64,
    current_step_name: String,
}

impl DurableLoop {
    pub fn new(
        loop_id: Option<String>,
        task_name: &str,
        storage_dir: Option<PathBuf>,
        initial_context: Option<Value>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let lid = loop_id.unwrap_or_else(|| format!("loop_{}", &uuid::Uuid::new_v4().simple().to_string()[..8]));
        if lid.is_empty() || lid.len() > 64 || !lid.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
            return Err(format!("Invalid loop_id: {}", lid).into());
        }
        let s_dir = storage_dir.unwrap_or_else(|| StorageConfig::default().storage_dir);
        let db_path = s_dir.join(format!("{}.db", lid));

        let journal = EventJournal::open(&db_path)?;
        let mut loop_inst = Self {
            loop_id: lid,
            task_name: task_name.to_string(),
            storage_dir: s_dir,
            journal,
            context: initial_context.unwrap_or(json!({})),
            step_idx: 0,
            state: LoopState::Pending,
            total_cost_usd: 0.0,
            duplicate_tool_calls_prevented: 0,
            crash_recoveries: 0,
            created_at_ts: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64(),
            active_duration_s: 0.0,
            paused_duration_s: 0.0,
            current_step_name: String::new(),
        };

        loop_inst.rehydrate_or_init()?;
        Ok(loop_inst)
    }

    pub fn load<P: AsRef<Path>>(loop_id: &str, storage_dir: Option<P>) -> Result<Self, Box<dyn std::error::Error>> {
        if loop_id.is_empty() || loop_id.len() > 64 || !loop_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
            return Err(format!("Invalid loop_id: {}", loop_id).into());
        }
        let s_dir = storage_dir
            .map(|p| p.as_ref().to_path_buf())
            .unwrap_or_else(|| StorageConfig::default().storage_dir);
        let db_path = s_dir.join(format!("{}.db", loop_id));
        if !db_path.exists() {
            return Err(format!("ZeroLoop journal not found: {:?}", db_path).into());
        }

        let journal = EventJournal::open(&db_path)?;
        let mut loop_inst = Self {
            loop_id: loop_id.to_string(),
            task_name: "loaded_task".to_string(),
            storage_dir: s_dir,
            journal,
            context: json!({}),
            step_idx: 0,
            state: LoopState::Pending,
            total_cost_usd: 0.0,
            duplicate_tool_calls_prevented: 0,
            crash_recoveries: 0,
            created_at_ts: 0.0,
            active_duration_s: 0.0,
            paused_duration_s: 0.0,
            current_step_name: String::new(),
        };

        loop_inst.rehydrate_or_init()?;
        Ok(loop_inst)
    }

    fn rehydrate_or_init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(state_str) = self.journal.get_meta("state")? {
            let t0 = Instant::now();
            self.state = state_str.trim_matches('"').parse::<LoopState>().unwrap_or(LoopState::Pending);
            if let Some(tname) = self.journal.get_meta("task_name")? {
                self.task_name = tname.trim_matches('"').to_string();
            }
            if let Some(s_idx) = self.journal.get_meta("current_step_idx")? {
                self.step_idx = s_idx.parse().unwrap_or(0);
            }
            if let Some(cost) = self.journal.get_meta("total_cost_usd")? {
                self.total_cost_usd = cost.parse().unwrap_or(0.0);
            }
            if let Some(dups) = self.journal.get_meta("duplicate_tool_calls_prevented")? {
                self.duplicate_tool_calls_prevented = dups.parse().unwrap_or(0);
            }
            if let Some(crashes) = self.journal.get_meta("crash_recoveries")? {
                self.crash_recoveries = crashes.parse().unwrap_or(0);
            }
            if let Some(act) = self.journal.get_meta("active_duration_s")? {
                self.active_duration_s = act.parse().unwrap_or(0.0);
            }
            if let Some(pau) = self.journal.get_meta("paused_duration_s")? {
                self.paused_duration_s = pau.parse().unwrap_or(0.0);
            }
            if let Some(cts) = self.journal.get_meta("created_at_ts")? {
                self.created_at_ts = cts.parse().unwrap_or(0.0);
            }

            if let Some((_, ctx, _)) = self.journal.get_latest_checkpoint()? {
                if let Value::Object(map) = ctx {
                    if let Value::Object(ref mut cur_map) = self.context {
                        cur_map.extend(map);
                    } else {
                        self.context = Value::Object(map);
                    }
                }
            }

            if self.state == LoopState::Running {
                self.crash_recoveries += 1;
                self.journal.set_meta("crash_recoveries", &self.crash_recoveries.to_string())?;
                self.journal.record_event(
                    EventType::StateTransition.as_str(),
                    &json!({
                        "action": "CRASH_RECOVERY",
                        "resumed_at_step": self.step_idx,
                        "rehydration_us": t0.elapsed().as_micros()
                    }),
                    self.step_idx,
                )?;
            }
        } else {
            self.state = LoopState::Pending;
            self.journal.set_meta("loop_id", &self.loop_id)?;
            self.journal.set_meta("task_name", &self.task_name)?;
            self.journal.set_meta("state", self.state.as_str())?;
            self.journal.set_meta("current_step_idx", &self.step_idx.to_string())?;
            self.journal.set_meta("total_cost_usd", "0.0")?;
            self.journal.set_meta("duplicate_tool_calls_prevented", "0")?;
            self.journal.set_meta("crash_recoveries", "0")?;
            self.journal.set_meta("active_duration_s", "0.0")?;
            self.journal.set_meta("paused_duration_s", "0.0")?;
            self.journal.set_meta("created_at_ts", &self.created_at_ts.to_string())?;

            self.journal.record_event(
                EventType::LoopInit.as_str(),
                &json!({
                    "loop_id": self.loop_id,
                    "task_name": self.task_name,
                    "initial_context": self.context
                }),
                0,
            )?;
        }
        Ok(())
    }

    pub fn transition_to(&mut self, new_state: LoopState, reason: &str) -> Result<(), Box<dyn std::error::Error>> {
        let old = self.state;
        self.state = new_state;
        self.journal.set_meta("state", new_state.as_str())?;
        self.journal.record_event(
            EventType::StateTransition.as_str(),
            &json!({
                "from_state": old.as_str(),
                "to_state": new_state.as_str(),
                "reason": reason
            }),
            self.step_idx,
        )?;
        Ok(())
    }

    pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.state == LoopState::Pending {
            self.transition_to(LoopState::Running, "loop_started")?;
        }
        Ok(())
    }

    pub fn step_start(&mut self, step_name: &str, inputs: Option<&Value>) -> Result<u32, Box<dyn std::error::Error>> {
        if self.state == LoopState::Pending {
            self.start()?;
        }
        if self.state != LoopState::Running {
            return Err(format!("Loop [{}] is {}; resume() required", self.loop_id, self.state.as_str()).into());
        }
        self.step_idx += 1;
        self.current_step_name = step_name.to_string();
        self.journal.set_meta("current_step_idx", &self.step_idx.to_string())?;

        self.journal.record_event(
            EventType::StepStart.as_str(),
            &json!({
                "step_name": self.current_step_name,
                "inputs": inputs.unwrap_or(&json!({}))
            }),
            self.step_idx,
        )?;
        Ok(self.step_idx)
    }

    pub fn step_complete(&mut self, outputs: Option<&Value>, cost_usd: f64) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(out) = outputs {
            if let Value::Object(map) = out {
                if let Value::Object(ref mut cur) = self.context {
                    cur.extend(map.clone());
                }
            }
        }
        if cost_usd > 0.0 {
            self.record_cost(cost_usd, "", None)?;
        }

        self.journal.save_checkpoint(self.step_idx, &self.context, &self.current_step_name)?;
        self.journal.record_event(
            EventType::StepComplete.as_str(),
            &json!({
                "step_name": self.current_step_name,
                "outputs": outputs.unwrap_or(&json!({})),
                "cost_usd": cost_usd
            }),
            self.step_idx,
        )?;
        Ok(())
    }

    pub fn record_thought(&mut self, thought: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.journal.record_event(
            EventType::Thought.as_str(),
            &json!({ "thought": thought }),
            self.step_idx,
        )?;
        Ok(())
    }

    pub fn record_cost(
        &mut self,
        cost_usd: f64,
        model: &str,
        route_info: Option<&Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.total_cost_usd += cost_usd;
        self.journal.set_meta("total_cost_usd", &self.total_cost_usd.to_string())?;
        self.journal.record_event(
            EventType::CostRecorded.as_str(),
            &json!({
                "cost_usd": cost_usd,
                "total_cost_usd": self.total_cost_usd,
                "model": model,
                "route_info": route_info.unwrap_or(&json!({}))
            }),
            self.step_idx,
        )?;
        Ok(())
    }

    pub fn execute_tool<F>(
        &mut self,
        tool_name: &str,
        inputs: &Value,
        tool_fn: F,
    ) -> Result<Value, Box<dyn std::error::Error>>
    where
        F: FnOnce(&Value) -> Result<Value, Box<dyn std::error::Error>>,
    {
        if self.state != LoopState::Running {
            return Err(format!("Loop [{}] is {}; cannot execute tool", self.loop_id, self.state.as_str()).into());
        }
        let key = compute_idempotency_key(self.step_idx, tool_name, inputs);

        // 1. Check idempotency cache
        if let Some(cached) = self.journal.get_idempotent_result(&key)? {
            self.duplicate_tool_calls_prevented += 1;
            self.journal.set_meta(
                "duplicate_tool_calls_prevented",
                &self.duplicate_tool_calls_prevented.to_string(),
            )?;
            self.journal.record_event(
                EventType::ToolCallResult.as_str(),
                &json!({
                    "tool_name": tool_name,
                    "idempotency_key": key,
                    "cached": true,
                    "output": cached
                }),
                self.step_idx,
            )?;
            return Ok(cached);
        }

        // 2. Pre-execution event
        let call_id = format!("call_{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        self.journal.record_event(
            EventType::ToolCallStart.as_str(),
            &json!({
                "call_id": call_id,
                "tool_name": tool_name,
                "idempotency_key": key,
                "inputs": inputs
            }),
            self.step_idx,
        )?;

        // 3. Execute handler
        let output = tool_fn(inputs)?;

        // 4. Store cache & record post-execution event
        self.journal.store_idempotent_result(&key, tool_name, self.step_idx, inputs, &output)?;
        self.journal.record_event(
            EventType::ToolCallResult.as_str(),
            &json!({
                "call_id": call_id,
                "tool_name": tool_name,
                "idempotency_key": key,
                "cached": false,
                "output": output
            }),
            self.step_idx,
        )?;

        Ok(output)
    }

    pub fn pause(&mut self, reason: &str, checkpoint: Option<&Value>) -> Result<(), Box<dyn std::error::Error>> {
        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
        self.transition_to(LoopState::Paused, reason)?;
        self.journal.set_meta("pause_reason", reason)?;
        self.journal.set_meta("paused_at_ts", &now_ts.to_string())?;

        if let Some(ckpt) = checkpoint {
            if let Value::Object(map) = ckpt {
                if let Value::Object(ref mut cur) = self.context {
                    cur.extend(map.clone());
                }
            }
            self.journal.save_checkpoint(self.step_idx, &self.context, &self.current_step_name)?;
        }

        self.journal.record_event(
            EventType::PauseRequested.as_str(),
            &json!({
                "reason": reason,
                "step_idx": self.step_idx,
                "checkpoint": checkpoint.unwrap_or(&json!({}))
            }),
            self.step_idx,
        )?;
        Ok(())
    }

    pub fn resume(&mut self, approval_payload: Option<&Value>) -> Result<Value, Box<dyn std::error::Error>> {
        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
        let paused_at_ts: f64 = self
            .journal
            .get_meta("paused_at_ts")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(now_ts);
        let pause_duration = (now_ts - paused_at_ts).max(0.0);
        self.paused_duration_s += pause_duration;
        self.journal.set_meta("paused_duration_s", &self.paused_duration_s.to_string())?;

        if let Some(approval) = approval_payload {
            if let Value::Object(map) = approval {
                if let Value::Object(ref mut cur) = self.context {
                    cur.extend(map.clone());
                }
            }
            self.journal.save_checkpoint(self.step_idx, &self.context, "resume_approval")?;
        }

        self.journal.record_event(
            EventType::ResumeAck.as_str(),
            &json!({
                "step_idx": self.step_idx,
                "approval_payload": approval_payload.unwrap_or(&json!({})),
                "paused_duration_s": pause_duration
            }),
            self.step_idx,
        )?;

        self.transition_to(LoopState::Running, "resumed_with_approval")?;
        Ok(self.context.clone())
    }

    pub fn complete(&mut self, final_output: Option<&Value>) -> Result<LoopReceipt, Box<dyn std::error::Error>> {
        self.transition_to(LoopState::Completed, "loop_completed")?;
        if let Some(out) = final_output {
            self.journal.set_meta("final_output", &out.to_string())?;
        }

        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
        let total_wall = (now_ts - self.created_at_ts).max(0.01);
        self.active_duration_s = (total_wall - self.paused_duration_s).max(0.01);
        self.journal.set_meta("active_duration_s", &self.active_duration_s.to_string())?;

        self.journal.record_event(
            EventType::LoopComplete.as_str(),
            &json!({
                "final_output": final_output.unwrap_or(&json!("success")),
                "total_steps": self.step_idx,
                "total_cost_usd": self.total_cost_usd
            }),
            self.step_idx,
        )?;

        let (is_valid, total_events, _) = self.journal.verify_chain()?;

        let receipt = LoopReceipt {
            loop_id: self.loop_id.clone(),
            total_steps: self.step_idx,
            duplicate_tool_calls_prevented: self.duplicate_tool_calls_prevented,
            crash_recoveries: self.crash_recoveries,
            active_duration_s: (self.active_duration_s * 100.0).round() / 100.0,
            paused_duration_s: (self.paused_duration_s * 100.0).round() / 100.0,
            total_cost_usd: (self.total_cost_usd * 10000.0).round() / 10000.0,
            chain_verified: is_valid,
            total_events,
            state: self.state.as_str().to_string(),
            final_output: final_output.cloned(),
        };

        // Run auto-prune
        let cfg = StorageConfig {
            storage_dir: self.storage_dir.clone(),
            ..StorageConfig::default()
        };
        let _ = prune_journals(&cfg);

        Ok(receipt)
    }

    pub fn fail(&mut self, error: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.transition_to(LoopState::Failed, error)?;
        self.journal.set_meta("error_message", error)?;
        self.journal.record_event(
            EventType::LoopFail.as_str(),
            &json!({ "error": error, "step_idx": self.step_idx }),
            self.step_idx,
        )?;
        Ok(())
    }
}

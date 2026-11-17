use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LoopState {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
}

impl LoopState {
    pub fn as_str(&self) -> &'static str {
        match self {
            LoopState::Pending => "PENDING",
            LoopState::Running => "RUNNING",
            LoopState::Paused => "PAUSED",
            LoopState::Completed => "COMPLETED",
            LoopState::Failed => "FAILED",
        }
    }
}

impl std::str::FromStr for LoopState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(LoopState::Pending),
            "RUNNING" => Ok(LoopState::Running),
            "PAUSED" => Ok(LoopState::Paused),
            "COMPLETED" => Ok(LoopState::Completed),
            "FAILED" => Ok(LoopState::Failed),
            other => Err(format!("Unknown LoopState: {}", other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    LoopInit,
    StateTransition,
    StepStart,
    StepComplete,
    Thought,
    ToolCallStart,
    ToolCallResult,
    CostRecorded,
    PauseRequested,
    ResumeAck,
    LoopComplete,
    LoopFail,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::LoopInit => "LOOP_INIT",
            EventType::StateTransition => "STATE_TRANSITION",
            EventType::StepStart => "STEP_START",
            EventType::StepComplete => "STEP_COMPLETE",
            EventType::Thought => "THOUGHT",
            EventType::ToolCallStart => "TOOL_CALL_START",
            EventType::ToolCallResult => "TOOL_CALL_RESULT",
            EventType::CostRecorded => "COST_RECORDED",
            EventType::PauseRequested => "PAUSE_REQUESTED",
            EventType::ResumeAck => "RESUME_ACK",
            EventType::LoopComplete => "LOOP_COMPLETE",
            EventType::LoopFail => "LOOP_FAIL",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopEvent {
    pub event_id: i64,
    pub event_uuid: String,
    pub timestamp_ns: i64,
    pub timestamp_iso: String,
    pub event_type: String,
    pub step_idx: u32,
    pub payload: Value,
    pub prev_event_hash: String,
    pub event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopReceipt {
    pub loop_id: String,
    pub total_steps: u32,
    pub duplicate_tool_calls_prevented: u32,
    pub crash_recoveries: u32,
    pub active_duration_s: f64,
    pub paused_duration_s: f64,
    pub total_cost_usd: f64,
    pub chain_verified: bool,
    pub total_events: usize,
    pub state: String,
    pub final_output: Option<Value>,
}

impl LoopReceipt {
    pub fn render(&self) -> String {
        let verif_str = if self.chain_verified {
            format!(
                "VERIFIED ({}/{} events cryptographically chained)",
                self.total_events, self.total_events
            )
        } else {
            "FAILED (Integrity Compromised)".to_string()
        };

        let paused_str = if self.paused_duration_s > 0.0 {
            if self.paused_duration_s >= 3600.0 {
                let h = (self.paused_duration_s / 3600.0) as u32;
                let m = ((self.paused_duration_s % 3600.0) / 60.0) as u32;
                format!(" (Paused for {}h {}m awaiting approval)", h, m)
            } else if self.paused_duration_s >= 60.0 {
                let m = (self.paused_duration_s / 60.0) as u32;
                let s = (self.paused_duration_s % 60.0) as u32;
                format!(" (Paused for {}m {}s awaiting approval)", m, s)
            } else {
                format!(" (Paused for {:.2}s awaiting approval)", self.paused_duration_s)
            }
        } else {
            String::new()
        };

        format!(
            "⚡ ZeroLoop Execution Receipt [{}]:\n\
             • Total Steps: {} completed ({} duplicates, {} crash recoveries)\n\
             • Duration: {:.2}s active{}\n\
             • Total Cost: ${:.4} (routed via HyperGate)\n\
             • Journal Integrity: {}",
            self.loop_id,
            self.total_steps,
            self.duplicate_tool_calls_prevented,
            self.crash_recoveries,
            self.active_duration_s,
            paused_str,
            self.total_cost_usd,
            verif_str
        )
    }
}

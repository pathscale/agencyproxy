//! Versioned, transport-independent AgencyProxy wire types.
//!
//! Local IPC and the later remote connector deliberately share these frames.
//! Transport access control is outside this crate; v0 local IPC has no
//! application-layer authentication.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion { major: 0, minor: 1 };
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    #[must_use]
    pub const fn negotiate(self, peer: Self) -> Option<Self> {
        if self.major != peer.major {
            return None;
        }
        Some(Self {
            major: self.major,
            minor: if self.minor < peer.minor {
                self.minor
            } else {
                peer.minor
            },
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RunId(pub String);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientFrame {
    pub request_id: u64,
    #[serde(flatten)]
    pub message: ClientMessage,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Hello {
        client_name: String,
        version: ProtocolVersion,
    },
    ListRuns,
    ProbeProviders,
    ReadAccountUsage,
    StartRun {
        run_id: RunId,
        request: Box<RunRequest>,
        idempotency_key: String,
    },
    AttachRun {
        run_id: RunId,
        after_sequence: u64,
    },
    DetachRun {
        run_id: RunId,
    },
    InjectMessage {
        run_id: RunId,
        body: String,
        idempotency_key: String,
    },
    CancelRun {
        run_id: RunId,
        idempotency_key: String,
    },
    DecideApproval {
        run_id: RunId,
        approval_id: String,
        decision: ApprovalDecision,
        idempotency_key: String,
    },
    AckEvents {
        run_id: RunId,
        through_sequence: u64,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRequest {
    pub provider: String,
    pub model: String,
    pub prompt: String,
    #[serde(default)]
    pub is_command: bool,
    pub system: Option<String>,
    pub permission: String,
    pub effort: Option<String>,
    pub extra_thinking: Option<bool>,
    pub approvals: bool,
    pub interactive: bool,
    pub workspace_roots: Vec<String>,
    pub resume_session_id: Option<String>,
    /// Test/development override; production clients normally leave this unset.
    pub binary: Option<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub unchecked_args: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    AllowOnce,
    AllowSimilar,
    Deny,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerFrame {
    Response {
        request_id: u64,
        #[serde(flatten)]
        response: ServerResponse,
    },
    Event {
        run_id: RunId,
        sequence: u64,
        event: RunEvent,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerResponse {
    Hello {
        server_name: String,
        version: ProtocolVersion,
        capabilities: Vec<Capability>,
    },
    Runs {
        runs: Vec<RunSnapshot>,
    },
    Providers {
        providers: Vec<ProviderStatus>,
    },
    AccountUsage {
        providers: Vec<ProviderAccountUsage>,
    },
    Run {
        run: RunSnapshot,
    },
    Accepted,
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    EventReplay,
    LiveInjection,
    Approvals,
    Cancellation,
    ProviderDetection,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderStatus {
    pub provider: String,
    pub installed: bool,
    pub version: Option<String>,
    pub outdated: bool,
    pub auth_state: String,
    pub detail: String,
    pub auth_method: Option<String>,
    pub account: Option<String>,
    pub plan: Option<String>,
    pub login_hint: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAccountUsage {
    pub provider: String,
    pub supported: bool,
    pub usage: Option<Value>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    IncompatibleVersion,
    ProtocolViolation,
    NotFound,
    NotImplemented,
    Conflict,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Starting,
    Running,
    WaitingApproval,
    Finishing,
    Completed,
    Failed,
    Canceled,
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSnapshot {
    pub run_id: RunId,
    pub state: RunState,
    pub provider: String,
    pub model: String,
    pub provider_session_id: Option<String>,
    pub latest_sequence: u64,
    pub acknowledged_sequence: u64,
    pub workspace_roots: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum RunEvent {
    /// The exact terminal outcome after the provider process has been reaped.
    Finished(Value),
    /// The provider process could not produce a terminal outcome.
    Failed(String),
    StateChanged(RunState),
    SessionOpened {
        provider_session_id: String,
        model: Option<String>,
    },
    Reasoning(String),
    Text(String),
    MessageBoundary,
    ToolCall {
        id: Option<String>,
        name: String,
        input: Value,
    },
    ToolResult {
        id: Option<String>,
        ok: Option<bool>,
        output: String,
    },
    ApprovalRequested {
        approval_id: String,
        title: String,
        detail: Value,
    },
    Usage(Value),
    RateLimit(Value),
    Compaction(Value),
    Commands(Value),
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_only_within_the_same_major_version() {
        assert_eq!(
            PROTOCOL_VERSION.negotiate(ProtocolVersion { major: 0, minor: 0 }),
            Some(ProtocolVersion { major: 0, minor: 0 })
        );
        assert_eq!(
            PROTOCOL_VERSION.negotiate(ProtocolVersion { major: 1, minor: 0 }),
            None
        );
    }

    #[test]
    fn frames_round_trip_without_transport_specific_state() {
        let frame = ClientFrame {
            request_id: 7,
            message: ClientMessage::AttachRun {
                run_id: RunId("run-7".into()),
                after_sequence: 41,
            },
        };
        let encoded = serde_json::to_vec(&frame).expect("frame should encode");
        let decoded: ClientFrame = serde_json::from_slice(&encoded).expect("frame should decode");
        assert_eq!(decoded, frame);
    }
}

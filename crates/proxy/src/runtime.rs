use agency_proxy_protocol::{ApprovalDecision, RunEvent, RunId, RunRequest, RunSnapshot, RunState};
use agent_abstraction::{
    Agent, AuthState, AuthStatus, Decision, Event, Permission, Probe, Request, VersionStatus,
    interrupt,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{RwLock, broadcast, oneshot};

const MAX_REPLAY_EVENTS: usize = 2_048;

#[derive(Clone, Debug)]
pub struct SequencedEvent {
    pub run_id: RunId,
    pub sequence: u64,
    pub event: RunEvent,
}

#[derive(Debug)]
struct LiveRun {
    snapshot: RunSnapshot,
    journal: VecDeque<SequencedEvent>,
    replay_floor: u64,
    events: broadcast::Sender<SequencedEvent>,
    control: agent_abstraction::RunControl,
    cancel: Option<oneshot::Sender<()>>,
}

#[derive(Clone, Debug)]
pub struct RuntimeRegistry {
    runs: Arc<RwLock<BTreeMap<RunId, LiveRun>>>,
    executor: tokio::runtime::Handle,
}

impl Default for RuntimeRegistry {
    fn default() -> Self {
        Self {
            runs: Arc::default(),
            // The registry owns provider tasks across connection transports.
            // Capturing the daemon runtime here prevents a WebSocket shard or
            // disconnected client runtime from becoming their accidental owner.
            executor: tokio::runtime::Handle::current(),
        }
    }
}

pub struct Attachment {
    pub snapshot: RunSnapshot,
    pub replay: Vec<SequencedEvent>,
    pub events: broadcast::Receiver<SequencedEvent>,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("run already exists")]
    Conflict,
    #[error("run does not exist")]
    NotFound,
    #[error("requested replay sequence {requested} is older than retained sequence {earliest}")]
    ReplayExpired { requested: u64, earliest: u64 },
    #[error("unsupported provider: {0}")]
    Provider(String),
    #[error("unsupported permission: {0}")]
    Permission(String),
    #[error("agent run could not start: {0}")]
    Start(String),
    #[error("agent control failed: {0}")]
    Control(String),
}

impl RuntimeRegistry {
    pub async fn account_usage(&self) -> Vec<agency_proxy_protocol::ProviderAccountUsage> {
        futures::future::join_all([Agent::Claude, Agent::Codex, Agent::Copilot].map(
            |agent| async move {
                let provider = agent_name(agent).to_string();
                if !agent.reports_account_usage() {
                    return agency_proxy_protocol::ProviderAccountUsage {
                        provider,
                        supported: false,
                        usage: None,
                        error: None,
                    };
                }
                match agent.account_usage().await {
                    Ok(usage) => agency_proxy_protocol::ProviderAccountUsage {
                        provider,
                        supported: true,
                        usage: serde_json::to_value(usage).ok(),
                        error: None,
                    },
                    Err(error) => agency_proxy_protocol::ProviderAccountUsage {
                        provider,
                        supported: true,
                        usage: None,
                        error: Some(error.to_string()),
                    },
                }
            },
        ))
        .await
    }

    pub async fn probe_providers(&self) -> Vec<agency_proxy_protocol::ProviderStatus> {
        futures::future::join_all([Agent::Claude, Agent::Codex, Agent::Copilot].map(
            |agent| async move {
                let probe = Probe::run(agent).await;
                let auth = AuthStatus::check(agent).await;
                let installed =
                    !matches!(probe, Err(agent_abstraction::Error::NotInstalled { .. }));
                let probe = probe.ok();
                let version = probe
                    .as_ref()
                    .and_then(|value| value.version.as_ref().map(ToString::to_string));
                let outdated = probe
                    .as_ref()
                    .is_some_and(|value| matches!(value.status, VersionStatus::Older));
                let (auth_state, detail, auth_method, account, plan, login_hint) = match auth {
                    Ok(status) => (
                        match status.state {
                            AuthState::LoggedIn => "logged_in",
                            AuthState::LoggedOut => "logged_out",
                            AuthState::Unknown => "unknown",
                            _ => "unknown",
                        }
                        .to_string(),
                        status.detail,
                        status.method,
                        status.account,
                        status.plan,
                        status.login_hint.to_string(),
                    ),
                    Err(error) => (
                        "unknown".into(),
                        error.to_string(),
                        None,
                        None,
                        None,
                        String::new(),
                    ),
                };
                agency_proxy_protocol::ProviderStatus {
                    provider: agent_name(agent).into(),
                    installed,
                    version,
                    outdated,
                    auth_state,
                    detail,
                    auth_method,
                    account,
                    plan,
                    login_hint,
                }
            },
        ))
        .await
    }

    pub async fn start(&self, run_id: RunId, spec: RunRequest) -> Result<(), RuntimeError> {
        let registry = self.clone();
        self.executor
            .spawn(async move { registry.start_owned(run_id, spec).await })
            .await
            .map_err(|error| RuntimeError::Start(format!("proxy runtime stopped: {error}")))?
    }

    async fn start_owned(&self, run_id: RunId, spec: RunRequest) -> Result<(), RuntimeError> {
        if self.runs.read().await.contains_key(&run_id) {
            return Err(RuntimeError::Conflict);
        }
        let request = build_request(spec.clone())?;
        let mut run = agent_abstraction::stream(&request)
            .map_err(|error| RuntimeError::Start(error.to_string()))?;
        let control = run.control();
        let (events, _) = broadcast::channel(256);
        let (cancel, mut cancelled) = oneshot::channel();
        let snapshot = RunSnapshot {
            run_id: run_id.clone(),
            state: RunState::Starting,
            provider: spec.provider,
            model: spec.model,
            provider_session_id: spec.resume_session_id,
            latest_sequence: 0,
            acknowledged_sequence: 0,
            workspace_roots: spec.workspace_roots,
            metadata: spec.metadata,
        };
        {
            let mut runs = self.runs.write().await;
            if runs.contains_key(&run_id) {
                return Err(RuntimeError::Conflict);
            }
            runs.insert(
                run_id.clone(),
                LiveRun {
                    snapshot,
                    journal: VecDeque::new(),
                    replay_floor: 0,
                    events,
                    control,
                    cancel: Some(cancel),
                },
            );
        }

        let registry = self.clone();
        self.executor.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancelled => {
                        // Cooperative cancellation gives interactive providers
                        // their protocol-level interrupt before the abstraction's
                        // bounded process-group fallback. Dropping this handle
                        // skipped Codex `turn/interrupt`, leaving the server-owned
                        // turn alive after AgencyProxy reported it canceled.
                        let _ = run.cancel().await;
                        registry
                            .publish_error(
                                &run_id,
                                "the run was canceled".into(),
                                RunState::Canceled,
                            )
                            .await;
                        return;
                    }
                    event = run.recv() => {
                        let Some(event) = event else { break };
                        registry.publish_provider(&run_id, event).await;
                    }
                }
            }
            match run.finish().await {
                Ok(outcome) => {
                    let state = if outcome.is_ok() {
                        RunState::Completed
                    } else {
                        RunState::Failed
                    };
                    registry.publish_finished(&run_id, outcome, state).await;
                }
                Err(error) => {
                    registry
                        .publish_error(&run_id, error.to_string(), RunState::Failed)
                        .await
                }
            }
        });
        Ok(())
    }

    pub async fn list(&self) -> Vec<RunSnapshot> {
        self.runs
            .read()
            .await
            .values()
            .map(|run| run.snapshot.clone())
            .collect()
    }

    pub async fn active_count(&self) -> usize {
        self.runs
            .read()
            .await
            .values()
            .filter(|run| {
                matches!(
                    run.snapshot.state,
                    RunState::Starting
                        | RunState::Running
                        | RunState::WaitingApproval
                        | RunState::Finishing
                )
            })
            .count()
    }

    pub async fn attach(&self, run_id: &RunId, after: u64) -> Result<Attachment, RuntimeError> {
        let runs = self.runs.read().await;
        let run = runs.get(run_id).ok_or(RuntimeError::NotFound)?;
        if after < run.replay_floor {
            return Err(RuntimeError::ReplayExpired {
                requested: after,
                earliest: run.replay_floor,
            });
        }
        Ok(Attachment {
            snapshot: run.snapshot.clone(),
            replay: run
                .journal
                .iter()
                .filter(|event| event.sequence > after)
                .cloned()
                .collect(),
            events: run.events.subscribe(),
        })
    }

    pub async fn acknowledge(&self, run_id: &RunId, through: u64) -> Result<(), RuntimeError> {
        let mut runs = self.runs.write().await;
        let run = runs.get_mut(run_id).ok_or(RuntimeError::NotFound)?;
        let through = through.min(run.snapshot.latest_sequence);
        run.snapshot.acknowledged_sequence = run.snapshot.acknowledged_sequence.max(through);
        while run
            .journal
            .front()
            .is_some_and(|event| event.sequence <= run.snapshot.acknowledged_sequence)
        {
            run.journal.pop_front();
        }
        Ok(())
    }

    pub async fn inject(&self, run_id: &RunId, body: &str) -> Result<(), RuntimeError> {
        let control = self.control(run_id).await?;
        control
            .send(body)
            .await
            .map_err(|error| RuntimeError::Control(error.to_string()))
    }

    pub async fn decide(
        &self,
        run_id: &RunId,
        approval_id: &str,
        decision: ApprovalDecision,
    ) -> Result<(), RuntimeError> {
        let control = self.control(run_id).await?;
        let decision = match decision {
            ApprovalDecision::AllowOnce | ApprovalDecision::AllowSimilar => Decision::Allow,
            ApprovalDecision::Deny => Decision::deny(),
        };
        control
            .respond(approval_id, &decision)
            .await
            .map_err(|error| RuntimeError::Control(error.to_string()))
    }

    pub async fn cancel(&self, run_id: &RunId) -> Result<(), RuntimeError> {
        let mut runs = self.runs.write().await;
        let run = runs.get_mut(run_id).ok_or(RuntimeError::NotFound)?;
        let cancel = run.cancel.take().ok_or(RuntimeError::Conflict)?;
        let _ = cancel.send(());
        Ok(())
    }

    /// Interrupt an orphaned provider turn without submitting a new prompt.
    pub async fn interrupt_session(
        &self,
        provider: &str,
        session_id: &str,
        binary: Option<String>,
    ) -> Result<bool, RuntimeError> {
        let agent = agent_for_provider(provider)?;
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return Err(RuntimeError::Provider("the session id is empty".into()));
        }
        let mut request = Request::new(agent, "")
            .resume(session_id)
            .permission(Permission::ReadOnly);
        if let Some(binary) = binary.filter(|value| !value.is_empty()) {
            request = request.bin(binary);
        }
        interrupt(&request)
            .await
            .map_err(|error| RuntimeError::Control(error.to_string()))
    }

    /// Cooperatively stop every provider run that has not reached a terminal
    /// state. Returns the number of runs signalled.
    pub async fn cancel_all(&self) -> usize {
        let mut runs = self.runs.write().await;
        runs.values_mut()
            .filter(|run| {
                matches!(
                    run.snapshot.state,
                    RunState::Starting
                        | RunState::Running
                        | RunState::WaitingApproval
                        | RunState::Finishing
                )
            })
            .filter_map(|run| run.cancel.take())
            .map(|cancel| {
                let _ = cancel.send(());
            })
            .count()
    }

    async fn control(&self, run_id: &RunId) -> Result<agent_abstraction::RunControl, RuntimeError> {
        self.runs
            .read()
            .await
            .get(run_id)
            .map(|run| run.control.clone())
            .ok_or(RuntimeError::NotFound)
    }

    async fn publish_provider(&self, run_id: &RunId, event: Event) {
        let (event, state, session) = match event {
            Event::Started { session, model } => (
                RunEvent::SessionOpened {
                    provider_session_id: session.clone(),
                    model,
                },
                RunState::Running,
                Some(session),
            ),
            Event::Thinking(text) => (RunEvent::Reasoning(text), RunState::Running, None),
            Event::Text(text) => (RunEvent::Text(text), RunState::Running, None),
            Event::MessageBoundary => (RunEvent::MessageBoundary, RunState::Running, None),
            Event::ToolCall { id, name, input } => (
                RunEvent::ToolCall { id, name, input },
                RunState::Running,
                None,
            ),
            Event::ToolResult { id, ok, output } => (
                RunEvent::ToolResult { id, ok, output },
                RunState::Running,
                None,
            ),
            Event::ApprovalRequest(approval) => (
                RunEvent::ApprovalRequested {
                    approval_id: approval.id,
                    title: approval.tool,
                    detail: approval.input,
                },
                RunState::WaitingApproval,
                None,
            ),
            Event::Usage(value) => match serde_json::to_value(value) {
                Ok(value) => (RunEvent::Usage(value), RunState::Running, None),
                Err(error) => {
                    self.publish_error(run_id, error.to_string(), RunState::Failed)
                        .await;
                    return;
                }
            },
            Event::RateLimit(value) => match serde_json::to_value(value) {
                Ok(value) => (RunEvent::RateLimit(value), RunState::Running, None),
                Err(error) => {
                    self.publish_error(run_id, error.to_string(), RunState::Failed)
                        .await;
                    return;
                }
            },
            Event::Compaction(value) => match serde_json::to_value(value) {
                Ok(value) => (RunEvent::Compaction(value), RunState::Running, None),
                Err(error) => {
                    self.publish_error(run_id, error.to_string(), RunState::Failed)
                        .await;
                    return;
                }
            },
            Event::Commands(value) => match serde_json::to_value(value) {
                Ok(value) => (RunEvent::Commands(value), RunState::Running, None),
                Err(error) => {
                    self.publish_error(run_id, error.to_string(), RunState::Failed)
                        .await;
                    return;
                }
            },
            _ => return,
        };
        self.publish(run_id, event, Some(state), session).await;
    }

    async fn publish_finished(
        &self,
        run_id: &RunId,
        outcome: agent_abstraction::Outcome,
        state: RunState,
    ) {
        match serde_json::to_value(outcome) {
            Ok(outcome) => {
                self.publish(run_id, RunEvent::Finished(outcome), Some(state), None)
                    .await
            }
            Err(error) => {
                self.publish_error(run_id, error.to_string(), RunState::Failed)
                    .await
            }
        }
    }

    async fn publish_error(&self, run_id: &RunId, error: String, state: RunState) {
        self.publish(run_id, RunEvent::Failed(error), Some(state), None)
            .await;
    }

    async fn publish(
        &self,
        run_id: &RunId,
        event: RunEvent,
        state: Option<RunState>,
        session: Option<String>,
    ) {
        let mut runs = self.runs.write().await;
        let Some(run) = runs.get_mut(run_id) else {
            return;
        };
        run.snapshot.latest_sequence += 1;
        if let Some(state) = state {
            run.snapshot.state = state;
        }
        if let Some(session) = session {
            run.snapshot.provider_session_id = Some(session);
        }
        let event = SequencedEvent {
            run_id: run_id.clone(),
            sequence: run.snapshot.latest_sequence,
            event,
        };
        push_journal(&mut run.journal, &mut run.replay_floor, event.clone());
        let _ = run.events.send(event);
    }
}

fn push_journal(
    journal: &mut VecDeque<SequencedEvent>,
    replay_floor: &mut u64,
    event: SequencedEvent,
) {
    journal.push_back(event);
    while journal.len() > MAX_REPLAY_EVENTS {
        if let Some(expired) = journal.pop_front() {
            *replay_floor = (*replay_floor).max(expired.sequence);
        }
    }
}

fn agent_name(agent: Agent) -> &'static str {
    match agent {
        Agent::Claude => "claude",
        Agent::Codex => "codex",
        Agent::Copilot => "copilot",
    }
}

fn agent_for_provider(provider: &str) -> Result<Agent, RuntimeError> {
    match provider {
        "claude" => Ok(Agent::Claude),
        "codex" => Ok(Agent::Codex),
        "copilot" => Ok(Agent::Copilot),
        other => Err(RuntimeError::Provider(other.into())),
    }
}

fn build_request(spec: RunRequest) -> Result<Request, RuntimeError> {
    let agent = agent_for_provider(&spec.provider)?;
    let permission = match spec.permission.as_str() {
        "read_only" | "read-only" => Permission::ReadOnly,
        "plan" => Permission::Plan,
        "edit" => Permission::Edit,
        "auto" => Permission::Auto,
        "bypass" => Permission::Bypass,
        other => return Err(RuntimeError::Permission(other.into())),
    };
    let mut request = if spec.is_command {
        match spec.prompt.as_str() {
            "/compact" => Request::command(
                agent,
                &agent_abstraction::Command::Compact { instructions: None },
            ),
            other => return Err(RuntimeError::Start(format!("unsupported command: {other}"))),
        }
    } else {
        Request::new(agent, spec.prompt)
    }
    .permission(permission);
    if let Some(root) = spec.workspace_roots.first() {
        request = request.cwd(root);
        for extra in spec.workspace_roots.iter().skip(1) {
            request = request.add_dir(extra);
        }
    }
    if !spec.model.is_empty() {
        request = request.model(spec.model);
    }
    if let Some(system) = spec.system.filter(|value| !value.is_empty()) {
        request = request.system(system);
    }
    if let Some(effort) = spec.effort.filter(|value| !value.is_empty()) {
        request = request.effort(effort);
    }
    if let Some(thinking) = spec.extra_thinking {
        request = request.thinking(thinking);
    }
    if let Some(session) = spec.resume_session_id.filter(|value| !value.is_empty()) {
        request = request.resume(session);
    }
    if let Some(binary) = spec.binary.filter(|value| !value.is_empty()) {
        request = request.bin(binary);
    }
    if spec.approvals {
        request = request.approvals();
    }
    if spec.interactive {
        request = request.interactive();
    }
    for (name, value) in spec.environment {
        request = request.env(name, value);
    }
    if !spec.unchecked_args.is_empty() {
        request = request.unchecked_args(spec.unchecked_args);
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_journal_is_bounded_and_tracks_expired_sequence() {
        let mut journal = VecDeque::new();
        let mut replay_floor = 0;
        let run_id = RunId("bounded-run".into());

        for sequence in 1..=(MAX_REPLAY_EVENTS as u64 + 3) {
            push_journal(
                &mut journal,
                &mut replay_floor,
                SequencedEvent {
                    run_id: run_id.clone(),
                    sequence,
                    event: RunEvent::Text(sequence.to_string()),
                },
            );
        }

        assert_eq!(journal.len(), MAX_REPLAY_EVENTS);
        assert_eq!(replay_floor, 3);
        assert_eq!(journal.front().map(|event| event.sequence), Some(4));
    }
}

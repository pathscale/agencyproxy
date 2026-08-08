use agency_proxy_protocol::{ApprovalDecision, RunEvent, RunId, RunRequest, RunSnapshot, RunState};
use agent_abstraction::{Agent, Decision, Event, Permission, Request};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{RwLock, broadcast, oneshot};

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
    events: broadcast::Sender<SequencedEvent>,
    control: agent_abstraction::RunControl,
    cancel: Option<oneshot::Sender<()>>,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeRegistry(Arc<RwLock<BTreeMap<RunId, LiveRun>>>);

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
    pub async fn start(&self, run_id: RunId, spec: RunRequest) -> Result<(), RuntimeError> {
        if self.0.read().await.contains_key(&run_id) {
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
        };
        {
            let mut runs = self.0.write().await;
            if runs.contains_key(&run_id) {
                return Err(RuntimeError::Conflict);
            }
            runs.insert(
                run_id.clone(),
                LiveRun {
                    snapshot,
                    journal: VecDeque::new(),
                    events,
                    control,
                    cancel: Some(cancel),
                },
            );
        }

        let registry = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancelled => {
                        match run.cancel().await {
                            Ok(outcome) => {
                                registry.publish_finished(&run_id, outcome, RunState::Canceled).await;
                            }
                            Err(error) => {
                                registry.publish_error(&run_id, error.to_string(), RunState::Canceled).await;
                            }
                        }
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
        self.0
            .read()
            .await
            .values()
            .map(|run| run.snapshot.clone())
            .collect()
    }

    pub async fn attach(&self, run_id: &RunId, after: u64) -> Result<Attachment, RuntimeError> {
        let runs = self.0.read().await;
        let run = runs.get(run_id).ok_or(RuntimeError::NotFound)?;
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
        let mut runs = self.0.write().await;
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
        let mut runs = self.0.write().await;
        let run = runs.get_mut(run_id).ok_or(RuntimeError::NotFound)?;
        let cancel = run.cancel.take().ok_or(RuntimeError::Conflict)?;
        let _ = cancel.send(());
        Ok(())
    }

    async fn control(&self, run_id: &RunId) -> Result<agent_abstraction::RunControl, RuntimeError> {
        self.0
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
        self.publish(run_id, RunEvent::Error(error), Some(state), None)
            .await;
    }

    async fn publish(
        &self,
        run_id: &RunId,
        event: RunEvent,
        state: Option<RunState>,
        session: Option<String>,
    ) {
        let mut runs = self.0.write().await;
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
        run.journal.push_back(event.clone());
        let _ = run.events.send(event);
    }
}

fn build_request(spec: RunRequest) -> Result<Request, RuntimeError> {
    let agent = match spec.provider.as_str() {
        "claude" => Agent::Claude,
        "codex" => Agent::Codex,
        "copilot" => Agent::Copilot,
        other => return Err(RuntimeError::Provider(other.into())),
    };
    let permission = match spec.permission.as_str() {
        "read_only" | "read-only" => Permission::ReadOnly,
        "plan" => Permission::Plan,
        "edit" => Permission::Edit,
        "auto" => Permission::Auto,
        "bypass" => Permission::Bypass,
        other => return Err(RuntimeError::Permission(other.into())),
    };
    let mut request = Request::new(agent, spec.prompt).permission(permission);
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

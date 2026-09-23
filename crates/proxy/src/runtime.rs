use crate::{broadcast, flag::Flag};
use agency_proxy_protocol::{ApprovalDecision, RunEvent, RunId, RunRequest, RunSnapshot, RunState};
use agent_abstraction::{
    Agent, AuthState, AuthStatus, Decision, Event, Permission, Probe, Request, VersionStatus,
    interrupt,
};
use futures::{
    channel::oneshot,
    future::{Either, select},
};
use nagoya::reactor::Handle;
use nagoya::sync::{Notify, RwLock};
use std::{
    collections::{BTreeMap, VecDeque},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;

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
    completed: Arc<Flag>,
}

/// Owns provider runs across connection transports.
///
/// Under tokio the registry captured the daemon runtime's handle so that a
/// WebSocket shard or a disconnected client's runtime could not become the
/// accidental owner of provider tasks. Tasks now go to nagoya's shared pool,
/// which belongs to no connection. What the registry holds instead is the
/// reactor its provider processes are registered on, given to it by whoever
/// owns that reactor. There is no `Default`: a registry cannot conjure a
/// reactor without owning one.
#[derive(Clone, Debug)]
pub struct RuntimeRegistry {
    runs: Arc<RwLock<BTreeMap<RunId, LiveRun>>>,
    activity: RunActivity,
    reactor: Handle,
}

/// How many accepted runs have not reached a terminal state.
///
/// This was a `tokio::sync::watch::Sender<usize>`. Only the count and a wake
/// on change were used, so it is an atomic and a `Notify`.
#[derive(Clone, Debug, Default)]
struct RunActivity {
    inner: Arc<ActivityState>,
}

#[derive(Debug, Default)]
struct ActivityState {
    active: AtomicUsize,
    changed: Notify,
}

impl RunActivity {
    fn started(&self) {
        self.inner.active.fetch_add(1, Ordering::SeqCst);
        self.inner.changed.notify_waiters();
    }

    fn finished(&self) {
        let (Ok(previous) | Err(previous)) =
            self.inner
                .active
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                    Some(active.saturating_sub(1))
                });
        debug_assert!(previous > 0, "a run finished without being active");
        self.inner.changed.notify_waiters();
    }

    fn count(&self) -> usize {
        self.inner.active.load(Ordering::SeqCst)
    }

    async fn wait_until_idle(&self) {
        loop {
            // Created before the count is read: `Notify` snapshots its
            // broadcast generation here, so a run finishing between the read
            // and the await still wakes this waiter.
            let changed = self.inner.changed.notified();
            if self.count() == 0 {
                return;
            }
            changed.await;
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
    /// An empty registry whose provider processes are registered on
    /// `reactor`. The caller owns the reactor and must keep it running for as
    /// long as any run it started may still be live.
    #[must_use]
    pub fn new(reactor: Handle) -> Self {
        Self {
            runs: Arc::default(),
            activity: RunActivity::default(),
            reactor,
        }
    }

    /// Run `future` on the pool that owns this registry's provider tasks.
    ///
    /// For work started from a transport whose own executor is not the pool,
    /// such as an endpoint-libs WebSocket handler, which is polled in place on
    /// that server's reactor thread and has no spawner of its own.
    pub(crate) fn spawn<F>(&self, future: F) -> nagoya::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        nagoya::spawn(future)
    }

    pub async fn account_usage(&self) -> Vec<agency_proxy_protocol::ProviderAccountUsage> {
        let reactor = &self.reactor;
        futures::future::join_all(
            [Agent::Claude, Agent::Codex, Agent::Copilot, Agent::Grok].map(|agent| async move {
                let provider = agent_name(agent).to_string();
                if !agent.reports_account_usage() {
                    return agency_proxy_protocol::ProviderAccountUsage {
                        provider,
                        supported: false,
                        usage: None,
                        error: None,
                    };
                }
                match agent.account_usage(reactor).await {
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
            }),
        )
        .await
    }

    pub async fn probe_providers(&self) -> Vec<agency_proxy_protocol::ProviderStatus> {
        let reactor = &self.reactor;
        futures::future::join_all(
            [Agent::Claude, Agent::Codex, Agent::Copilot, Agent::Grok].map(|agent| async move {
                let probe = Probe::run(agent, reactor).await;
                let auth = AuthStatus::check(agent, reactor).await;
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
            }),
        )
        .await
    }

    pub async fn start(&self, run_id: RunId, spec: RunRequest) -> Result<(), RuntimeError> {
        let registry = self.clone();
        // `None` is nagoya's JoinError: the pool went away before the task
        // could finish. A panic inside is rethrown here rather than returned,
        // where tokio returned it as an error, but provider starts do not
        // panic by design and a panic is a bug either way.
        self.spawn(async move { registry.start_owned(run_id, spec).await })
            .await
            .ok_or_else(|| RuntimeError::Start("proxy runtime stopped".into()))?
    }

    async fn start_owned(&self, run_id: RunId, spec: RunRequest) -> Result<(), RuntimeError> {
        if self.runs.read().await.contains_key(&run_id) {
            return Err(RuntimeError::Conflict);
        }
        let request = build_request(spec.clone())?;
        let mut run = agent_abstraction::stream(&request, &self.reactor)
            .map_err(|error| RuntimeError::Start(error.to_string()))?;
        let control = run.control();
        let (events, _) = broadcast::channel(256);
        let (cancel, mut cancelled) = oneshot::channel();
        let completed = Arc::new(Flag::default());
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
                    completed,
                },
            );
            self.activity.started();
        }

        let registry = self.clone();
        // Detached, as tokio's spawn was: the run outlives the request that
        // started it and ends on its own terminal event or on cancellation.
        drop(self.spawn(async move {
            loop {
                // `select` polls its first argument first, which is the
                // `biased;` ordering the tokio version asked for: a pending
                // cancellation wins over an event that is ready at the same
                // time. The cancellation receiver is borrowed, so losing to an
                // event leaves it armed for the next turn; the `recv` future is
                // dropped at the end of this statement either way, which frees
                // `run` for `cancel` below. A dropped sender resolves the
                // receiver too, exactly as `&mut cancelled` did under tokio.
                let event = match select(&mut cancelled, pin!(run.recv())).await {
                    Either::Left(_) => None,
                    Either::Right((event, _)) => Some(event),
                };
                let Some(event) = event else {
                    // Cooperative cancellation gives interactive providers
                    // their protocol-level interrupt before the abstraction's
                    // bounded process-group fallback. Dropping this handle
                    // skipped Codex `turn/interrupt`, leaving the server-owned
                    // turn alive after AgencyProxy reported it canceled.
                    let _ = run.cancel().await;
                    registry
                        .publish_error(&run_id, "the run was canceled".into(), RunState::Canceled)
                        .await;
                    return;
                };
                let Some(event) = event else { break };
                registry.publish_provider(&run_id, event).await;
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
        }));
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
        self.activity.count()
    }

    /// Wait until every run accepted by this registry reaches a terminal state.
    pub async fn wait_until_idle(&self) {
        self.activity.wait_until_idle().await;
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
        // Take the completion flag before delivering cancellation. A fast
        // provider can emit its terminal event in the same scheduling turn;
        // the flag stays set once set, so the acknowledgement is race-free.
        let (cancel, completed) = {
            let mut runs = self.runs.write().await;
            let run = runs.get_mut(run_id).ok_or(RuntimeError::NotFound)?;
            let cancel = run.cancel.take().ok_or(RuntimeError::Conflict)?;
            (cancel, Arc::clone(&run.completed))
        };
        let _ = cancel.send(());
        wait_until_completed(completed).await;
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
        interrupt(&request, &self.reactor)
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
        let was_active = is_active_state(&run.snapshot.state);
        if let Some(state) = state {
            run.snapshot.state = state;
        }
        let finished = was_active && !is_active_state(&run.snapshot.state);
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
        if finished {
            run.completed.set();
            self.activity.finished();
        }
    }
}

async fn wait_until_completed(completed: Arc<Flag>) {
    completed.wait().await;
}

fn is_active_state(state: &RunState) -> bool {
    matches!(
        state,
        RunState::Starting | RunState::Running | RunState::WaitingApproval | RunState::Finishing
    )
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
        Agent::Grok => "grok",
    }
}

fn agent_for_provider(provider: &str) -> Result<Agent, RuntimeError> {
    match provider {
        "claude" => Ok(Agent::Claude),
        "codex" => Ok(Agent::Codex),
        "copilot" => Ok(Agent::Copilot),
        "grok" => Ok(Agent::Grok),
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

    /// Long enough for the shared pool to have polled a freshly spawned task.
    ///
    /// The tokio tests ran on a current-thread runtime, where one `yield_now`
    /// guaranteed the spawned waiter had been polled. nagoya's pool runs it on
    /// another thread, so a yield on this one proves nothing about it; a short
    /// sleep gives it time to park. The assertions only ever check that the
    /// waiter has *not* finished, so a slow pool cannot make them flaky.
    const SETTLE: std::time::Duration = std::time::Duration::from_millis(20);

    #[test]
    fn activity_waits_for_the_last_run_without_polling() {
        nagoya::block_on(async {
            let activity = RunActivity::default();
            activity.started();
            activity.started();

            let waiter = nagoya::spawn({
                let activity = activity.clone();
                async move { activity.wait_until_idle().await }
            });
            nagoya::sleep(SETTLE).await;
            assert!(!waiter.is_finished());

            activity.finished();
            nagoya::sleep(SETTLE).await;
            assert!(!waiter.is_finished());

            activity.finished();
            waiter.await.expect("the waiter should run to completion");
            assert_eq!(activity.count(), 0);
        });
    }

    #[test]
    fn activity_wait_returns_immediately_when_idle() {
        nagoya::block_on(RunActivity::default().wait_until_idle());
    }

    #[test]
    fn completion_wait_observes_a_signal_without_polling() {
        nagoya::block_on(async {
            let completed = Arc::new(Flag::default());
            let waiter = nagoya::spawn(wait_until_completed(Arc::clone(&completed)));
            nagoya::sleep(SETTLE).await;
            assert!(!waiter.is_finished());

            completed.set();
            waiter.await.expect("the waiter should run to completion");
        });
    }

    #[test]
    fn completion_wait_returns_immediately_after_the_signal() {
        nagoya::block_on(async {
            let completed = Arc::new(Flag::default());
            completed.set();
            wait_until_completed(completed).await;
        });
    }
}

use crate::{RuntimeError, RuntimeRegistry};
use agency_proxy_protocol::{
    ApprovalDecision, ProviderAccountUsage, ProviderStatus, RunEvent, RunId, RunRequest,
    RunSnapshot,
};
use async_trait::async_trait;
use endpoint_libs::{
    libs::{
        error_code::ErrorCode,
        handler::{HandlerError, RequestHandler, Response},
        peer::PeerIdentity,
        toolbox::{ArcToolbox, RequestContext, TOOLBOX},
        ws::{
            AuthController, ConnectionId, OnDisconnect, WebsocketServer, WsConnection, WsRequest,
            WsResponse, WsServerConfig, mcp::McpServerInfo, toolbox::CustomError,
        },
    },
    model::TypeRegistry,
};
use futures::{FutureExt, future::LocalBoxFuture};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

const AUTHENTICATED_ROLE: u32 = 1;

#[derive(Clone, Debug)]
pub struct WebSocketConfig {
    pub address: SocketAddr,
    pub authentication_key: String,
    pub allowed_origins: Vec<String>,
    pub tls: Option<WebSocketTlsConfig>,
}

#[derive(Clone, Debug)]
pub struct WebSocketTlsConfig {
    pub certificates: Vec<PathBuf>,
    pub private_key: PathBuf,
}

struct Authentication {
    protocol: String,
}

impl AuthController for Authentication {
    fn auth(
        self: Arc<Self>,
        _toolbox: &ArcToolbox,
        header: String,
        connection: Arc<WsConnection>,
    ) -> LocalBoxFuture<'static, eyre::Result<()>> {
        async move {
            let authenticated = header
                .split(',')
                .map(str::trim)
                .any(|protocol| protocol == self.protocol);
            if !authenticated {
                eyre::bail!("AgencyProxy authentication failed");
            }
            connection.set_roles(Arc::new(vec![AUTHENTICATED_ROLE]));
            Ok(())
        }
        .boxed_local()
    }
}

pub async fn serve_websocket(
    registry: RuntimeRegistry,
    config: WebSocketConfig,
) -> eyre::Result<()> {
    let drain_registry = registry.clone();
    let (insecure, pub_certs, priv_key) = match config.tls {
        Some(tls) => (false, Some(tls.certificates), Some(tls.private_key)),
        None => (true, None, None),
    };
    let mut server = WebsocketServer::new(WsServerConfig {
        name: "agency-proxy".into(),
        address: config.address.to_string(),
        insecure,
        pub_certs,
        priv_key,
        allow_cors_urls: Arc::new(Some(config.allowed_origins)),
        drop_conn_on_buffer_full: true,
        mcp_only: true,
        ..Default::default()
    });
    server.set_auth_controller(Authentication {
        protocol: format!("agency-proxy.{}", config.authentication_key),
    });
    let subscriptions = RunSubscriptions::default();
    server.add_on_disconnect_hook(subscriptions.clone());
    server.add_handler(MethodListRuns(registry.clone()));
    server.add_handler(MethodReadRun(registry.clone()));
    server.add_handler(MethodStartRun(registry.clone()));
    server.add_handler(MethodInjectMessage(registry.clone()));
    server.add_handler(MethodDecideApproval(registry.clone()));
    server.add_handler(MethodCancelRun(registry.clone()));
    server.add_handler(MethodAcknowledgeRun(registry.clone()));
    server.add_handler(MethodProbeProviders(registry.clone()));
    server.add_handler(MethodReadAccountUsage(registry));
    server.add_handler(MethodSubscribeRun {
        registry: drain_registry.clone(),
        subscriptions: subscriptions.clone(),
    });
    server.add_handler(MethodUnsubscribeRun { subscriptions });
    server.enable_mcp(
        &TypeRegistry::new(),
        McpServerInfo {
            name: "agency-proxy".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
    )?;
    let result = server.listen().await;
    let active = drain_registry.active_count().await;
    if active > 0 {
        eprintln!("AgencyProxy WebSocket admission closed; draining {active} active run(s)");
    }
    // endpoint-libs closes admission before returning from `listen` on TERM.
    // Keep the process alive until every provider already accepted by this
    // registry settles, matching the Unix transport's drain restart contract.
    while drain_registry.active_count().await > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    if active > 0 {
        eprintln!("AgencyProxy WebSocket runs drained");
    }
    result
}

type SubscriptionKey = (ConnectionId, String);

#[derive(Clone, Default)]
struct RunSubscriptions {
    active: Arc<Mutex<HashMap<SubscriptionKey, (u64, tokio::task::AbortHandle)>>>,
    next_generation: Arc<AtomicU64>,
}

impl RunSubscriptions {
    fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn insert(&self, key: SubscriptionKey, generation: u64, handle: tokio::task::AbortHandle) {
        if let Ok(mut active) = self.active.lock()
            && let Some((_, previous)) = active.insert(key, (generation, handle))
        {
            previous.abort();
        }
    }

    fn remove_if_current(&self, key: &SubscriptionKey, generation: u64) {
        if let Ok(mut active) = self.active.lock()
            && active
                .get(key)
                .is_some_and(|(current, _)| *current == generation)
        {
            active.remove(key);
        }
    }

    fn unsubscribe(&self, connection_id: ConnectionId, run_id: &str) -> bool {
        let key = (connection_id, run_id.to_string());
        let removed = self
            .active
            .lock()
            .ok()
            .and_then(|mut active| active.remove(&key));
        if let Some((_, handle)) = removed {
            handle.abort();
            true
        } else {
            false
        }
    }

    fn disconnect(&self, connection_id: ConnectionId) {
        let removed = self
            .active
            .lock()
            .map(|mut active| {
                let keys = active
                    .keys()
                    .filter(|(candidate, _)| *candidate == connection_id)
                    .cloned()
                    .collect::<Vec<_>>();
                keys.into_iter()
                    .filter_map(|key| active.remove(&key))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for (_, handle) in removed {
            handle.abort();
        }
    }
}

#[async_trait(?Send)]
impl OnDisconnect for RunSubscriptions {
    async fn on_disconnect(&self, connection_id: ConnectionId, _peer: &PeerIdentity) {
        self.disconnect(connection_id);
    }
}

fn runtime_error(error: RuntimeError) -> HandlerError<CustomError> {
    let code = match error {
        RuntimeError::Conflict => ErrorCode::CONFLICT,
        RuntimeError::NotFound => ErrorCode::NOT_FOUND,
        RuntimeError::ReplayExpired { .. } => ErrorCode::CONFLICT,
        RuntimeError::Provider(_) | RuntimeError::Permission(_) => ErrorCode::BAD_REQUEST,
        RuntimeError::Start(_) | RuntimeError::Control(_) => ErrorCode::INTERNAL_ERROR,
    };
    CustomError::new(code)
        .with_message(error.to_string())
        .into()
}

macro_rules! response {
    ($name:ident, $request:ident, { $($field:ident: $ty:ty),* $(,)? }) => {
        #[derive(Clone, Debug, Deserialize, Serialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name { $(pub $field: $ty),* }

        impl WsResponse for $name {
            type Request = $request;
        }
    };
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListRunsRequest {}

response!(ListRunsResponse, ListRunsRequest, { runs: Vec<RunSnapshot> });

impl WsRequest for ListRunsRequest {
    type Response = ListRunsResponse;
    const METHOD_ID: u32 = 1;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"ListRuns","code":1,"parameters":[],
        "returns":[{"name":"runs","ty":{"Vec":"Object"}}],
        "description":"Lists every run currently owned by AgencyProxy.","roles":[]
    }"#;
}

struct MethodListRuns(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodListRuns {
    type Request = ListRunsRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        _req: ListRunsRequest,
    ) -> Response<ListRunsRequest> {
        Ok(ListRunsResponse {
            runs: self.0.list().await,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadRunRequest {
    pub run_id: String,
    #[serde(default)]
    pub after_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebRunEvent {
    pub sequence: u64,
    pub event: RunEvent,
}

response!(ReadRunResponse, ReadRunRequest, {
    run: RunSnapshot,
    events: Vec<WebRunEvent>,
});

impl WsRequest for ReadRunRequest {
    type Response = ReadRunResponse;
    const METHOD_ID: u32 = 2;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"ReadRun","code":2,
        "parameters":[{"name":"runId","ty":"String"},{"name":"afterSequence","ty":"Int64"}],
        "returns":[{"name":"run","ty":"Object"},{"name":"events","ty":{"Vec":"Object"}}],
        "description":"Reads one run snapshot and replays events after a sequence cursor.","roles":[]
    }"#;
}

struct MethodReadRun(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodReadRun {
    type Request = ReadRunRequest;
    type Error = CustomError;

    async fn handle(&self, _ctx: RequestContext, req: ReadRunRequest) -> Response<ReadRunRequest> {
        let attachment = self
            .0
            .attach(&RunId(req.run_id), req.after_sequence)
            .await
            .map_err(runtime_error)?;
        Ok(ReadRunResponse {
            run: attachment.snapshot,
            events: attachment
                .replay
                .into_iter()
                .map(|event| WebRunEvent {
                    sequence: event.sequence,
                    event: event.event,
                })
                .collect(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRunRequest {
    pub run_id: String,
    #[serde(default)]
    pub after_sequence: u64,
}

response!(SubscribeRunResponse, SubscribeRunRequest, {
    run: RunSnapshot,
    events: Vec<WebRunEvent>,
});

impl WsRequest for SubscribeRunRequest {
    type Response = SubscribeRunResponse;
    const METHOD_ID: u32 = 10;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"SubscribeRun","code":10,
        "parameters":[{"name":"runId","ty":"String"},{"name":"afterSequence","ty":"Int64"}],
        "returns":[{"name":"run","ty":"Object"},{"name":"events","ty":{"Vec":"Object"}}],
        "description":"Returns a run snapshot and ordered replay, then pushes new events as MCP notifications/run_event notifications on this WebSocket.","roles":[]
    }"#;
}

struct MethodSubscribeRun {
    registry: RuntimeRegistry,
    subscriptions: RunSubscriptions,
}

#[async_trait(?Send)]
impl RequestHandler for MethodSubscribeRun {
    type Request = SubscribeRunRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        ctx: RequestContext,
        req: SubscribeRunRequest,
    ) -> Response<SubscribeRunRequest> {
        let run_id = RunId(req.run_id.clone());
        let attachment = self
            .registry
            .attach(&run_id, req.after_sequence)
            .await
            .map_err(runtime_error)?;
        let replay = attachment
            .replay
            .iter()
            .map(|event| WebRunEvent {
                sequence: event.sequence,
                event: event.event.clone(),
            })
            .collect::<Vec<_>>();
        let last_sequence = attachment
            .replay
            .last()
            .map_or(req.after_sequence, |event| event.sequence);
        let key = (ctx.connection_id, req.run_id);
        let generation = self.subscriptions.next_generation();
        let subscriptions = self.subscriptions.clone();
        let cleanup_key = key.clone();
        let registry = self.registry.clone();
        let toolbox = TOOLBOX.with(Arc::clone);
        let connection_id = ctx.connection_id;
        let task = tokio::task::spawn_local(async move {
            forward_run_events(
                registry,
                toolbox,
                connection_id,
                run_id,
                last_sequence,
                attachment.events,
            )
            .await;
            subscriptions.remove_if_current(&cleanup_key, generation);
        });
        self.subscriptions
            .insert(key, generation, task.abort_handle());
        drop(task);

        Ok(SubscribeRunResponse {
            run: attachment.snapshot,
            events: replay,
        })
    }
}

async fn forward_run_events(
    registry: RuntimeRegistry,
    toolbox: ArcToolbox,
    connection_id: ConnectionId,
    run_id: RunId,
    mut last_sequence: u64,
    mut events: tokio::sync::broadcast::Receiver<crate::SequencedEvent>,
) {
    loop {
        match events.recv().await {
            Ok(event) => {
                if event.sequence <= last_sequence {
                    continue;
                }
                last_sequence = event.sequence;
                if !send_run_event(&toolbox, connection_id, &event) {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                let Ok(attachment) = registry.attach(&run_id, last_sequence).await else {
                    return;
                };
                for event in attachment.replay {
                    if event.sequence <= last_sequence {
                        continue;
                    }
                    last_sequence = event.sequence;
                    if !send_run_event(&toolbox, connection_id, &event) {
                        return;
                    }
                }
                events = attachment.events;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

fn send_run_event(
    toolbox: &ArcToolbox,
    connection_id: ConnectionId,
    event: &crate::SequencedEvent,
) -> bool {
    toolbox.send_raw(
        connection_id,
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/run_event",
            "params": {
                "runId": event.run_id,
                "sequence": event.sequence,
                "event": event.event,
            }
        })
        .to_string(),
    )
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeRunRequest {
    pub run_id: String,
}

response!(UnsubscribeRunResponse, UnsubscribeRunRequest, { removed: bool });

impl WsRequest for UnsubscribeRunRequest {
    type Response = UnsubscribeRunResponse;
    const METHOD_ID: u32 = 11;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"UnsubscribeRun","code":11,
        "parameters":[{"name":"runId","ty":"String"}],
        "returns":[{"name":"removed","ty":"Boolean"}],
        "description":"Stops MCP run-event notifications for one run on this WebSocket.","roles":[]
    }"#;
}

struct MethodUnsubscribeRun {
    subscriptions: RunSubscriptions,
}

#[async_trait(?Send)]
impl RequestHandler for MethodUnsubscribeRun {
    type Request = UnsubscribeRunRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        ctx: RequestContext,
        req: UnsubscribeRunRequest,
    ) -> Response<UnsubscribeRunRequest> {
        Ok(UnsubscribeRunResponse {
            removed: self
                .subscriptions
                .unsubscribe(ctx.connection_id, &req.run_id),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRunRequest {
    pub run_id: String,
    pub request: RunRequest,
}

response!(StartRunResponse, StartRunRequest, { accepted: bool });

impl WsRequest for StartRunRequest {
    type Response = StartRunResponse;
    const METHOD_ID: u32 = 3;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"StartRun","code":3,
        "parameters":[{"name":"runId","ty":"String"},{"name":"request","ty":"Object"}],
        "returns":[{"name":"accepted","ty":"Boolean"}],
        "description":"Starts a coding-agent run under a stable caller-supplied run id.","roles":[]
    }"#;
}

struct MethodStartRun(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodStartRun {
    type Request = StartRunRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        req: StartRunRequest,
    ) -> Response<StartRunRequest> {
        self.0
            .start(RunId(req.run_id), req.request)
            .await
            .map_err(runtime_error)?;
        Ok(StartRunResponse { accepted: true })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectMessageRequest {
    pub run_id: String,
    pub body: String,
}

response!(InjectMessageResponse, InjectMessageRequest, { accepted: bool });

impl WsRequest for InjectMessageRequest {
    type Response = InjectMessageResponse;
    const METHOD_ID: u32 = 4;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"InjectMessage","code":4,
        "parameters":[{"name":"runId","ty":"String"},{"name":"body","ty":"String"}],
        "returns":[{"name":"accepted","ty":"Boolean"}],
        "description":"Injects a follow-up message into a live interactive run.","roles":[]
    }"#;
}

struct MethodInjectMessage(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodInjectMessage {
    type Request = InjectMessageRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        req: InjectMessageRequest,
    ) -> Response<InjectMessageRequest> {
        self.0
            .inject(&RunId(req.run_id), &req.body)
            .await
            .map_err(runtime_error)?;
        Ok(InjectMessageResponse { accepted: true })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecideApprovalRequest {
    pub run_id: String,
    pub approval_id: String,
    pub decision: ApprovalDecision,
}

response!(DecideApprovalResponse, DecideApprovalRequest, { accepted: bool });

impl WsRequest for DecideApprovalRequest {
    type Response = DecideApprovalResponse;
    const METHOD_ID: u32 = 5;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"DecideApproval","code":5,
        "parameters":[{"name":"runId","ty":"String"},{"name":"approvalId","ty":"String"},{"name":"decision","ty":"String"}],
        "returns":[{"name":"accepted","ty":"Boolean"}],
        "description":"Answers a pending provider approval request.","roles":[]
    }"#;
}

struct MethodDecideApproval(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodDecideApproval {
    type Request = DecideApprovalRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        req: DecideApprovalRequest,
    ) -> Response<DecideApprovalRequest> {
        self.0
            .decide(&RunId(req.run_id), &req.approval_id, req.decision)
            .await
            .map_err(runtime_error)?;
        Ok(DecideApprovalResponse { accepted: true })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunRequest {
    pub run_id: String,
}

response!(CancelRunResponse, CancelRunRequest, { accepted: bool });

impl WsRequest for CancelRunRequest {
    type Response = CancelRunResponse;
    const METHOD_ID: u32 = 6;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"CancelRun","code":6,"parameters":[{"name":"runId","ty":"String"}],
        "returns":[{"name":"accepted","ty":"Boolean"}],
        "description":"Cooperatively cancels a live run.","roles":[]
    }"#;
}

struct MethodCancelRun(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodCancelRun {
    type Request = CancelRunRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        req: CancelRunRequest,
    ) -> Response<CancelRunRequest> {
        self.0
            .cancel(&RunId(req.run_id))
            .await
            .map_err(runtime_error)?;
        Ok(CancelRunResponse { accepted: true })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcknowledgeRunRequest {
    pub run_id: String,
    pub through_sequence: u64,
}

response!(AcknowledgeRunResponse, AcknowledgeRunRequest, { accepted: bool });

impl WsRequest for AcknowledgeRunRequest {
    type Response = AcknowledgeRunResponse;
    const METHOD_ID: u32 = 7;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"AcknowledgeRun","code":7,
        "parameters":[{"name":"runId","ty":"String"},{"name":"throughSequence","ty":"Int64"}],
        "returns":[{"name":"accepted","ty":"Boolean"}],
        "description":"Acknowledges replayed run events through a sequence cursor.","roles":[]
    }"#;
}

struct MethodAcknowledgeRun(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodAcknowledgeRun {
    type Request = AcknowledgeRunRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        req: AcknowledgeRunRequest,
    ) -> Response<AcknowledgeRunRequest> {
        self.0
            .acknowledge(&RunId(req.run_id), req.through_sequence)
            .await
            .map_err(runtime_error)?;
        Ok(AcknowledgeRunResponse { accepted: true })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProbeProvidersRequest {}

response!(ProbeProvidersResponse, ProbeProvidersRequest, { providers: Vec<ProviderStatus> });

impl WsRequest for ProbeProvidersRequest {
    type Response = ProbeProvidersResponse;
    const METHOD_ID: u32 = 8;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"ProbeProviders","code":8,"parameters":[],
        "returns":[{"name":"providers","ty":{"Vec":"Object"}}],
        "description":"Reports installed coding-agent providers and their authentication state.","roles":[]
    }"#;
}

struct MethodProbeProviders(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodProbeProviders {
    type Request = ProbeProvidersRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        _req: ProbeProvidersRequest,
    ) -> Response<ProbeProvidersRequest> {
        Ok(ProbeProvidersResponse {
            providers: self.0.probe_providers().await,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadAccountUsageRequest {}

response!(ReadAccountUsageResponse, ReadAccountUsageRequest, {
    providers: Vec<ProviderAccountUsage>,
});

impl WsRequest for ReadAccountUsageRequest {
    type Response = ReadAccountUsageResponse;
    const METHOD_ID: u32 = 9;
    const ROLES: &'static [u32] = &[AUTHENTICATED_ROLE];
    const SCHEMA: &'static str = r#"{
        "name":"ReadAccountUsage","code":9,"parameters":[],
        "returns":[{"name":"providers","ty":{"Vec":"Object"}}],
        "description":"Reads provider account usage when supported by the installed CLI.","roles":[]
    }"#;
}

struct MethodReadAccountUsage(RuntimeRegistry);

#[async_trait(?Send)]
impl RequestHandler for MethodReadAccountUsage {
    type Request = ReadAccountUsageRequest;
    type Error = CustomError;

    async fn handle(
        &self,
        _ctx: RequestContext,
        _req: ReadAccountUsageRequest,
    ) -> Response<ReadAccountUsageRequest> {
        Ok(ReadAccountUsageResponse {
            providers: self.0.account_usage().await,
        })
    }
}

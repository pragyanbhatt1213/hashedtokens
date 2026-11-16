use super::submission::EdmsConfig;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    WebSocketStream, connect_async,
    tungstenite::{Error as WsError, Message},
};

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct ApiStatus {
    pub(crate) code: i64,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct ApiResult {
    pub(crate) status: ApiStatus,
    #[serde(default)]
    pub(crate) detail: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct Job {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) op: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
    #[serde(default)]
    pub(crate) started_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) finished_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) result: Option<ApiResult>,
    #[serde(default)]
    pub(crate) error: Option<Value>,
    #[serde(default)]
    pub(crate) progress: Option<Value>,
    #[serde(default)]
    pub(crate) age_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TransactionSuccess {
    pub(crate) transaction_id: String,
    pub(crate) op: Option<String>,
    pub(crate) result: ApiResult,
}

#[derive(Debug, Error)]
pub(crate) enum TransportError {
    #[error("invalid EDMS URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("EDMS timeout must be greater than zero")]
    InvalidTimeout,
    #[error("transaction ID cannot be empty")]
    EmptyTransactionId,
    #[error("unsupported EDMS URL scheme '{scheme}'")]
    UnsupportedScheme { scheme: String },
    #[error("EDMS WebSocket connection failed: {0}")]
    Connection(#[source] WsError),
    #[error("EDMS WebSocket connection timed out")]
    ConnectTimeout,
    #[error("initial EDMS WebSocket message was not a transaction snapshot")]
    UnexpectedSnapshot,
    #[error("another waiter is already registered for this transaction")]
    AlreadyWaiting,
    #[error("EDMS WebSocket state lock was poisoned")]
    Poisoned,
    #[error("EDMS job recovery connection failed: {0}")]
    RecoveryConnection(#[source] reqwest::Error),
    #[error("EDMS job recovery timed out: {0}")]
    RecoveryTimeout(#[source] reqwest::Error),
    #[error("EDMS job recovery failed: {0}")]
    RecoveryRequest(#[source] reqwest::Error),
    #[error("EDMS job recovery returned HTTP {status}: {body}")]
    HttpStatus { status: StatusCode, body: String },
    #[error("invalid EDMS job JSON: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("EDMS job '{transaction_id}' was not found")]
    JobNotFound { transaction_id: String },
    #[error("EDMS job '{transaction_id}' completed without a result")]
    MissingResult { transaction_id: String },
    #[error("EDMS job status '{status}' is not supported")]
    UnexpectedJobStatus { status: String },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TransactionFailure {
    pub(crate) transaction_id: String,
    pub(crate) op: Option<String>,
    pub(crate) result: Option<ApiResult>,
    pub(crate) error: Option<Value>,
}

#[derive(Debug, Error)]
pub(crate) enum WaitError {
    #[error(transparent)]
    Transport(#[from] Box<TransportError>),
    #[error("EDMS transaction {} failed", .0.transaction_id)]
    Transaction(Box<TransactionFailure>),
    #[error("EDMS transaction {transaction_id} is still running")]
    StillRunning { transaction_id: String },
}

impl From<TransportError> for WaitError {
    fn from(error: TransportError) -> Self {
        Self::Transport(Box::new(error))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EdmsConnection {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    _snapshot: Vec<Job>,
    state: Arc<Mutex<State>>,
    timeout: Duration,
    base_url: String,
    reader: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct State {
    alive: bool,
    seen: HashSet<String>,
    buffered: HashMap<String, ParsedEvent>,
    waiters: HashMap<String, oneshot::Sender<Notice>>,
}

#[derive(Debug)]
enum Notice {
    Event(ParsedEvent),
    Disconnected,
}

#[derive(Debug)]
struct ParsedEvent {
    transaction_id: String,
    op: Option<String>,
    result: ApiResult,
}

#[derive(Debug, Deserialize)]
struct SnapshotMessage {
    transactions: Vec<Job>,
}

#[derive(Debug)]
enum Incoming {
    Snapshot(Vec<Job>),
    DevApi(ParsedEvent),
    Ignored,
    Malformed,
}

impl EdmsConnection {
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> &[Job] {
        &self.inner._snapshot
    }

    pub(crate) async fn wait_for(
        &self,
        transaction_id: &str,
    ) -> Result<TransactionSuccess, WaitError> {
        if transaction_id.is_empty() {
            return Err(TransportError::EmptyTransactionId.into());
        }

        enum Phase {
            HaveEvent(ParsedEvent),
            NeedRecovery,
            WillWait(oneshot::Receiver<Notice>),
            AlreadyWaiting,
            Poisoned,
        }

        let phase = {
            match self.inner.state.lock() {
                Err(_) => Phase::Poisoned,
                Ok(mut state) => {
                    if let Some(event) = state.buffered.remove(transaction_id) {
                        Phase::HaveEvent(event)
                    } else if !state.alive {
                        Phase::NeedRecovery
                    } else if state.waiters.contains_key(transaction_id) {
                        Phase::AlreadyWaiting
                    } else {
                        let (sender, receiver) = oneshot::channel();
                        state.waiters.insert(transaction_id.to_owned(), sender);
                        Phase::WillWait(receiver)
                    }
                }
            }
        };

        match phase {
            Phase::Poisoned => Err(TransportError::Poisoned.into()),
            Phase::HaveEvent(event) => outcome_from_event(event),
            Phase::NeedRecovery => self.recover(transaction_id).await,
            Phase::AlreadyWaiting => Err(TransportError::AlreadyWaiting.into()),
            Phase::WillWait(receiver) => match timeout(self.inner.timeout, receiver).await {
                Ok(Ok(Notice::Event(event))) => outcome_from_event(event),
                Ok(Ok(Notice::Disconnected)) | Ok(Err(_)) => self.recover(transaction_id).await,
                Err(_) => {
                    let pre_recovery_event = match self.inner.state.lock() {
                        Err(_) => return Err(TransportError::Poisoned.into()),
                        Ok(mut state) => {
                            state.waiters.remove(transaction_id);
                            state.buffered.remove(transaction_id)
                        }
                    };
                    if let Some(event) = pre_recovery_event {
                        return outcome_from_event(event);
                    }

                    let recovery_outcome = self.recover(transaction_id).await;

                    let post_recovery_event = match self.inner.state.lock() {
                        Err(_) => return Err(TransportError::Poisoned.into()),
                        Ok(mut state) => state.buffered.remove(transaction_id),
                    };
                    if let Some(event) = post_recovery_event {
                        outcome_from_event(event)
                    } else {
                        recovery_outcome
                    }
                }
            },
        }
    }

    async fn recover(&self, transaction_id: &str) -> Result<TransactionSuccess, WaitError> {
        let config = EdmsConfig {
            base_url: self.inner.base_url.clone(),
            timeout: self.inner.timeout,
        };
        outcome_from_job(fetch_job(&config, transaction_id).await?)
    }

    #[cfg(test)]
    fn pending_waiters(&self) -> usize {
        self.inner
            .state
            .lock()
            .map(|state| state.waiters.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn is_connected(&self) -> bool {
        self.inner
            .state
            .lock()
            .map(|state| state.alive)
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn buffered_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .map(|state| state.buffered.len())
            .unwrap_or(0)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.reader.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
    }
}

pub(crate) async fn connect(config: &EdmsConfig) -> Result<EdmsConnection, TransportError> {
    validate_timeout(config.timeout)?;
    let endpoint = ws_endpoint(&config.base_url)?;
    let connected = timeout(config.timeout, open_socket(endpoint)).await;
    let (stream, jobs) = match connected {
        Ok(result) => result?,
        Err(_) => return Err(TransportError::ConnectTimeout),
    };
    let state = Arc::new(Mutex::new(State {
        alive: true,
        seen: HashSet::new(),
        buffered: HashMap::new(),
        waiters: HashMap::new(),
    }));
    let reader = tokio::spawn(read_socket(stream, Arc::clone(&state)));
    Ok(EdmsConnection {
        inner: Arc::new(Inner {
            _snapshot: jobs,
            state,
            timeout: config.timeout,
            base_url: config.base_url.clone(),
            reader: Mutex::new(Some(reader)),
        }),
    })
}

pub(crate) async fn fetch_job(
    config: &EdmsConfig,
    transaction_id: &str,
) -> Result<Job, TransportError> {
    validate_timeout(config.timeout)?;
    let url = job_endpoint(&config.base_url, transaction_id)?;
    let client = Client::builder()
        .timeout(config.timeout)
        .build()
        .map_err(TransportError::RecoveryRequest)?;
    let response = client.get(url).send().await.map_err(map_recovery_error)?;
    let status = response.status();
    let body = response.bytes().await.map_err(map_recovery_error)?;
    if status == StatusCode::NOT_FOUND {
        return Err(TransportError::JobNotFound {
            transaction_id: transaction_id.to_owned(),
        });
    }
    if status != StatusCode::OK {
        return Err(TransportError::HttpStatus {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    serde_json::from_slice(&body).map_err(TransportError::InvalidJson)
}

pub(crate) fn outcome_from_job(job: Job) -> Result<TransactionSuccess, WaitError> {
    let transaction_id = job.id;
    match job.status.as_deref() {
        Some("running") => Err(WaitError::StillRunning { transaction_id }),
        Some("failed") => Err(WaitError::Transaction(Box::new(TransactionFailure {
            transaction_id,
            op: job.op,
            result: job.result,
            error: job.error,
        }))),
        Some("done") => match job.result {
            Some(result) => outcome_from_result(transaction_id, job.op, result, None),
            None => Err(TransportError::MissingResult { transaction_id }.into()),
        },
        Some(status) => Err(TransportError::UnexpectedJobStatus {
            status: status.to_owned(),
        }
        .into()),
        None => Err(TransportError::UnexpectedJobStatus {
            status: "missing".to_owned(),
        }
        .into()),
    }
}

async fn open_socket(endpoint: Url) -> Result<(WsStream, Vec<Job>), TransportError> {
    let (mut stream, _) = connect_async(endpoint.as_str())
        .await
        .map_err(TransportError::Connection)?;
    let text = read_text(&mut stream).await?;
    match parse_incoming(&text) {
        Incoming::Snapshot(jobs) => Ok((stream, jobs)),
        Incoming::DevApi(_) | Incoming::Ignored | Incoming::Malformed => {
            Err(TransportError::UnexpectedSnapshot)
        }
    }
}

async fn read_text(stream: &mut WsStream) -> Result<String, TransportError> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
            Some(Ok(Message::Ping(payload))) => stream
                .send(Message::Pong(payload))
                .await
                .map_err(TransportError::Connection)?,
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Binary(_) | Message::Close(_) | Message::Frame(_))) | None => {
                return Err(TransportError::UnexpectedSnapshot);
            }
            Some(Err(error)) => return Err(TransportError::Connection(error)),
        }
    }
}

async fn read_socket(mut stream: WsStream, state: Arc<Mutex<State>>) {
    loop {
        let message = match stream.next().await {
            Some(Ok(message)) => message,
            Some(Err(_error)) => break,
            None => break,
        };
        match message {
            Message::Text(text) => {
                if let Incoming::DevApi(event) = parse_incoming(&text) {
                    let mut state = lock_or_recover(&state);
                    publish(&mut state, event);
                }
            }
            Message::Ping(payload) => {
                if stream.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            Message::Close(_) => break,
        }
    }
    mark_closed(&state);
}

fn lock_or_recover(state: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    state.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn publish(state: &mut State, event: ParsedEvent) {
    let id = event.transaction_id.clone();
    if !state.seen.insert(id.clone()) {
        return;
    }
    if let Some(waiter) = state.waiters.remove(&id) {
        if let Err(Notice::Event(event)) = waiter.send(Notice::Event(event)) {
            state.buffered.insert(id, event);
        }
    } else {
        state.buffered.insert(id, event);
    }
}

fn mark_closed(state: &Mutex<State>) {
    let mut state = lock_or_recover(state);
    state.alive = false;
    for (_, waiter) in state.waiters.drain() {
        let _ = waiter.send(Notice::Disconnected);
    }
}

fn parse_incoming(text: &str) -> Incoming {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return Incoming::Malformed;
    };
    match value.get("type").and_then(Value::as_str) {
        Some("transactions") => match serde_json::from_value::<SnapshotMessage>(value) {
            Ok(snapshot) => Incoming::Snapshot(snapshot.transactions),
            Err(_) => Incoming::Malformed,
        },
        Some("event") => parse_event(&value),
        _ => Incoming::Ignored,
    }
}

fn parse_event(value: &Value) -> Incoming {
    if value.pointer("/event/type").and_then(Value::as_str) != Some("DevApiResult") {
        return Incoming::Ignored;
    }
    let payload = &value["event"]["payload"];
    let Some(transaction_id) = payload
        .get("transaction_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return Incoming::Malformed;
    };
    let Ok(result) = serde_json::from_value::<ApiResult>(payload["result"].clone()) else {
        return Incoming::Malformed;
    };
    Incoming::DevApi(ParsedEvent {
        transaction_id: transaction_id.to_owned(),
        op: payload.get("op").and_then(Value::as_str).map(str::to_owned),
        result,
    })
}

fn outcome_from_event(event: ParsedEvent) -> Result<TransactionSuccess, WaitError> {
    outcome_from_result(event.transaction_id, event.op, event.result, None)
}

fn outcome_from_result(
    transaction_id: String,
    op: Option<String>,
    result: ApiResult,
    error: Option<Value>,
) -> Result<TransactionSuccess, WaitError> {
    if result.status.code == 0 {
        Ok(TransactionSuccess {
            transaction_id,
            op,
            result,
        })
    } else {
        Err(WaitError::Transaction(Box::new(TransactionFailure {
            transaction_id,
            op,
            result: Some(result),
            error,
        })))
    }
}

fn validate_timeout(timeout: Duration) -> Result<(), TransportError> {
    if timeout.is_zero() {
        Err(TransportError::InvalidTimeout)
    } else {
        Ok(())
    }
}

fn join_path(base_url: &str, path: &str) -> Result<Url, TransportError> {
    let base = Url::parse(base_url).map_err(|error| TransportError::InvalidUrl {
        url: base_url.to_owned(),
        reason: error.to_string(),
    })?;
    let base = if base.as_str().ends_with('/') {
        base
    } else {
        let with_slash = format!("{}/", base);
        Url::parse(&with_slash).map_err(|error| TransportError::InvalidUrl {
            url: base_url.to_owned(),
            reason: error.to_string(),
        })?
    };
    base.join(path).map_err(|error| TransportError::InvalidUrl {
        url: base_url.to_owned(),
        reason: error.to_string(),
    })
}

fn ws_endpoint(base_url: &str) -> Result<Url, TransportError> {
    let url = join_path(base_url, "hashedtokens/edms/api/ws")?;
    match url.scheme() {
        "ws" | "wss" => Ok(url),
        "http" => replace_scheme(url, "ws"),
        "https" => replace_scheme(url, "wss"),
        other => Err(TransportError::UnsupportedScheme {
            scheme: other.to_owned(),
        }),
    }
}

fn replace_scheme(url: Url, scheme: &str) -> Result<Url, TransportError> {
    let rest = url.as_str().trim_start_matches(url.scheme());
    Url::parse(&format!("{scheme}{rest}")).map_err(|error| TransportError::InvalidUrl {
        url: url.to_string(),
        reason: error.to_string(),
    })
}

fn job_endpoint(base_url: &str, transaction_id: &str) -> Result<Url, TransportError> {
    if transaction_id.is_empty() {
        return Err(TransportError::EmptyTransactionId);
    }
    let mut url = join_path(base_url, "jobs/")?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| TransportError::InvalidUrl {
                url: base_url.to_owned(),
                reason: "cannot append a transaction ID onto this URL".to_owned(),
            })?;
        segments.pop();
        segments.push(transaction_id);
    }
    Ok(url)
}

fn map_recovery_error(error: reqwest::Error) -> TransportError {
    if error.is_timeout() {
        TransportError::RecoveryTimeout(error)
    } else if error.is_connect() {
        TransportError::RecoveryConnection(error)
    } else {
        TransportError::RecoveryRequest(error)
    }
}

type WsStream = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{
            Path, State,
            ws::{Message, WebSocket, WebSocketUpgrade},
        },
        response::{IntoResponse, Response},
        routing::get,
    };
    use serde_json::json;
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::{net::TcpListener, sync::mpsc};

    const DEV_API_SAMPLE: &str = r#"{
  "type": "event",
  "event": {
    "type": "DevApiResult",
    "payload": {
      "transaction_id": "job-1748000000000-1",
      "op": "api_collections_insert",
      "result": {
        "status": {
          "code": 0,
          "name": "OK",
          "message": "All good"
        },
        "detail": "Operation completed"
      }
    }
  }
}"#;

    fn dev_api(id: &str, detail: &str, code: i64) -> String {
        json!({
            "type": "event",
            "event": {
                "type": "DevApiResult",
                "payload": {
                    "transaction_id": id,
                    "op": "api_collections_insert",
                    "result": {
                        "status": {
                            "code": code,
                            "name": if code == 0 { "OK" } else { "ERR" },
                            "message": "All good"
                        },
                        "detail": detail
                    }
                }
            }
        })
        .to_string()
    }

    fn done_job(id: &str, detail: &str) -> Value {
        json!({
            "id": id,
            "op": "api_collections_insert",
            "status": "done",
            "started_at_ms": 10,
            "finished_at_ms": 20,
            "result": {
                "status": {"code": 0, "name": "OK", "message": "All good"},
                "detail": detail
            },
            "error": null,
            "progress": null,
            "age_ms": 15
        })
    }

    enum ServerCommand {
        Send(String),
        Close,
    }

    struct MockEdms {
        base_url: String,
        commands: mpsc::UnboundedSender<ServerCommand>,
        hits: Arc<AtomicUsize>,
        jobs: Arc<Mutex<HashMap<String, (u16, Value)>>>,
    }

    impl MockEdms {
        async fn start(snapshot: Value) -> Self {
            let (commands, outbound) = mpsc::unbounded_channel();
            let hits = Arc::new(AtomicUsize::new(0));
            let jobs = Arc::new(Mutex::new(HashMap::new()));
            let state = MockState {
                snapshot: snapshot.to_string(),
                outbound: Arc::new(tokio::sync::Mutex::new(Some(outbound))),
                hits: Arc::clone(&hits),
                jobs: Arc::clone(&jobs),
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = Router::new()
                .route("/hashedtokens/edms/api/ws", get(ws))
                .route("/jobs/{id}", get(job))
                .with_state(state);
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                base_url: format!("http://{addr}"),
                commands,
                hits,
                jobs,
            }
        }

        fn config(&self, timeout: Duration) -> EdmsConfig {
            EdmsConfig {
                base_url: self.base_url.clone(),
                timeout,
            }
        }

        async fn connect(&self) -> EdmsConnection {
            connect(&self.config(Duration::from_secs(2)))
                .await
                .expect("websocket should connect")
        }

        fn send(&self, text: impl Into<String>) {
            self.commands
                .send(ServerCommand::Send(text.into()))
                .expect("mock socket should accept a frame");
        }

        fn close(&self) {
            self.commands
                .send(ServerCommand::Close)
                .expect("mock socket should accept close");
        }

        fn insert_job(&self, id: &str, status: u16, body: Value) {
            self.jobs
                .lock()
                .unwrap()
                .insert(id.to_owned(), (status, body));
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    #[derive(Clone)]
    struct MockState {
        snapshot: String,
        outbound: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<ServerCommand>>>>,
        hits: Arc<AtomicUsize>,
        jobs: Arc<Mutex<HashMap<String, (u16, Value)>>>,
    }

    async fn ws(upgrade: WebSocketUpgrade, State(state): State<MockState>) -> impl IntoResponse {
        upgrade.on_upgrade(move |socket| handle_socket(socket, state))
    }

    async fn handle_socket(mut socket: WebSocket, state: MockState) {
        if socket
            .send(Message::Text(state.snapshot.clone().into()))
            .await
            .is_err()
        {
            return;
        }
        let mut outbound = state.outbound.lock().await;
        let Some(mut outbound) = outbound.take() else {
            return;
        };
        while let Some(command) = outbound.recv().await {
            match command {
                ServerCommand::Send(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                ServerCommand::Close => {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
            }
        }
    }

    async fn job(Path(id): Path<String>, State(state): State<MockState>) -> Response {
        state.hits.fetch_add(1, Ordering::SeqCst);
        let jobs = state.jobs.lock().unwrap();
        match jobs.get(&id) {
            Some((status, body)) => (
                axum::http::StatusCode::from_u16(*status).unwrap(),
                Json(body.clone()),
            )
                .into_response(),
            None => axum::http::StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        let start = tokio::time::Instant::now();
        while !ready() {
            if start.elapsed() > Duration::from_secs(2) {
                panic!("timed out waiting for websocket state");
            }
            tokio::task::yield_now().await;
        }
    }

    fn detail(result: &TransactionSuccess) -> &str {
        result
            .result
            .detail
            .as_ref()
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    #[test]
    fn builds_websocket_and_job_urls() {
        let ws = ws_endpoint("http://edms.example").unwrap();
        assert_eq!(ws.as_str(), "ws://edms.example/hashedtokens/edms/api/ws");

        let ws_slash = ws_endpoint("http://edms.example/").unwrap();
        assert_eq!(
            ws_slash.as_str(),
            "ws://edms.example/hashedtokens/edms/api/ws"
        );

        let wss = ws_endpoint("https://edms.example/base/").unwrap();
        assert_eq!(
            wss.as_str(),
            "wss://edms.example/base/hashedtokens/edms/api/ws"
        );

        let wss_no_slash = ws_endpoint("https://edms.example/base").unwrap();
        assert_eq!(
            wss_no_slash.as_str(),
            "wss://edms.example/base/hashedtokens/edms/api/ws"
        );

        let job = job_endpoint("http://edms.example", "job-1").unwrap();
        assert_eq!(job.as_str(), "http://edms.example/jobs/job-1");

        let encoded = job_endpoint("http://edms.example/base/", "job 1").unwrap();
        assert_eq!(encoded.as_str(), "http://edms.example/base/jobs/job%201");

        let no_slash = job_endpoint("http://edms.example/base", "job-2").unwrap();
        assert_eq!(no_slash.as_str(), "http://edms.example/base/jobs/job-2");

        assert!(matches!(
            ws_endpoint("ftp://edms.example"),
            Err(TransportError::UnsupportedScheme { .. })
        ));

        assert!(matches!(
            job_endpoint("http://edms.example", ""),
            Err(TransportError::EmptyTransactionId)
        ));
    }

    #[test]
    fn classifies_snapshot_events_and_malformed_text() {
        let snapshot = parse_incoming(
            r#"{"type":"transactions","transactions":[{"id":"job-existing","status":"running"}]}"#,
        );
        match snapshot {
            Incoming::Snapshot(jobs) => assert_eq!(jobs[0].id, "job-existing"),
            other => panic!("expected snapshot, got {other:?}"),
        }
        assert!(matches!(
            parse_incoming(DEV_API_SAMPLE),
            Incoming::DevApi(event)
                if event.transaction_id == "job-1748000000000-1"
                    && event.op.as_deref() == Some("api_collections_insert")
                    && event.result.status.code == 0
                    && event.result.status.name.as_deref() == Some("OK")
                    && event.result.detail == Some(json!("Operation completed"))
        ));
        let with_response = r#"{"type":"event","event":{"type":"DevApiResult","payload":{"transaction_id":"job-1","op":"api_collections_insert","result":{"status":{"code":0},"detail":"Operation completed","response":null}}}}"#;
        assert!(matches!(parse_incoming(with_response), Incoming::DevApi(_)));
        assert!(matches!(
            parse_incoming(
                r#"{"type":"event","event":{"type":"Heartbeat","payload":{"transaction_id":"job-1"}}}"#
            ),
            Incoming::Ignored
        ));
        assert!(matches!(parse_incoming("not-json"), Incoming::Malformed));
        assert!(matches!(
            parse_incoming(
                r#"{"type":"event","event":{"type":"DevApiResult","payload":{"op":"api_collections_insert"}}}"#
            ),
            Incoming::Malformed
        ));
    }

    #[tokio::test]
    async fn connects_and_reads_an_empty_snapshot() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        assert!(connection.snapshot().is_empty());
        assert!(connection.is_connected());
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn parses_snapshot_without_treating_it_as_completion() {
        let server = MockEdms::start(json!({
            "type": "transactions",
            "transactions": [{
                "id": "job-existing",
                "op": "api_collections_insert",
                "status": "done",
                "started_at_ms": 10,
                "finished_at_ms": null,
                "result": {
                    "status": {"code": 0, "name": "OK", "message": "All good"},
                    "detail": "from snapshot"
                },
                "error": null,
                "progress": {"percent": 1},
                "age_ms": 5
            }]
        }))
        .await;
        let connection = connect(&server.config(Duration::from_millis(100)))
            .await
            .unwrap();
        let job = &connection.snapshot()[0];
        assert_eq!(job.id, "job-existing");
        assert_eq!(job.op.as_deref(), Some("api_collections_insert"));
        assert_eq!(job.status.as_deref(), Some("done"));
        assert_eq!(job.started_at_ms, Some(10));
        assert_eq!(job.finished_at_ms, None);
        assert_eq!(job.progress, Some(json!({"percent": 1})));
        assert_eq!(job.age_ms, Some(5));
        let error = connection.wait_for("job-existing").await.unwrap_err();
        assert!(matches!(
            error,
            WaitError::Transport(err)
                if matches!(*err, TransportError::JobNotFound { ref transaction_id } if transaction_id == "job-existing")
        ));
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn parses_and_matches_dev_api_result() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        server.send(DEV_API_SAMPLE);
        let success = connection
            .wait_for("job-1748000000000-1")
            .await
            .expect("matching event should complete");
        assert_eq!(success.transaction_id, "job-1748000000000-1");
        assert_eq!(success.op.as_deref(), Some("api_collections_insert"));
        assert_eq!(success.result.status.code, 0);
        assert_eq!(success.result.status.name.as_deref(), Some("OK"));
        assert_eq!(success.result.status.message.as_deref(), Some("All good"));
        assert_eq!(detail(&success), "Operation completed");
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn buffered_event_is_delivered_to_later_waiter() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;

        server.send(dev_api("job-buffered", "buffered detail", 0));
        wait_until(|| connection.buffered_count() == 1).await;

        let success = connection
            .wait_for("job-buffered")
            .await
            .expect("buffered event should be returned immediately");
        assert_eq!(success.transaction_id, "job-buffered");
        assert_eq!(detail(&success), "buffered detail");
        assert_eq!(connection.buffered_count(), 0);
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn ignores_unrelated_transaction_ids() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        let pending = connection.clone();
        let waiting = tokio::spawn(async move { connection.wait_for("job-wanted").await });
        wait_until(|| pending.pending_waiters() == 1).await;
        server.send(dev_api("job-other", "other", 0));
        server.send(dev_api("job-wanted", "wanted", 0));
        let success = waiting.await.unwrap().unwrap();
        assert_eq!(success.transaction_id, "job-wanted");
        assert_eq!(detail(&success), "wanted");
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn ignores_unrelated_event_types() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        let pending = connection.clone();
        let waiting = tokio::spawn(async move { connection.wait_for("job-1").await });
        wait_until(|| pending.pending_waiters() == 1).await;
        server.send(
            json!({
                "type": "event",
                "event": {"type": "Heartbeat", "payload": {"transaction_id": "job-1"}}
            })
            .to_string(),
        );
        server.send(dev_api("job-1", "after heartbeat", 0));
        let success = waiting.await.unwrap().unwrap();
        assert_eq!(detail(&success), "after heartbeat");
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn concurrent_waiters_do_not_share_results() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        let left = connection.clone();
        let right = connection.clone();
        let pending = connection.clone();
        let waiting =
            tokio::spawn(
                async move { tokio::join!(left.wait_for("job-a"), right.wait_for("job-b")) },
            );
        wait_until(|| pending.pending_waiters() == 2).await;
        server.send(dev_api("job-b", "result-b", 0));
        server.send(dev_api("job-a", "result-a", 0));
        let (first, second) = waiting.await.unwrap();
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.transaction_id, "job-a");
        assert_eq!(detail(&first), "result-a");
        assert_eq!(second.transaction_id, "job-b");
        assert_eq!(detail(&second), "result-b");
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn duplicate_waiter_cannot_take_the_same_result() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        let first = connection.clone();
        let waiting = tokio::spawn(async move { first.wait_for("job-1").await });
        wait_until(|| connection.pending_waiters() == 1).await;
        let duplicate = connection.wait_for("job-1").await.unwrap_err();
        assert!(matches!(
            duplicate,
            WaitError::Transport(err) if matches!(*err, TransportError::AlreadyWaiting)
        ));
        server.send(dev_api("job-1", "only once", 0));
        let success = waiting.await.unwrap().unwrap();
        assert_eq!(detail(&success), "only once");
    }

    #[tokio::test]
    async fn disconnect_during_active_wait_triggers_recovery() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        server.insert_job("job-1", 200, done_job("job-1", "recovered-inflight"));
        let connection = server.connect().await;
        let pending = connection.clone();

        let waiting = tokio::spawn(async move { connection.wait_for("job-1").await });
        wait_until(|| pending.pending_waiters() == 1).await;

        server.close();
        let success = waiting
            .await
            .unwrap()
            .expect("in-flight disconnect should recover");
        assert_eq!(success.transaction_id, "job-1");
        assert_eq!(detail(&success), "recovered-inflight");
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn disconnect_recovers_with_one_job_request() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        server.insert_job("job-1", 200, done_job("job-1", "recovered"));
        let connection = server.connect().await;
        server.close();
        wait_until(|| !connection.is_connected()).await;
        let success = connection.wait_for("job-1").await.unwrap();
        assert_eq!(success.transaction_id, "job-1");
        assert_eq!(detail(&success), "recovered");
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn recovery_endpoint_reports_missing_and_http_failures() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let config = server.config(Duration::from_secs(1));
        let missing = fetch_job(&config, "missing").await.unwrap_err();
        assert!(matches!(
            missing,
            TransportError::JobNotFound { ref transaction_id } if transaction_id == "missing"
        ));
        server.insert_job("broken", 200, json!("not-a-job"));
        let invalid = fetch_job(&config, "broken").await.unwrap_err();
        assert!(matches!(invalid, TransportError::InvalidJson(_)));
        server.insert_job("down", 503, json!({"error": "unavailable"}));
        let status = fetch_job(&config, "down").await.unwrap_err();
        assert!(matches!(
            status,
            TransportError::HttpStatus { status, .. } if status == StatusCode::SERVICE_UNAVAILABLE
        ));
        let empty = fetch_job(&config, "").await.unwrap_err();
        assert!(matches!(empty, TransportError::EmptyTransactionId));
    }

    #[tokio::test]
    async fn recovery_distinguishes_running_done_and_failed() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let config = server.config(Duration::from_secs(1));
        server.insert_job(
            "running-job",
            200,
            json!({"id": "running-job", "op": "api_collections_insert", "status": "running"}),
        );
        let running = outcome_from_job(fetch_job(&config, "running-job").await.unwrap());
        assert!(matches!(
            running,
            Err(WaitError::StillRunning { ref transaction_id }) if transaction_id == "running-job"
        ));

        server.insert_job("done-job", 200, done_job("done-job", "finished"));
        let done = outcome_from_job(fetch_job(&config, "done-job").await.unwrap()).unwrap();
        assert_eq!(done.transaction_id, "done-job");
        assert_eq!(detail(&done), "finished");

        server.insert_job(
            "failed-job",
            200,
            json!({
                "id": "failed-job",
                "op": "api_collections_insert",
                "status": "failed",
                "error": "insert rejected",
                "result": {"status": {"code": 2, "name": "ERR", "message": "no"}, "detail": "failed detail"}
            }),
        );
        let failed = outcome_from_job(fetch_job(&config, "failed-job").await.unwrap()).unwrap_err();
        match failed {
            WaitError::Transaction(failure) => {
                assert_eq!(failure.transaction_id, "failed-job");
                assert_eq!(failure.error, Some(json!("insert rejected")));
                assert_eq!(failure.result.unwrap().status.code, 2);
            }
            other => panic!("expected transaction failure, got {other:?}"),
        }

        let connection = connect(&server.config(Duration::from_millis(100)))
            .await
            .unwrap();
        let still_running = connection.wait_for("running-job").await.unwrap_err();
        assert!(matches!(
            still_running,
            WaitError::StillRunning { ref transaction_id } if transaction_id == "running-job"
        ));
    }

    #[tokio::test]
    async fn websocket_event_during_recovery_is_preferred_over_running_status() {
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        let gate_rx = Arc::new(tokio::sync::Mutex::new(gate_rx));

        let (commands, outbound) = mpsc::unbounded_channel::<ServerCommand>();
        let hits = Arc::new(AtomicUsize::new(0));
        let jobs: Arc<Mutex<HashMap<String, (u16, Value)>>> = Arc::new(Mutex::new(HashMap::new()));

        let snapshot_str = json!({"type": "transactions", "transactions": []}).to_string();

        let state = MockStateWithGate {
            snapshot: snapshot_str,
            outbound: Arc::new(tokio::sync::Mutex::new(Some(outbound))),
            hits: Arc::clone(&hits),
            jobs: Arc::clone(&jobs),
            gate: Arc::clone(&gate_rx),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/hashedtokens/edms/api/ws", get(ws_gate))
            .route("/jobs/{id}", get(job_gate))
            .with_state(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let config = EdmsConfig {
            base_url: format!("http://{addr}"),
            timeout: Duration::from_millis(100),
        };
        let connection = connect(&config).await.unwrap();

        jobs.lock().unwrap().insert(
            "job-race".to_owned(),
            (200, json!({"id": "job-race", "status": "running"})),
        );

        let conn = connection.clone();
        let waiting = tokio::spawn(async move { conn.wait_for("job-race").await });

        wait_until(|| hits.load(Ordering::SeqCst) == 1).await;

        commands
            .send(ServerCommand::Send(dev_api("job-race", "from-ws", 0)))
            .unwrap();

        wait_until(|| connection.buffered_count() == 1).await;

        gate_tx.send(true).unwrap();

        let success = waiting
            .await
            .unwrap()
            .expect("WebSocket result should win over StillRunning");
        assert_eq!(success.transaction_id, "job-race");
        assert_eq!(detail(&success), "from-ws");
        assert_eq!(connection.buffered_count(), 0);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missed_event_uses_one_recovery_read() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        server.insert_job("job-1", 200, done_job("job-1", "from job endpoint"));
        let connection = connect(&server.config(Duration::from_millis(100)))
            .await
            .unwrap();
        let success = connection.wait_for("job-1").await.unwrap();
        assert_eq!(detail(&success), "from job endpoint");
        assert_eq!(server.hits(), 1);
    }

    #[tokio::test]
    async fn malformed_and_duplicate_messages_do_not_change_the_match() {
        let server = MockEdms::start(json!("not-json")).await;
        let error = connect(&server.config(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(error, TransportError::UnexpectedSnapshot));

        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        let pending = connection.clone();
        let waiting = tokio::spawn(async move { connection.wait_for("job-1").await });
        wait_until(|| pending.pending_waiters() == 1).await;
        server.send("not-json");
        server.send(json!({"type": "unexpected"}).to_string());
        server.send(
            json!({
                "type": "event",
                "event": {"type": "DevApiResult", "payload": {"op": "api_collections_insert"}}
            })
            .to_string(),
        );
        server.send(dev_api("job-1", "first", 0));
        server.send(dev_api("job-1", "second", 0));
        let success = waiting.await.unwrap().unwrap();
        assert_eq!(detail(&success), "first");
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn duplicate_event_after_buffer_claim_is_dropped() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;

        server.send(dev_api("job-dup", "original", 0));
        wait_until(|| connection.buffered_count() == 1).await;

        let success = connection.wait_for("job-dup").await.unwrap();
        assert_eq!(detail(&success), "original");
        assert_eq!(connection.buffered_count(), 0);

        server.send(dev_api("job-dup", "duplicate", 0));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(connection.buffered_count(), 0);
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn non_zero_result_code_is_a_transaction_failure() {
        let server = MockEdms::start(json!({"type": "transactions", "transactions": []})).await;
        let connection = server.connect().await;
        server.send(dev_api("job-1", "nope", 7));
        let error = connection.wait_for("job-1").await.unwrap_err();
        match error {
            WaitError::Transaction(failure) => {
                assert_eq!(failure.transaction_id, "job-1");
                assert_eq!(failure.result.unwrap().status.code, 7);
            }
            other => panic!("expected transaction failure, got {other:?}"),
        }
        assert_eq!(server.hits(), 0);
    }

    #[tokio::test]
    async fn rejects_invalid_timeout_and_closed_port() {
        let error = connect(&EdmsConfig {
            base_url: "http://edms.example".to_owned(),
            timeout: Duration::ZERO,
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransportError::InvalidTimeout));
        let error = connect(&EdmsConfig {
            base_url: "not a url".to_owned(),
            timeout: Duration::from_millis(200),
        })
        .await
        .unwrap_err();
        assert!(matches!(error, TransportError::InvalidUrl { .. }));
        let error = connect(&EdmsConfig {
            base_url: "http://127.0.0.1:1".to_owned(),
            timeout: Duration::from_millis(200),
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            TransportError::Connection(_) | TransportError::ConnectTimeout
        ));
    }

    #[derive(Clone)]
    struct MockStateWithGate {
        snapshot: String,
        outbound: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<ServerCommand>>>>,
        hits: Arc<AtomicUsize>,
        jobs: Arc<Mutex<HashMap<String, (u16, Value)>>>,
        gate: Arc<tokio::sync::Mutex<tokio::sync::watch::Receiver<bool>>>,
    }

    async fn ws_gate(
        upgrade: WebSocketUpgrade,
        State(state): State<MockStateWithGate>,
    ) -> impl IntoResponse {
        upgrade.on_upgrade(move |socket| handle_socket_gate(socket, state))
    }

    async fn handle_socket_gate(mut socket: WebSocket, state: MockStateWithGate) {
        if socket
            .send(Message::Text(state.snapshot.clone().into()))
            .await
            .is_err()
        {
            return;
        }
        let mut outbound = state.outbound.lock().await;
        let Some(mut outbound) = outbound.take() else {
            return;
        };
        while let Some(command) = outbound.recv().await {
            match command {
                ServerCommand::Send(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                ServerCommand::Close => {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
            }
        }
    }

    async fn job_gate(Path(id): Path<String>, State(state): State<MockStateWithGate>) -> Response {
        state.hits.fetch_add(1, Ordering::SeqCst);
        {
            let mut rx = state.gate.lock().await;
            let _ = rx.wait_for(|&v| v).await;
        }
        let jobs = state.jobs.lock().unwrap();
        match jobs.get(&id) {
            Some((status, body)) => (
                axum::http::StatusCode::from_u16(*status).unwrap(),
                Json(body.clone()),
            )
                .into_response(),
            None => axum::http::StatusCode::NOT_FOUND.into_response(),
        }
    }
}

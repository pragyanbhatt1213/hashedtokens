use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::{
    Json, Router,
    extract::{Path, State as AxumState},
    response::IntoResponse,
    routing::{get, post},
};
use hashedtokens::{
    ClientConfig, EdmsClientConfig, EdmsError, EdmsOutcome, HashedTokens, HttpMethod, RequestError,
    RequestOptions,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::*};

enum EdmsCmd {
    Send(String),
    Close,
}

struct MockEdms {
    base_url: String,
    ws_cmd: mpsc::UnboundedSender<EdmsCmd>,
    insert_hits: Arc<AtomicUsize>,
    jobs: Arc<std::sync::Mutex<HashMap<String, (u16, Value)>>>,
    last_insert_body: Arc<std::sync::Mutex<Option<Value>>>,
}

#[derive(Clone)]
struct EdmsState {
    ws_cmd: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<EdmsCmd>>>>,
    insert_hits: Arc<AtomicUsize>,
    jobs: Arc<std::sync::Mutex<HashMap<String, (u16, Value)>>>,
    insert_response: Arc<std::sync::Mutex<Value>>,
    last_insert_body: Arc<std::sync::Mutex<Option<Value>>>,
}

impl MockEdms {
    async fn start() -> Self {
        Self::start_with_insert_response(json!({
            "ok": true,
            "transaction_id": "tx-test-1",
            "ops": "insert"
        }))
        .await
    }

    async fn start_with_insert_response(insert_resp: Value) -> Self {
        let (ws_cmd_tx, ws_cmd_rx) = mpsc::unbounded_channel();
        let insert_hits = Arc::new(AtomicUsize::new(0));
        let jobs = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let last_insert_body = Arc::new(std::sync::Mutex::new(None));

        let state = EdmsState {
            ws_cmd: Arc::new(tokio::sync::Mutex::new(Some(ws_cmd_rx))),
            insert_hits: Arc::clone(&insert_hits),
            jobs: Arc::clone(&jobs),
            insert_response: Arc::new(std::sync::Mutex::new(insert_resp)),
            last_insert_body: Arc::clone(&last_insert_body),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/hashedtokens/edms/api/ws", get(edms_ws_handler))
            .route(
                "/hashedtokens/edms/api/collections/",
                post(edms_insert_handler),
            )
            .route("/jobs/{id}", get(edms_job_handler))
            .with_state(state);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{addr}"),
            ws_cmd: ws_cmd_tx,
            insert_hits,
            jobs,
            last_insert_body,
        }
    }

    fn client(&self, timeout: Duration) -> HashedTokens {
        HashedTokens::with_config(
            ClientConfig::default().edms(EdmsClientConfig::new(&self.base_url).timeout(timeout)),
        )
        .expect("client should build")
    }

    fn send_event(&self, msg: impl Into<String>) {
        self.ws_cmd.send(EdmsCmd::Send(msg.into())).unwrap();
    }

    fn close_ws(&self) {
        self.ws_cmd.send(EdmsCmd::Close).unwrap();
    }

    fn insert_hits(&self) -> usize {
        self.insert_hits.load(Ordering::SeqCst)
    }

    fn insert_job(&self, id: &str, status: u16, body: Value) {
        self.jobs
            .lock()
            .unwrap()
            .insert(id.to_owned(), (status, body));
    }

    fn last_insert_body(&self) -> Option<Value> {
        self.last_insert_body.lock().unwrap().clone()
    }
}

async fn edms_ws_handler(
    upgrade: WebSocketUpgrade,
    AxumState(state): AxumState<EdmsState>,
) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| edms_ws_serve(socket, state))
}

async fn edms_ws_serve(mut socket: WebSocket, state: EdmsState) {
    if socket
        .send(Message::Text(
            json!({"type":"transactions","transactions":[]})
                .to_string()
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    let mut rx_guard = state.ws_cmd.lock().await;
    let Some(mut rx) = rx_guard.take() else {
        return;
    };
    drop(rx_guard);
    while let Some(cmd) = rx.recv().await {
        match cmd {
            EdmsCmd::Send(text) => {
                if socket.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            EdmsCmd::Close => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
        }
    }
}

async fn edms_insert_handler(
    AxumState(state): AxumState<EdmsState>,
    body: Json<Value>,
) -> impl IntoResponse {
    state.insert_hits.fetch_add(1, Ordering::SeqCst);
    *state.last_insert_body.lock().unwrap() = Some(body.0);
    let resp = state.insert_response.lock().unwrap().clone();
    (axum::http::StatusCode::ACCEPTED, Json(resp))
}

async fn edms_job_handler(
    Path(id): Path<String>,
    AxumState(state): AxumState<EdmsState>,
) -> impl IntoResponse {
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

fn dev_api_event(transaction_id: &str, detail: &str, code: i64) -> String {
    json!({
        "type": "event",
        "event": {
            "type": "DevApiResult",
            "payload": {
                "transaction_id": transaction_id,
                "op": "api_collections_insert",
                "result": {
                    "status": {
                        "code": code,
                        "name": if code == 0 { "OK" } else { "ERR" },
                        "message": "processed"
                    },
                    "detail": detail
                }
            }
        }
    })
    .to_string()
}

fn done_job(id: &str) -> Value {
    json!({
        "id": id,
        "op": "api_collections_insert",
        "status": "done",
        "started_at_ms": 1,
        "finished_at_ms": 2,
        "result": {
            "status": {"code": 0, "name": "OK", "message": "ok"},
            "detail": "recovered"
        },
        "error": null,
        "progress": null,
        "age_ms": 1
    })
}

#[tokio::test]
async fn target_200_edms_insert_and_ws_event_succeeds() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 1})))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/items", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Post,
                RequestOptions::default()
                    .json(json!({"name": "widget"}))
                    .edms_collection("items"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "stored", 0));

    let outcome = handle.await.unwrap().expect("request should succeed");
    assert_eq!(outcome.response.status_code(), 200);
    assert!(matches!(outcome.edms, EdmsOutcome::Stored(ref s) if s.transaction_id == "tx-test-1"));
}

#[tokio::test]
async fn target_404_edms_insert_succeeds() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "not found"})))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/missing", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Get,
                RequestOptions::default().edms_collection("responses"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "stored", 0));

    let outcome = handle.await.unwrap().expect("lifecycle should succeed");
    assert_eq!(outcome.response.status_code(), 404);
    assert!(outcome.response.is_client_error());
    assert!(matches!(outcome.edms, EdmsOutcome::Stored(_)));
}

#[tokio::test]
async fn target_500_edms_insert_succeeds_both_preserved() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/broken"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/broken", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Get,
                RequestOptions::default().edms_collection("errors"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "stored", 0));

    let outcome = handle.await.unwrap().expect("lifecycle should succeed");
    assert_eq!(outcome.response.status_code(), 500);
    assert!(outcome.response.is_server_error());
    assert!(matches!(outcome.edms, EdmsOutcome::Stored(_)));
}

#[tokio::test]
async fn target_transport_failure_returns_err() {
    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(1));

    let result = client
        .request(
            "http://127.0.0.1:1",
            HttpMethod::Get,
            RequestOptions::default()
                .timeout(Duration::from_millis(300))
                .edms_collection("whatever"),
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        RequestError::Connection(_) | RequestError::RequestTimeout(_)
    ));
    assert_eq!(edms.insert_hits(), 0);
}

#[tokio::test]
async fn edms_ws_connection_failure_returns_err() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&target)
        .await;

    let client = HashedTokens::with_config(
        ClientConfig::default()
            .edms(EdmsClientConfig::new("http://127.0.0.1:1").timeout(Duration::from_millis(300))),
    )
    .expect("client should build");

    let result = client
        .request(
            &format!("{}/data", target.uri()),
            HttpMethod::Get,
            RequestOptions::default().edms_collection("data"),
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        RequestError::EdmsConnection(_)
    ));
}

#[tokio::test]
async fn edms_submission_failure_returns_edms_failed_outcome() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&target)
        .await;

    let _edms = MockEdms::start_with_insert_response(json!({"error": "db down"})).await;
    let edms_bad =
        MockEdms::start_with_insert_response(json!({"ok": false, "ops": "insert"})).await;

    let client = edms_bad.client(Duration::from_secs(2));
    let url = format!("{}/data", target.uri());
    let outcome = client
        .request(
            &url,
            HttpMethod::Get,
            RequestOptions::default().edms_collection("data"),
        )
        .await
        .expect("target succeeded; overall should not error");

    assert_eq!(outcome.response.status_code(), 200);
    assert!(matches!(
        outcome.edms,
        EdmsOutcome::Failed(EdmsError::Submission(_))
    ));
}

#[tokio::test]
async fn edms_invalid_acceptance_response_gives_failed_outcome() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&target)
        .await;

    let edms = MockEdms::start_with_insert_response(json!({"ok": true, "ops": "insert"})).await;
    let client = edms.client(Duration::from_secs(2));
    let url = format!("{}/data", target.uri());

    let outcome = client
        .request(
            &url,
            HttpMethod::Get,
            RequestOptions::default().edms_collection("data"),
        )
        .await
        .expect("target succeeded");

    assert_eq!(outcome.response.status_code(), 200);
    assert!(matches!(
        outcome.edms,
        EdmsOutcome::Failed(EdmsError::Submission(_))
    ));
}

#[tokio::test]
async fn edms_transaction_failure_gives_failed_outcome() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"id": 2})))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/items", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Post,
                RequestOptions::default()
                    .json(json!({"name": "x"}))
                    .edms_collection("items"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "rejected", 7));

    let outcome = handle.await.unwrap().expect("target succeeded");
    assert_eq!(outcome.response.status_code(), 201);
    assert!(matches!(
        outcome.edms,
        EdmsOutcome::Failed(EdmsError::Transaction(_))
    ));
}

#[tokio::test]
async fn unrelated_ws_event_ignored_matching_event_wins() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"v": 1})))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/data", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Get,
                RequestOptions::default().edms_collection("data"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-other-999", "other", 0));
    edms.send_event(dev_api_event("tx-test-1", "correct", 0));

    let outcome = handle.await.unwrap().expect("should succeed");
    assert!(matches!(
        outcome.edms,
        EdmsOutcome::Stored(ref s) if s.transaction_id == "tx-test-1"
    ));
}

#[tokio::test]
async fn ws_disconnect_then_recovery_succeeds() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    edms.insert_job("tx-test-1", 200, done_job("tx-test-1"));
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/data", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Get,
                RequestOptions::default().edms_collection("data"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.close_ws();

    let outcome = handle.await.unwrap().expect("recovery should succeed");
    assert_eq!(outcome.response.status_code(), 200);
    assert!(matches!(outcome.edms, EdmsOutcome::Stored(_)));
}

#[tokio::test]
async fn ws_disconnect_then_recovery_failure_gives_failed_outcome() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/data", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Get,
                RequestOptions::default().edms_collection("data"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.close_ws();

    let outcome = handle.await.unwrap().expect("target succeeded");
    assert_eq!(outcome.response.status_code(), 200);
    assert!(matches!(
        outcome.edms,
        EdmsOutcome::Failed(EdmsError::Transaction(_))
    ));
}

#[tokio::test]
async fn target_api_executed_exactly_once() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"order_id": 42})))
        .expect(1)
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/order", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Post,
                RequestOptions::default()
                    .json(json!({"item": "book"}))
                    .edms_collection("orders"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "ok", 0));

    handle.await.unwrap().expect("should succeed");
}

#[tokio::test]
async fn captured_request_and_response_match_actual_exchange() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/echo"))
        .and(body_json(json!({"key": "value"})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"echoed": true}))
                .insert_header("x-trace", "abc"),
        )
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/echo", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Post,
                RequestOptions::default()
                    .json(json!({"key": "value"}))
                    .edms_collection("echo"),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "ok", 0));

    let outcome = handle.await.unwrap().expect("should succeed");
    assert_eq!(outcome.response.json(), Some(&json!({"echoed": true})));
    assert_eq!(outcome.response.headers["x-trace"], "abc");

    let insert_body = edms.last_insert_body().expect("insert body was recorded");
    assert_eq!(insert_body["ops"], "insert");
    assert_eq!(insert_body["request_body"], json!({"key": "value"}));
    assert_eq!(insert_body["response_body"], json!({"echoed": true}));
    assert_eq!(insert_body["collection"], "echo");
    assert_eq!(insert_body["method"], "POST");
}

#[tokio::test]
async fn edms_payload_uses_insert_op_and_established_schema() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schema-check"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": 1})))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let url = format!("{}/schema-check", target.uri());
    let handle = tokio::spawn(async move {
        client
            .request(
                &url,
                HttpMethod::Get,
                RequestOptions::default()
                    .edms_collection("schema_col")
                    .edms_annotation("my-annotation")
                    .edms_tags(vec!["tag1".to_owned()]),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    while edms.insert_hits() == 0 {
        tokio::task::yield_now().await;
    }
    edms.send_event(dev_api_event("tx-test-1", "ok", 0));
    handle.await.unwrap().expect("should succeed");

    let body = edms.last_insert_body().unwrap();
    assert_eq!(body["ops"], "insert");
    assert_eq!(body["collection"], "schema_col");
    assert_eq!(body["method"], "GET");
    assert!(
        body["endpoint_str"]
            .as_str()
            .unwrap()
            .contains("/schema-check")
    );
    assert_eq!(body["annotation"], "my-annotation");
    assert_eq!(body["tags"], json!(["tag1"]));
    assert!(
        !body
            .as_object()
            .unwrap()
            .contains_key("collection_annotation")
    );
    assert!(!body.as_object().unwrap().contains_key("collection_tags"));
}

#[tokio::test]
async fn without_edms_config_outcome_is_skipped() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/plain"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"plain": true})))
        .mount(&target)
        .await;

    let client = HashedTokens::new().expect("client should build");
    let outcome = client
        .request(
            &format!("{}/plain", target.uri()),
            HttpMethod::Get,
            RequestOptions::default(),
        )
        .await
        .expect("should succeed");

    assert_eq!(outcome.response.status_code(), 200);
    assert!(matches!(outcome.edms, EdmsOutcome::Skipped));
}

#[tokio::test]
async fn edms_configured_without_collection_returns_err() {
    let target = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&target)
        .await;

    let edms = MockEdms::start().await;
    let client = edms.client(Duration::from_secs(2));

    let result = client
        .request(
            &format!("{}/data", target.uri()),
            HttpMethod::Get,
            RequestOptions::default(),
        )
        .await;

    assert!(matches!(result, Err(RequestError::EdmsCollectionRequired)));
}

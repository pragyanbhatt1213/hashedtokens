use super::payload::InsertPayload;
use reqwest::{Client, StatusCode, Url, header::CONTENT_TYPE};
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone)]
pub(crate) struct EdmsConfig {
    pub(crate) base_url: String,
    pub(crate) timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct SubmissionResponse {
    ok: bool,
    transaction_id: Option<String>,
    ops: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SubmissionResult {
    pub(crate) transaction_id: String,
}

#[derive(Debug, Error)]
pub(crate) enum SubmissionError {
    #[error("invalid EDMS URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("EDMS timeout must be greater than zero")]
    InvalidTimeout,
    #[error("EDMS connection failed: {0}")]
    Connection(#[source] reqwest::Error),
    #[error("EDMS request timed out: {0}")]
    Timeout(#[source] reqwest::Error),
    #[error("EDMS request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("EDMS returned HTTP {status}: {body}")]
    HttpStatus { status: StatusCode, body: String },
    #[error("invalid EDMS JSON response: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("EDMS response did not contain a valid transaction ID")]
    TransactionId,
    #[error("unexpected EDMS operation: {0}")]
    Operation(String),
    #[error("EDMS response was not successful")]
    NotSuccessful,
}

pub(crate) async fn submit(
    config: &EdmsConfig,
    payload: &InsertPayload,
) -> Result<SubmissionResult, SubmissionError> {
    if config.timeout.is_zero() {
        return Err(SubmissionError::InvalidTimeout);
    }
    let url = endpoint(&config.base_url)?;
    let client = Client::builder()
        .timeout(config.timeout)
        .build()
        .map_err(SubmissionError::Request)?;
    let response = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .json(payload)
        .send()
        .await
        .map_err(map_error)?;
    let status = response.status();
    let body = response.bytes().await.map_err(map_error)?;
    if status != StatusCode::ACCEPTED {
        return Err(SubmissionError::HttpStatus {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    let response: SubmissionResponse =
        serde_json::from_slice(&body).map_err(SubmissionError::InvalidJson)?;
    if response.ops.as_deref() != Some("insert") {
        return Err(SubmissionError::Operation(
            response.ops.unwrap_or_else(|| "missing".to_owned()),
        ));
    }
    if !response.ok {
        return Err(SubmissionError::NotSuccessful);
    }
    let transaction_id = response
        .transaction_id
        .filter(|value| !value.trim().is_empty())
        .ok_or(SubmissionError::TransactionId)?;
    Ok(SubmissionResult { transaction_id })
}

fn endpoint(base_url: &str) -> Result<Url, SubmissionError> {
    let base = Url::parse(base_url).map_err(|error| SubmissionError::InvalidUrl {
        url: base_url.to_owned(),
        reason: error.to_string(),
    })?;
    let base = if base.as_str().ends_with('/') {
        base
    } else {
        let with_slash = format!("{}/", base);
        Url::parse(&with_slash).map_err(|error| SubmissionError::InvalidUrl {
            url: base_url.to_owned(),
            reason: error.to_string(),
        })?
    };
    base.join("hashedtokens/edms/api/collections/")
        .map_err(|error| SubmissionError::InvalidUrl {
            url: base_url.to_owned(),
            reason: error.to_string(),
        })
}

fn map_error(error: reqwest::Error) -> SubmissionError {
    if error.is_timeout() {
        SubmissionError::Timeout(error)
    } else if error.is_connect() {
        SubmissionError::Connection(error)
    } else {
        SubmissionError::Request(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edms::payload::{CollectionConfig, insert_payload};
    use crate::response::ResponseBody;
    use crate::types::{CapturedExchange, CapturedRequest};
    use reqwest::{Method, StatusCode, header::HeaderMap};
    use serde_json::json;
    use std::time::Duration;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, header, method, path},
    };

    fn payload() -> InsertPayload {
        insert_payload(
            CapturedExchange {
                request: CapturedRequest {
                    url: Url::parse("https://target.example/items").unwrap(),
                    method: Method::POST,
                    headers: HeaderMap::new(),
                    body: Some(br#"{"id":1}"#.to_vec()),
                },
                response: crate::response::ApiResponse {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    body: ResponseBody::Json(json!({"ok": true})),
                },
            },
            CollectionConfig {
                name: "items".to_owned(),
                ..CollectionConfig::default()
            },
        )
        .unwrap()
    }

    fn config(server: &MockServer) -> EdmsConfig {
        EdmsConfig {
            base_url: server.uri(),
            timeout: Duration::from_secs(2),
        }
    }

    #[tokio::test]
    async fn submits_insert_payload_and_returns_transaction_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hashedtokens/edms/api/collections/"))
            .and(header("content-type", "application/json"))
            .and(body_json(json!({
                "ops": "insert",
                "endpoint_str": "https://target.example/items",
                "method": "POST",
                "collection": "items",
                "request_body": {"id": 1},
                "response_body": {"ok": true},
                "request_headers": {},
                "response_headers": {}
            })))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "ok": true,
                "transaction_id": "tx-1",
                "ops": "insert"
            })))
            .mount(&server)
            .await;

        let result = submit(&config(&server), &payload()).await.unwrap();
        assert_eq!(result.transaction_id, "tx-1");
    }

    #[tokio::test]
    async fn rejects_invalid_success_responses() {
        for body in [
            json!({"ok": true, "ops": "insert"}),
            json!({"ok": true, "transaction_id": "", "ops": "insert"}),
            json!({"ok": true, "transaction_id": "tx", "ops": "test"}),
            json!({"ok": false, "transaction_id": "tx", "ops": "insert"}),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(202).set_body_json(body))
                .mount(&server)
                .await;
            let error = submit(&config(&server), &payload()).await.unwrap_err();
            assert!(matches!(
                error,
                SubmissionError::TransactionId
                    | SubmissionError::Operation(_)
                    | SubmissionError::NotSuccessful
            ));
        }
    }

    #[tokio::test]
    async fn rejects_malformed_and_non_202_responses() {
        for status in [StatusCode::BAD_REQUEST, StatusCode::SERVICE_UNAVAILABLE] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(status.as_u16()).set_body_string("rejected"))
                .mount(&server)
                .await;
            let error = submit(&config(&server), &payload()).await.unwrap_err();
            assert!(matches!(error, SubmissionError::HttpStatus { .. }));
        }

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(202).set_body_string("not-json"))
            .mount(&server)
            .await;
        let error = submit(&config(&server), &payload()).await.unwrap_err();
        assert!(matches!(error, SubmissionError::InvalidJson(_)));
    }

    #[tokio::test]
    async fn handles_timeout_and_connection_failure_without_retry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_delay(Duration::from_millis(500))
                    .set_body_json(json!({
                        "ok": true,
                        "transaction_id": "tx",
                        "ops": "insert"
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let error = submit(
            &EdmsConfig {
                base_url: server.uri(),
                timeout: Duration::from_millis(50),
            },
            &payload(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SubmissionError::Timeout(_)));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);

        let error = submit(
            &EdmsConfig {
                base_url: "http://127.0.0.1:1".to_owned(),
                timeout: Duration::from_secs(1),
            },
            &payload(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SubmissionError::Connection(_)
                | SubmissionError::Request(_)
                | SubmissionError::Timeout(_)
        ));
    }

    #[tokio::test]
    async fn validates_configuration() {
        let server = MockServer::start().await;
        let error = submit(
            &EdmsConfig {
                base_url: "not a url".to_owned(),
                timeout: Duration::from_secs(1),
            },
            &payload(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SubmissionError::InvalidUrl { .. }));
        let error = submit(
            &EdmsConfig {
                base_url: server.uri(),
                timeout: Duration::ZERO,
            },
            &payload(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SubmissionError::InvalidTimeout));
    }
}

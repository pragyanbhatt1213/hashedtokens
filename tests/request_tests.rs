use hashedtokens::{HashedTokens, HttpMethod, RequestError, RequestOptions};
use serde_json::json;
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path, query_param},
};

async fn client() -> HashedTokens {
    HashedTokens::new().expect("client should build")
}

#[tokio::test]
async fn supports_methods_payloads_queries_and_headers() {
    let server = MockServer::start().await;
    for (http_method, request_method) in [
        ("GET", HttpMethod::Get),
        ("POST", HttpMethod::Post),
        ("PUT", HttpMethod::Put),
        ("PATCH", HttpMethod::Patch),
        ("DELETE", HttpMethod::Delete),
    ] {
        Mock::given(method(http_method))
            .and(path("/resource"))
            .and(query_param("q", "hello world"))
            .and(header("x-test", "yes"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let options = RequestOptions::default()
            .query("q", "hello world")
            .header("x-test", "yes");
        let options = if matches!(
            request_method,
            HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch
        ) {
            options.json(json!({"value": 1}))
        } else {
            options
        };
        let outcome = client()
            .await
            .request(
                &format!("{}/resource", server.uri()),
                request_method,
                options,
            )
            .await
            .expect("request should succeed");
        assert!(outcome.response.is_success());
    }

    Mock::given(method("POST"))
        .and(path("/json"))
        .and(body_json(json!({"name": "example"})))
        .respond_with(ResponseTemplate::new(201))
        .mount(&server)
        .await;
    client()
        .await
        .request(
            &format!("{}/json", server.uri()),
            HttpMethod::Post,
            RequestOptions::default().json(json!({"name": "example"})),
        )
        .await
        .expect("JSON request should succeed");
}

#[tokio::test]
async fn validates_per_request_timeout_and_headers() {
    let client = client().await;
    let error = client
        .request(
            "http://127.0.0.1:1",
            HttpMethod::Get,
            RequestOptions::default().timeout(Duration::ZERO),
        )
        .await
        .expect_err("zero timeout should fail");
    assert!(matches!(error, RequestError::InvalidTimeout));
}

#[tokio::test]
async fn validates_urls_and_headers() {
    let client = client().await;
    let error = client
        .request("not a url", HttpMethod::Get, RequestOptions::default())
        .await
        .expect_err("invalid URL should fail");
    assert!(matches!(error, RequestError::InvalidUrl { .. }));
    let error = client
        .request(
            "http://127.0.0.1:1",
            HttpMethod::Get,
            RequestOptions::default().header("Invalid Header", "value"),
        )
        .await
        .expect_err("invalid header should fail");
    assert!(matches!(error, RequestError::InvalidHeaderName { .. }));
    let error = client
        .request(
            "http://127.0.0.1:1",
            HttpMethod::Get,
            RequestOptions::default().header("x-test", "bad\nvalue"),
        )
        .await
        .expect_err("invalid header value should fail");
    assert!(matches!(error, RequestError::InvalidHeaderValue { .. }));
}

#[tokio::test]
async fn reports_connection_failures() {
    let error = client()
        .await
        .request(
            "http://127.0.0.1:1",
            HttpMethod::Get,
            RequestOptions::default().timeout(Duration::from_secs(1)),
        )
        .await
        .expect_err("closed local port should fail");
    assert!(matches!(
        error,
        RequestError::Connection(_) | RequestError::RequestTimeout(_)
    ));
}

use hashedtokens::{HashedTokens, HttpMethod, RequestOptions, ResponseBody};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn get(server: &MockServer, route: &str) -> hashedtokens::ApiResponse {
    HashedTokens::new()
        .expect("client should build")
        .request(
            &format!("{}{}", server.uri(), route),
            HttpMethod::Get,
            RequestOptions::default(),
        )
        .await
        .expect("request should succeed")
        .response
}

#[tokio::test]
async fn classifies_json_media_types_and_preserves_headers() {
    let server = MockServer::start().await;
    for (route, content_type) in [
        ("/problem", "application/problem+json"),
        ("/vendor", "application/vnd.example+json"),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(
                ResponseTemplate::new(400)
                    .insert_header("content-type", content_type)
                    .insert_header("x-response", "present")
                    .set_body_json(json!({"error": "bad"})),
            )
            .mount(&server)
            .await;
        let response = get(&server, route).await;
        assert_eq!(response.json(), Some(&json!({"error": "bad"})));
        assert!(response.is_client_error());
        assert_eq!(response.headers["x-response"], "present");
    }
}

#[tokio::test]
async fn handles_text_empty_binary_missing_type_and_status_helpers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/text"))
        .respond_with(
            ResponseTemplate::new(500)
                .insert_header("content-type", "text/plain")
                .set_body_string("failure"),
        )
        .mount(&server)
        .await;
    let response = get(&server, "/text").await;
    assert_eq!(response.body, ResponseBody::Text("failure".into()));
    assert!(response.is_server_error());

    Mock::given(method("GET"))
        .and(path("/empty"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    assert_eq!(get(&server, "/empty").await.body, ResponseBody::Empty);

    Mock::given(method("GET"))
        .and(path("/json"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(br#"{"ok":true}"#.to_vec()))
        .mount(&server)
        .await;
    assert_eq!(
        get(&server, "/json").await.json(),
        Some(&json!({"ok": true}))
    );

    Mock::given(method("GET"))
        .and(path("/bad"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(vec![0xff, 0xfe]),
        )
        .mount(&server)
        .await;
    assert!(matches!(
        get(&server, "/bad").await.body,
        ResponseBody::Binary(_)
    ));
}

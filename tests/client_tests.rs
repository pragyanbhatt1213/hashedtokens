use hashedtokens::{ClientConfig, HashedTokens, HttpMethod, RequestOptions};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

#[tokio::test]
async fn applies_default_headers_and_reuses_client() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/one"))
        .and(header("x-default", "configured"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/two"))
        .and(header("x-default", "configured"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let config = ClientConfig::default()
        .default_header("x-default", "configured")
        .expect("header should be valid");
    let client = HashedTokens::with_config(config).expect("client should build");
    for route in ["/one", "/two"] {
        let outcome = client
            .request(
                &format!("{}{}", server.uri(), route),
                HttpMethod::Get,
                RequestOptions::default(),
            )
            .await
            .expect("request should succeed");
        assert!(outcome.response.is_success());
    }
}

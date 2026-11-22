use hashedtokens::{
    ClientConfig, EdmsClientConfig, EdmsOutcome, HashedTokens, HttpMethod, RequestOptions,
};
use serde_json::json;
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
#[ignore = "Requires running local EDMS instance"]
async fn live_edms_insert_and_ws_confirmation() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/orders"))
        .respond_with(
            ResponseTemplate::new(201).set_body_json(json!({"order_id": 42, "status": "created"})),
        )
        .expect(1)
        .mount(&target)
        .await;

    let edms_url =
        std::env::var("EDMS_BASE_URL").unwrap_or_else(|_| "http://localhost:3000".to_owned());

    let client = HashedTokens::with_config(
        ClientConfig::default()
            .edms(EdmsClientConfig::new(&edms_url).timeout(Duration::from_secs(15))),
    )
    .expect("client should build");

    let outcome = client
        .request(
            &format!("{}/orders", target.uri()),
            HttpMethod::Post,
            RequestOptions::default()
                .json(json!({"item": "book", "quantity": 1}))
                .edms_collection("rust_wrapper_live_test"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(outcome.response.status_code(), 201);

    match &outcome.edms {
        EdmsOutcome::Stored(info) => {
            assert!(!info.transaction_id.is_empty());
            println!("transaction_id: {}", info.transaction_id);
            println!("op:             {:?}", info.op);
            println!("status_code:    {}", info.status_code);
        }
        EdmsOutcome::Failed(err) => panic!("EDMS storage failed: {err}"),
        EdmsOutcome::Skipped => panic!("EDMS outcome was Skipped"),
    }
}

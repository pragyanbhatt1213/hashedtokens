# HashedTokens

A Rust HTTP client library that executes target API requests, captures the full exchange, and optionally stores it in an EDMS (External Data Management System) backend with WebSocket-based transaction tracking.

## Features

- **HTTP request execution** — GET, POST, PUT, PATCH, and DELETE via a typed `HttpMethod` enum.
- **Full exchange capture** — request URL, method, headers, and body are recorded alongside the response status, headers, and body.
- **Optional EDMS integration** — when configured, every request's exchange is serialized and submitted to an EDMS collection endpoint.
- **WebSocket transaction tracking** — the client opens a WebSocket connection before each EDMS-backed request and waits for a confirmed transaction result, with automatic recovery over HTTP if the socket disconnects mid-wait.
- **All HTTP status codes preserved** — 4xx and 5xx responses from the target are not errors; they are captured and returned alongside the EDMS outcome.

## Installation

HashedTokens is not published to crates.io. Add it as a Git dependency:

```toml
[dependencies]
hashedtokens = { git = "https://github.com/your-org/hashedtokens" }
```

## Quick Start

Create a client and make a request without EDMS:

```rust
use hashedtokens::{HashedTokens, HttpMethod, RequestOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HashedTokens::new()?;

    let outcome = client
        .request(
            "https://api.example.com/items",
            HttpMethod::Post,
            RequestOptions::default()
                .json(json!({"name": "widget"}))
                .header("x-api-key", "secret"),
        )
        .await?;

    println!("Status: {}", outcome.response.status_code());
    if let Some(body) = outcome.response.json() {
        println!("Body: {body}");
    }

    Ok(())
}
```

## EDMS Configuration

When EDMS is configured, every call to `request()` captures the exchange and submits it to the EDMS backend. The WebSocket connection is opened before the target request and is used to confirm the transaction.

```rust
use hashedtokens::{
    ClientConfig, EdmsClientConfig, EdmsOutcome, HashedTokens, HttpMethod, RequestOptions,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let edms = EdmsClientConfig::new("http://edms.example")
        .timeout(Duration::from_secs(10));

    let client = HashedTokens::with_config(
        ClientConfig::default().edms(edms),
    )?;

    let outcome = client
        .request(
            "https://api.example.com/orders",
            HttpMethod::Post,
            RequestOptions::default()
                .json(serde_json::json!({"item": "book"}))
                .edms_collection("orders")
                .edms_annotation("checkout-flow")
                .edms_tags(vec!["production".to_owned()]),
        )
        .await?;

    println!("Target status: {}", outcome.response.status_code());

    match outcome.edms {
        EdmsOutcome::Stored(success) => {
            println!("Stored. Transaction: {}", success.transaction_id);
        }
        EdmsOutcome::Failed(error) => {
            eprintln!("EDMS storage failed: {error}");
        }
        EdmsOutcome::Skipped => {}
    }

    Ok(())
}
```

`RequestOptions` EDMS fields:

| Method | Purpose |
|--------|---------|
| `.edms_collection("name")` | Required — the EDMS collection to insert into |
| `.edms_annotation("note")` | Optional per-endpoint annotation |
| `.edms_collection_annotation("note")` | Optional per-collection annotation |
| `.edms_tags(vec![...])` | Optional endpoint-level tags |
| `.edms_collection_tags(vec![...])` | Optional collection-level tags |

## Error Handling

`request()` returns `Result<RequestOutcome, RequestError>`.

**`Err(RequestError)`** means the call failed before a response was received:
- Target transport errors (`Connection`, `RequestTimeout`, `Http`).
- Invalid request parameters (`InvalidUrl`, `InvalidHeaderName`, `InvalidHeaderValue`, `InvalidTimeout`).
- EDMS WebSocket connection failure before the target request (`EdmsConnection`).
- Missing collection name when EDMS is configured (`EdmsCollectionRequired`).

**`Ok(RequestOutcome)`** is returned even on 4xx/5xx target responses. Inspect `outcome.response` for the target result and `outcome.edms` for the storage result:

- `EdmsOutcome::Skipped` — EDMS is not configured.
- `EdmsOutcome::Stored(EdmsSuccess)` — the exchange was stored and the transaction confirmed.
- `EdmsOutcome::Failed(EdmsError)` — the target call succeeded but EDMS storage failed. The target `ApiResponse` is still accessible on `outcome.response`. `EdmsError` has four variants: `Connection`, `Serialization`, `Submission`, and `Transaction`.

## Testing

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

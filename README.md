# HashedTokens

HashedTokens is a Rust async HTTP client library that executes a target API request exactly once, captures the complete exchange (URL, method, headers, body, response), and asynchronously records it in an EDMS backend via `ops: "insert"`. Confirmation arrives over WebSocket (`DevApiResult` event), with an automatic `/jobs/:id` HTTP fallback if the socket disconnects.

## Add to your project

```toml
[dependencies]
hashedtokens = { git = "https://github.com/pragyanbhatt1213/hashedtokens" }
```

## Usage without EDMS

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

    println!("{}", outcome.response.status_code());
    Ok(())
}
```

## Usage with EDMS

```rust
use hashedtokens::{
    ClientConfig, EdmsClientConfig, EdmsOutcome, HashedTokens, HttpMethod, RequestOptions,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HashedTokens::with_config(
        ClientConfig::default().edms(
            EdmsClientConfig::new("http://edms.example").timeout(Duration::from_secs(10)),
        ),
    )?;

    let outcome = client
        .request(
            "https://api.example.com/orders",
            HttpMethod::Post,
            RequestOptions::default()
                .json(serde_json::json!({"item": "book"}))
                .edms_collection("orders")
                .edms_annotation("checkout-flow"),
        )
        .await?;

    println!("target: {}", outcome.response.status_code());

    match outcome.edms {
        EdmsOutcome::Stored(s) => println!("stored, tx: {}", s.transaction_id),
        EdmsOutcome::Failed(e) => eprintln!("EDMS failed: {e}"),
        EdmsOutcome::Skipped => {}
    }

    Ok(())
}
```

`RequestOptions` EDMS fields:

| Method | Purpose |
|---|---|
| `.edms_collection("name")` | Required — the EDMS collection to insert into |
| `.edms_annotation("note")` | Optional per-endpoint annotation |
| `.edms_collection_annotation("note")` | Optional per-collection annotation |
| `.edms_tags(vec![…])` | Optional endpoint-level tags |
| `.edms_collection_tags(vec![…])` | Optional collection-level tags |

## Error model

`request()` returns `Result<RequestOutcome, RequestError>`.

`Err` only when no response exists: transport error, bad URL, failed WebSocket connect, or missing collection name.

`Ok(RequestOutcome)` for every completed HTTP exchange, including 4xx and 5xx. Inspect `outcome.edms` for the storage result:

- `EdmsOutcome::Skipped` — EDMS not configured.
- `EdmsOutcome::Stored(EdmsSuccess)` — exchange stored and transaction confirmed.
- `EdmsOutcome::Failed(EdmsError)` — target responded but EDMS failed. `EdmsError` has four variants: `Connection`, `Serialization`, `Submission`, `Transaction`.

## Running tests

```sh
# All mock-based tests (no external services required)
cargo test

# Live integration test against a running EDMS backend
EDMS_BASE_URL=http://localhost:3000 cargo test --test live_edms_test -- --ignored --nocapture
```

PowerShell equivalent for the live test:

```powershell
$env:EDMS_BASE_URL="http://localhost:3000"
cargo test --test live_edms_test -- --ignored --nocapture
```

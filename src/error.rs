use crate::edms::EdmsError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("invalid URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("invalid header name '{name}': {reason}")]
    InvalidHeaderName { name: String, reason: String },
    #[error("invalid header value for '{name}': {reason}")]
    InvalidHeaderValue { name: String, reason: String },
    #[error("timeout must be greater than zero")]
    InvalidTimeout,
    #[error("request timed out: {0}")]
    RequestTimeout(#[source] reqwest::Error),
    #[error("connection failed: {0}")]
    Connection(#[source] reqwest::Error),
    #[error("request construction failed: {0}")]
    RequestConstruction(#[source] reqwest::Error),
    #[error("response body could not be read: {0}")]
    ResponseBody(#[source] reqwest::Error),
    #[error("HTTP client error: {0}")]
    Http(#[source] reqwest::Error),
    #[error("EDMS collection name is required when EDMS is configured")]
    EdmsCollectionRequired,
    #[error("EDMS WebSocket connection failed: {0}")]
    EdmsConnection(#[source] EdmsError),
}

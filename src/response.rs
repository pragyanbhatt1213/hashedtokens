use crate::edms::EdmsError;
use reqwest::{StatusCode, header::HeaderMap};
use serde::de::DeserializeOwned;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseBody {
    Empty,
    Json(Value),
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: ResponseBody,
}

impl ApiResponse {
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    pub fn status_code(&self) -> u16 {
        self.status.as_u16()
    }

    pub fn is_client_error(&self) -> bool {
        self.status.is_client_error()
    }

    pub fn is_server_error(&self) -> bool {
        self.status.is_server_error()
    }

    pub fn json(&self) -> Option<&Value> {
        match &self.body {
            ResponseBody::Json(value) => Some(value),
            _ => None,
        }
    }

    pub fn json_into<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        match &self.body {
            ResponseBody::Json(value) => serde_json::from_value(value.clone()),
            ResponseBody::Text(text) => serde_json::from_str(text),
            ResponseBody::Binary(bytes) => serde_json::from_slice(bytes),
            ResponseBody::Empty => serde_json::from_value(Value::Null),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EdmsSuccess {
    pub transaction_id: String,
    pub op: Option<String>,
    pub status_code: i64,
}

#[derive(Debug)]
pub enum EdmsOutcome {
    Skipped,
    Stored(EdmsSuccess),
    Failed(EdmsError),
}

#[derive(Debug)]
pub struct RequestOutcome {
    pub response: ApiResponse,
    pub edms: EdmsOutcome,
}

impl RequestOutcome {
    pub(crate) fn plain(response: ApiResponse) -> Self {
        Self {
            response,
            edms: EdmsOutcome::Skipped,
        }
    }

    pub(crate) fn stored(response: ApiResponse, success: EdmsSuccess) -> Self {
        Self {
            response,
            edms: EdmsOutcome::Stored(success),
        }
    }

    pub(crate) fn edms_failed(response: ApiResponse, error: EdmsError) -> Self {
        Self {
            response,
            edms: EdmsOutcome::Failed(error),
        }
    }
}

pub(crate) fn classify_body(bytes: &[u8], content_type: Option<&str>) -> ResponseBody {
    if bytes.is_empty() {
        return ResponseBody::Empty;
    }
    let media_type = content_type
        .and_then(|value| value.split(';').next())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let is_json = media_type == "application/json"
        || media_type == "application/problem+json"
        || media_type.ends_with("+json");
    if is_json {
        if let Ok(value) = serde_json::from_slice(bytes) {
            return ResponseBody::Json(value);
        }
    } else if media_type.starts_with("text/") {
        return String::from_utf8(bytes.to_vec())
            .map(ResponseBody::Text)
            .unwrap_or_else(|error| ResponseBody::Binary(error.into_bytes()));
    } else if let Ok(value) = serde_json::from_slice(bytes) {
        return ResponseBody::Json(value);
    }
    String::from_utf8(bytes.to_vec())
        .map(ResponseBody::Text)
        .unwrap_or_else(|error| ResponseBody::Binary(error.into_bytes()))
}

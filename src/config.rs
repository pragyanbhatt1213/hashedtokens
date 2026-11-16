use reqwest::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use std::time::Duration;

use crate::error::RequestError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedirectPolicy {
    None,
    Limited(usize),
}

#[derive(Debug, Clone)]
pub struct EdmsClientConfig {
    pub base_url: String,
    pub timeout: Duration,
}

impl EdmsClientConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub default_headers: HeaderMap,
    pub user_agent: String,
    pub pool_max_idle_per_host: usize,
    pub redirect_policy: RedirectPolicy,
    pub edms: Option<EdmsClientConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            default_headers: HeaderMap::new(),
            user_agent: format!("hashedtokens/{}", env!("CARGO_PKG_VERSION")),
            pool_max_idle_per_host: usize::MAX,
            redirect_policy: RedirectPolicy::Limited(10),
            edms: None,
        }
    }
}

impl ClientConfig {
    pub fn default_header(mut self, name: &str, value: &str) -> Result<Self, RequestError> {
        let name = name
            .parse::<HeaderName>()
            .map_err(|error| RequestError::InvalidHeaderName {
                name: name.to_owned(),
                reason: error.to_string(),
            })?;
        let value =
            HeaderValue::from_str(value).map_err(|error| RequestError::InvalidHeaderValue {
                name: name.to_string(),
                reason: error.to_string(),
            })?;
        self.default_headers.insert(name, value);
        Ok(self)
    }

    pub fn edms(mut self, edms: EdmsClientConfig) -> Self {
        self.edms = Some(edms);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), RequestError> {
        if self.request_timeout.is_zero() || self.connect_timeout.is_zero() {
            return Err(RequestError::InvalidTimeout);
        }
        HeaderValue::from_str(&self.user_agent).map_err(|error| {
            RequestError::InvalidHeaderValue {
                name: USER_AGENT.to_string(),
                reason: error.to_string(),
            }
        })?;
        if self.edms.as_ref().is_some_and(|e| e.timeout.is_zero()) {
            return Err(RequestError::InvalidTimeout);
        }
        Ok(())
    }
}

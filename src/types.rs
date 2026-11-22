use reqwest::{Method, Url, header::HeaderMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedRequest {
    pub(crate) url: Url,
    pub(crate) method: Method,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedExchange {
    pub(crate) request: CapturedRequest,
    pub(crate) response: crate::response::ApiResponse,
}

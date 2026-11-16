use serde_json::Value;
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Default, Clone)]
pub struct RequestOptions {
    pub payload: Option<Value>,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub timeout: Option<Duration>,
    pub timeout_secs: Option<u64>,
    pub edms_collection: Option<String>,
    pub edms_annotation: Option<String>,
    pub edms_collection_annotation: Option<String>,
    pub edms_tags: Option<Vec<String>>,
    pub edms_collection_tags: Option<Vec<String>>,
}

impl RequestOptions {
    pub fn json(mut self, payload: Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.insert(name.into(), value.into());
        self
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn edms_collection(mut self, name: impl Into<String>) -> Self {
        self.edms_collection = Some(name.into());
        self
    }

    pub fn edms_annotation(mut self, annotation: impl Into<String>) -> Self {
        self.edms_annotation = Some(annotation.into());
        self
    }

    pub fn edms_collection_annotation(mut self, annotation: impl Into<String>) -> Self {
        self.edms_collection_annotation = Some(annotation.into());
        self
    }

    pub fn edms_tags(mut self, tags: Vec<String>) -> Self {
        self.edms_tags = Some(tags);
        self
    }

    pub fn edms_collection_tags(mut self, tags: Vec<String>) -> Self {
        self.edms_collection_tags = Some(tags);
        self
    }
}

pub type Options = RequestOptions;

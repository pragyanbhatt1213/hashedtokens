use crate::{
    response::{ApiResponse, ResponseBody},
    types::CapturedExchange,
};
use reqwest::header::HeaderMap;
use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Debug, Clone, Default)]
pub(crate) struct CollectionConfig {
    pub(crate) name: String,
    pub(crate) annotation: Option<String>,
    pub(crate) collection_annotation: Option<String>,
    pub(crate) tags: Option<Vec<String>>,
    pub(crate) collection_tags: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InsertPayload {
    ops: &'static str,
    endpoint_str: String,
    method: String,
    collection: String,
    request_body: Value,
    response_body: Value,
    request_headers: Map<String, Value>,
    response_headers: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    annotation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection_annotation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection_tags: Option<Vec<String>>,
}

#[derive(Debug, Error)]
pub(crate) enum SerializationError {
    #[error("binary body cannot be stored in an EDMS insert payload")]
    BinaryBody,
    #[error("captured request body is not valid JSON: {0}")]
    RequestBody(serde_json::Error),
    #[error("header '{name}' cannot be represented as a string")]
    HeaderValue { name: String },
    #[error("collection name cannot be empty")]
    CollectionName,
}

pub(crate) fn insert_payload(
    exchange: CapturedExchange,
    collection: CollectionConfig,
) -> Result<InsertPayload, SerializationError> {
    if collection.name.trim().is_empty() {
        return Err(SerializationError::CollectionName);
    }

    Ok(InsertPayload {
        ops: "insert",
        endpoint_str: exchange.request.url.to_string(),
        method: exchange.request.method.to_string(),
        collection: collection.name,
        request_body: request_body(exchange.request.body.as_deref())?,
        response_body: response_body(&exchange.response)?,
        request_headers: headers(&exchange.request.headers)?,
        response_headers: headers(&exchange.response.headers)?,
        annotation: collection.annotation,
        collection_annotation: collection.collection_annotation,
        tags: collection.tags,
        collection_tags: collection.collection_tags,
    })
}

fn request_body(body: Option<&[u8]>) -> Result<Value, SerializationError> {
    body.map_or(Ok(Value::Null), |bytes| {
        if std::str::from_utf8(bytes).is_err() {
            return Err(SerializationError::BinaryBody);
        }
        serde_json::from_slice(bytes).map_err(SerializationError::RequestBody)
    })
}

fn response_body(response: &ApiResponse) -> Result<Value, SerializationError> {
    match &response.body {
        ResponseBody::Empty => Ok(Value::Null),
        ResponseBody::Json(value) => Ok(value.clone()),
        ResponseBody::Text(value) => Ok(Value::String(value.clone())),
        ResponseBody::Binary(_) => Err(SerializationError::BinaryBody),
    }
}

fn headers(headers: &HeaderMap) -> Result<Map<String, Value>, SerializationError> {
    headers
        .iter()
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| (name.to_string(), Value::String(value.to_owned())))
                .map_err(|_| SerializationError::HeaderValue {
                    name: name.to_string(),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{response::ResponseBody, types::CapturedRequest};
    use reqwest::{
        Method, StatusCode, Url,
        header::{HeaderName, HeaderValue},
    };
    use serde_json::json;

    fn exchange(body: Option<Vec<u8>>, response: ResponseBody) -> CapturedExchange {
        let mut request_headers = HeaderMap::new();
        request_headers.insert("x-request", HeaderValue::from_static("request"));
        let mut response_headers = HeaderMap::new();
        response_headers.insert("x-response", HeaderValue::from_static("response"));
        CapturedExchange {
            request: CapturedRequest {
                url: Url::parse("https://target.example/items?page=1").unwrap(),
                method: Method::POST,
                headers: request_headers,
                body,
            },
            response: ApiResponse {
                status: StatusCode::CREATED,
                headers: response_headers,
                body: response,
            },
        }
    }

    fn collection() -> CollectionConfig {
        CollectionConfig {
            name: "items".to_owned(),
            ..CollectionConfig::default()
        }
    }

    #[test]
    fn serializes_insert_payload() {
        let payload = insert_payload(
            exchange(
                Some(br#"{"id":1}"#.to_vec()),
                ResponseBody::Json(json!({"ok": true})),
            ),
            CollectionConfig {
                annotation: Some("endpoint note".to_owned()),
                collection_annotation: Some("collection note".to_owned()),
                tags: Some(vec!["target".to_owned()]),
                collection_tags: Some(vec!["items".to_owned()]),
                ..collection()
            },
        )
        .unwrap();
        let value = serde_json::to_value(payload).unwrap();
        assert_eq!(value["ops"], "insert");
        assert_eq!(value["endpoint_str"], "https://target.example/items?page=1");
        assert_eq!(value["method"], "POST");
        assert_eq!(value["collection"], "items");
        assert_eq!(value["request_body"], json!({"id": 1}));
        assert_eq!(value["response_body"], json!({"ok": true}));
        assert_eq!(value["request_headers"]["x-request"], "request");
        assert_eq!(value["response_headers"]["x-response"], "response");
        assert_eq!(value["annotation"], "endpoint note");
        assert_eq!(value["collection_annotation"], "collection note");
        assert_eq!(value["tags"], json!(["target"]));
        assert_eq!(value["collection_tags"], json!(["items"]));
        assert!(!value.as_object().unwrap().contains_key("status"));
    }

    #[test]
    fn serializes_text_and_empty_bodies() {
        let text = insert_payload(
            exchange(None, ResponseBody::Text("failure".to_owned())),
            collection(),
        )
        .unwrap();
        let text = serde_json::to_value(text).unwrap();
        assert_eq!(text["request_body"], Value::Null);
        assert_eq!(text["response_body"], "failure");

        let empty = insert_payload(exchange(None, ResponseBody::Empty), collection()).unwrap();
        assert_eq!(
            serde_json::to_value(empty).unwrap()["response_body"],
            Value::Null
        );
    }

    #[test]
    fn rejects_binary_bodies() {
        let response = insert_payload(
            exchange(None, ResponseBody::Binary(vec![1, 2])),
            collection(),
        );
        assert!(matches!(response, Err(SerializationError::BinaryBody)));
    }

    #[test]
    fn rejects_invalid_request_json() {
        let result = insert_payload(
            exchange(Some(b"not-json".to_vec()), ResponseBody::Empty),
            collection(),
        );
        assert!(matches!(result, Err(SerializationError::RequestBody(_))));
    }

    #[test]
    fn rejects_invalid_headers() {
        let mut exchange = exchange(None, ResponseBody::Empty);
        exchange.request.headers.insert(
            HeaderName::from_static("x-invalid"),
            HeaderValue::from_bytes(&[0xff]).unwrap(),
        );
        let result = insert_payload(exchange, collection());
        assert!(matches!(
            result,
            Err(SerializationError::HeaderValue { name }) if name == "x-invalid"
        ));
    }

    #[test]
    fn rejects_empty_collection_name() {
        let result = insert_payload(
            exchange(None, ResponseBody::Empty),
            CollectionConfig::default(),
        );
        assert!(matches!(result, Err(SerializationError::CollectionName)));
    }
}

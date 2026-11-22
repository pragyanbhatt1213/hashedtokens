use crate::{
    config::{ClientConfig, RedirectPolicy},
    edms::{self, payload::CollectionConfig, submission::EdmsConfig},
    error::RequestError,
    request::RequestOptions,
    response::{ApiResponse, RequestOutcome, classify_body},
    types::{CapturedExchange, CapturedRequest, HttpMethod},
};
use reqwest::{
    Client,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};

pub struct HashedTokens {
    client: Client,
    default_headers: HeaderMap,
    edms: Option<EdmsConfig>,
}

impl HashedTokens {
    pub fn new() -> Result<Self, RequestError> {
        Self::with_config(ClientConfig::default())
    }

    pub fn with_config(config: ClientConfig) -> Result<Self, RequestError> {
        config.validate()?;
        let mut headers = config.default_headers;
        let user_agent = HeaderValue::from_str(&config.user_agent).map_err(|error| {
            RequestError::InvalidHeaderValue {
                name: USER_AGENT.to_string(),
                reason: error.to_string(),
            }
        })?;
        headers.insert(USER_AGENT, user_agent);
        let mut builder = Client::builder()
            .default_headers(headers.clone())
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(config.pool_max_idle_per_host);
        builder = match config.redirect_policy {
            RedirectPolicy::None => builder.redirect(reqwest::redirect::Policy::none()),
            RedirectPolicy::Limited(limit) => {
                builder.redirect(reqwest::redirect::Policy::limited(limit))
            }
        };
        let edms = config.edms.map(|ec| EdmsConfig {
            base_url: ec.base_url,
            timeout: ec.timeout,
        });
        builder
            .build()
            .map_err(RequestError::Http)
            .map(|client| Self {
                client,
                default_headers: headers,
                edms,
            })
    }

    pub async fn request(
        &self,
        url: &str,
        method: HttpMethod,
        options: RequestOptions,
    ) -> Result<RequestOutcome, RequestError> {
        let Some(edms_config) = &self.edms else {
            let exchange = self.execute(url, method, options).await?;
            return Ok(RequestOutcome::plain(exchange.response));
        };

        let collection_name = options
            .edms_collection
            .clone()
            .filter(|s| !s.trim().is_empty())
            .ok_or(RequestError::EdmsCollectionRequired)?;

        let collection = CollectionConfig {
            name: collection_name,
            annotation: options.edms_annotation.clone(),
            collection_annotation: options.edms_collection_annotation.clone(),
            tags: options.edms_tags.clone(),
            collection_tags: options.edms_collection_tags.clone(),
        };

        let connection = edms::client::open(edms_config)
            .await
            .map_err(RequestError::EdmsConnection)?;

        let exchange = self.execute(url, method, options).await?;
        let response = exchange.response.clone();

        match edms::client::store(edms_config, connection, collection, exchange).await {
            Ok(success) => Ok(RequestOutcome::stored(response, success)),
            Err(error) => Ok(RequestOutcome::edms_failed(response, error)),
        }
    }

    pub(crate) async fn execute(
        &self,
        url: &str,
        method: HttpMethod,
        options: RequestOptions,
    ) -> Result<CapturedExchange, RequestError> {
        let parsed = reqwest::Url::parse(url).map_err(|error| RequestError::InvalidUrl {
            url: url.to_owned(),
            reason: error.to_string(),
        })?;
        let timeout = options.timeout;
        if timeout.is_some_and(|value| value.is_zero()) {
            return Err(RequestError::InvalidTimeout);
        }
        let mut headers = self.default_headers.clone();
        for (name, value) in options.headers {
            let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                RequestError::InvalidHeaderName {
                    name: name.clone(),
                    reason: error.to_string(),
                }
            })?;
            let header_value = HeaderValue::from_str(&value).map_err(|error| {
                RequestError::InvalidHeaderValue {
                    name,
                    reason: error.to_string(),
                }
            })?;
            headers.insert(header_name, header_value);
        }
        let mut builder = match method {
            HttpMethod::Get => self.client.get(parsed),
            HttpMethod::Post => self.client.post(parsed),
            HttpMethod::Put => self.client.put(parsed),
            HttpMethod::Patch => self.client.patch(parsed),
            HttpMethod::Delete => self.client.delete(parsed),
        }
        .query(&options.query)
        .headers(headers);
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(payload) = options.payload {
            builder = builder.json(&payload);
        }
        let request = builder.build().map_err(RequestError::RequestConstruction)?;
        let captured_request = CapturedRequest {
            url: request.url().clone(),
            method: request.method().clone(),
            headers: request.headers().clone(),
            body: request
                .body()
                .and_then(|body| body.as_bytes().map(Vec::from)),
        };
        let response = self
            .client
            .execute(request)
            .await
            .map_err(map_request_error)?;
        let status = response.status();
        let headers = response.headers().clone();
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let content_type = content_type.map(str::to_owned);
        let bytes = response.bytes().await.map_err(RequestError::ResponseBody)?;
        Ok(CapturedExchange {
            request: captured_request,
            response: ApiResponse {
                status,
                headers,
                body: classify_body(&bytes, content_type.as_deref()),
            },
        })
    }
}

fn map_request_error(error: reqwest::Error) -> RequestError {
    if error.is_timeout() {
        RequestError::RequestTimeout(error)
    } else if error.is_connect() {
        RequestError::Connection(error)
    } else if error.is_builder() {
        RequestError::RequestConstruction(error)
    } else {
        RequestError::Http(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[tokio::test]
    async fn captures_the_built_request_and_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/target"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header("x-response", "captured")
                    .set_body_json(json!({"ok": true})),
            )
            .mount(&server)
            .await;

        let config = ClientConfig::default()
            .default_header("x-default", "configured")
            .expect("header should be valid");
        let client = HashedTokens::with_config(config).expect("client should build");
        let exchange = client
            .execute(
                &format!("{}/target", server.uri()),
                HttpMethod::Post,
                RequestOptions::default()
                    .query("page", "1")
                    .header("x-custom", "request")
                    .json(json!({"name": "example"})),
            )
            .await
            .expect("request should succeed");

        assert_eq!(
            exchange.request.url.as_str(),
            format!("{}/target?page=1", server.uri())
        );
        assert_eq!(exchange.request.method, reqwest::Method::POST);
        assert_eq!(exchange.request.headers["x-default"], "configured");
        assert_eq!(exchange.request.headers["x-custom"], "request");
        assert_eq!(
            exchange.request.body,
            Some(br#"{"name":"example"}"#.to_vec())
        );
        assert_eq!(exchange.response.status, reqwest::StatusCode::CREATED);
        assert_eq!(exchange.response.headers["x-response"], "captured");
        assert_eq!(exchange.response.json(), Some(&json!({"ok": true})));

        let requests = server.received_requests().await.expect("requests recorded");
        assert_eq!(requests.len(), 1);
    }
}

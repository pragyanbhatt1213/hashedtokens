use super::{
    payload::{CollectionConfig, insert_payload},
    submission::{self, EdmsConfig},
    websocket,
};
use crate::{response::EdmsSuccess, types::CapturedExchange};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EdmsError {
    #[error("EDMS WebSocket connection failed: {0}")]
    Connection(Box<dyn std::error::Error + Send + Sync>),
    #[error("EDMS serialization failed: {0}")]
    Serialization(Box<dyn std::error::Error + Send + Sync>),
    #[error("EDMS insert submission failed: {0}")]
    Submission(Box<dyn std::error::Error + Send + Sync>),
    #[error("EDMS transaction failed: {0}")]
    Transaction(Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) async fn open(config: &EdmsConfig) -> Result<websocket::EdmsConnection, EdmsError> {
    websocket::connect(config)
        .await
        .map_err(|e| EdmsError::Connection(Box::new(e)))
}

pub(crate) async fn store(
    config: &EdmsConfig,
    connection: websocket::EdmsConnection,
    collection: CollectionConfig,
    exchange: CapturedExchange,
) -> Result<EdmsSuccess, EdmsError> {
    let payload =
        insert_payload(exchange, collection).map_err(|e| EdmsError::Serialization(Box::new(e)))?;

    let submission = submission::submit(config, &payload)
        .await
        .map_err(|e| EdmsError::Submission(Box::new(e)))?;

    let success = connection
        .wait_for(&submission.transaction_id)
        .await
        .map_err(|e| EdmsError::Transaction(Box::new(e)))?;

    Ok(EdmsSuccess {
        transaction_id: success.transaction_id,
        op: success.op,
        status_code: success.result.status.code,
    })
}

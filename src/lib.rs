mod client;
mod config;
pub(crate) mod edms;
mod error;
mod request;
mod response;
mod types;

pub use client::HashedTokens;
pub use config::{ClientConfig, EdmsClientConfig, RedirectPolicy};
pub use edms::EdmsError;
pub use error::RequestError;
pub use request::{Options, RequestOptions};
pub use reqwest::{StatusCode, header};
pub use response::{ApiResponse, EdmsOutcome, EdmsSuccess, RequestOutcome, ResponseBody};
pub use types::{CrudType, HttpMethod};

pub fn hello() -> &'static str {
    "Hello from HashedTokens!"
}

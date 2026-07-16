use pubky::errors::{AuthError, BuildError, PkarrError, RequestError};
use pubky::StatusCode;
use thiserror::Error;

pub type ClientResult<T> = std::result::Result<T, ClientError>;

/// HTTP / Pubky client failures from the connector and resolver.
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("PubkyClient not initialized")]
    NotInitialized,

    #[error("404: {message}")]
    NotFound404 { message: String },

    #[error("429: {message}")]
    TooManyRequests429 { message: String },

    #[error("Server error (5xx): {message}")]
    ServerError5xx { message: String },

    #[error("Request failed: {message}")]
    RequestFailed { message: String },

    #[error("Pkarr failed: {0}")]
    PkarrFailed(#[from] PkarrError),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(#[from] AuthError),

    #[error("Build failed: {0}")]
    BuildFailed(#[from] BuildError),

    #[error("Parse failed: {0}")]
    ParseFailed(#[from] url::ParseError),
}

impl From<pubky::Error> for ClientError {
    fn from(err: pubky::Error) -> Self {
        match err {
            pubky::Error::Request(RequestError::Server { status, message }) => match status {
                StatusCode::NOT_FOUND => Self::NotFound404 { message },
                StatusCode::TOO_MANY_REQUESTS => Self::TooManyRequests429 { message },
                s if s.is_server_error() => Self::ServerError5xx { message },
                _ => Self::RequestFailed { message },
            },
            pubky::Error::Request(RequestError::Transport(e)) => Self::RequestFailed {
                message: e.to_string(),
            },
            pubky::Error::Request(
                RequestError::Validation { message } | RequestError::DecodeJson { message },
            ) => Self::RequestFailed { message },

            pubky::Error::Pkarr(e) => e.into(),
            pubky::Error::Authentication(e) => e.into(),
            pubky::Error::Build(e) => e.into(),
            pubky::Error::Parse(e) => e.into(),
        }
    }
}

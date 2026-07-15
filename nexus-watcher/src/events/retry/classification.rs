use crate::errors::EventProcessorError;
use pubky_watcher::{ClientError, RetryableError};

impl RetryableError for EventProcessorError {
    fn should_not_retry_now(&self) -> bool {
        matches!(
            self,
            EventProcessorError::GraphQueryFailed(true, _)
                | EventProcessorError::IndexOperationFailed(true, _)
                | EventProcessorError::HsEventsStreamRateLimitExhausted
        )
    }

    fn is_missing_dependency(&self) -> bool {
        matches!(self, EventProcessorError::MissingDependency { .. })
    }

    fn should_enqueue_for_retry(&self) -> bool {
        match self {
            EventProcessorError::ClientError(err) => match err.as_ref() {
                ClientError::NotInitialized
                | ClientError::TooManyRequests429 { .. }
                | ClientError::ServerError5xx { .. }
                | ClientError::RequestFailed { .. }
                | ClientError::PkarrFailed(_) => true,

                ClientError::NotFound404 { .. }
                | ClientError::AuthenticationFailed(_)
                | ClientError::BuildFailed(_)
                | ClientError::ParseFailed(_) => false,
            },

            EventProcessorError::InvalidEventLine(_)
            | EventProcessorError::SkipIndexing
            | EventProcessorError::SpecValidation(_)
            | EventProcessorError::HsBlacklisted { .. }
            | EventProcessorError::HsEventsStreamRateLimitExhausted
            | EventProcessorError::FetchSizeExceeded(_, _)
            | EventProcessorError::UserIdMismatch { .. } => false,

            _ => true,
        }
    }
}

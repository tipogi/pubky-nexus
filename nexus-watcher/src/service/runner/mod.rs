mod homeserver;
mod key_based;
mod key_based_hs_backoff;
mod key_based_user_backoff;

pub use homeserver::HsEventProcessorRunner;
pub use key_based::KeyBasedEventProcessorRunner;
pub use key_based_hs_backoff::HomeserverBackoff;
pub use key_based_user_backoff::UserNotFoundBackoff;

pub use crate::errors::{EventProcessorError, RunError};
pub use crate::events::Event;
pub use crate::service::indexer::DynEventProcessor;
pub use pubky_watcher::{status_from_run_result, TEventProcessorRunner};

pub type DynEventProcessorRunner =
    dyn TEventProcessorRunner<Event, EventProcessorError> + Send + Sync;

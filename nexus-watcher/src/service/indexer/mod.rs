mod homeserver;
mod key_based;

pub use homeserver::HsEventProcessor;
pub use key_based::{KeyBasedEventProcessor, KeyBasedEventSource, PubkyKeyBasedEventSource};

pub use crate::errors::{EventProcessorError, RunError};
pub use crate::events::Event;
pub use pubky_watcher::TEventProcessor;

pub type DynEventProcessor = dyn TEventProcessor<Event, EventProcessorError> + Send + Sync;

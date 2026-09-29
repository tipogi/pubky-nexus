mod error;
mod watcher_client;

pub use error::{ClientError, ClientResult};
pub use watcher_client::{
    ClientResponse, HomeserverEventSource, HomeserverResolver, KeyEventSource, KeyEventStream,
    ResourceReader, ResponseBody, WatcherClient,
};

//! The Client: the crate's read-only access to the Pubky network. Every Read goes through
//! [`WatcherClient`], built once and shared by everything that reads. Each of its four Reads
//! is a trait of its own, so that a caller, or a test, depends only on the one it uses:
//!
//! - [`FeedReader`]: a Homeserver's Events feed (`/events/`)
//! - [`StreamReader`]: a User pubkey's Event stream (`/events-stream`)
//! - [`ResourceReader`]: the Resource a `PUT` Event names, or a blob, under a size cap
//! - [`PkdnsLookup`]: the Homeserver record a User pubkey publishes
//!
//! The module depends on no Indexer's types: a Homeserver pubkey and a User pubkey are
//! [`pubky::PublicKey`]s, size caps are plain values, and it records no metrics.

mod error;
mod event;
mod fetch;
mod pkdns_lookup;
mod watcher_client;

pub use error::{ClientError, HsError, HsErrorKind, HsOp};
pub use event::EventType;
pub use pkdns_lookup::{published_homeserver, PkdnsLookup, PkdnsLookupError};
pub use watcher_client::{
    FeedReader, ResourceReader, StreamEvent, StreamReader, WatcherClient, WatcherClientBuilder,
    DEFAULT_MAX_FILE_SIZE, DEFAULT_REQUEST_TIMEOUT,
};

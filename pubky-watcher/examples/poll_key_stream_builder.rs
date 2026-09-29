//! Poll multiple users through a finite `/events-stream` feed on one homeserver.
//!
//! From the workspace root:
//!
//! ```bash
//! cargo run -p pubky-watcher --example poll_key_stream_builder
//! ```

use async_trait::async_trait;
use pubky::{Event, EventCursor, PublicKey};
use pubky_watcher::{EventHandler, Watcher, WatcherClient, WatcherError};
use tokio::sync::watch;

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const EVENTS_PER_RUN: u16 = 8;
const USER_KEYS: [&str; 3] = [
    "pubkywws1gjzowkp3aeacjku1mu9dewzor9mr6secjfreeskgz8as48gy",
    "pubky5a1diz4pghi47ywdfyfzpit5f3bdomzt4pugpbmq4rngdd4iub4y",
    "pubky68rkfi1d78baobycj6w4b7dga43o8qtnuhubban5at6qywrieb5y",
];

struct PrintingHandler;

#[async_trait]
impl EventHandler<Event, WatcherError> for PrintingHandler {
    async fn handle(&self, event: &Event) -> Result<(), WatcherError> {
        println!(
            "{} {} cursor={}",
            event.event_type,
            event.resource,
            event.cursor.id()
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), WatcherError> {
    let client = WatcherClient::mainnet()?;
    let homeserver = STAGING_HOMESERVER.parse::<PublicKey>()?;
    let users = USER_KEYS
        .into_iter()
        .map(|key| {
            key.parse::<PublicKey>()
                .map(|user| (user, EventCursor::new(0)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let watcher = Watcher::key_stream(client, homeserver, users)
        .handler(PrintingHandler)
        .events_limit(EVENTS_PER_RUN)
        .path("/pub/")
        .build(shutdown_rx)?;

    let outcome = watcher.run().await?;
    for (user, cursor) in outcome.cursors {
        println!("{user}: next cursor {}", cursor.id());
    }
    Ok(())
}

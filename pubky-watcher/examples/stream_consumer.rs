//! A Stream consumer: one Run over the Event streams of three User pubkeys on one
//! Homeserver, built with [`Watcher::stream`], with one Checkpoint per User pubkey.
//!
//! From the workspace root:
//!
//! ```bash
//! cargo run -p pubky-watcher --example stream_consumer
//! ```

use async_trait::async_trait;
use pubky::{EventCursor, PublicKey};
use pubky_watcher::{EventHandler, StreamEvent, Watcher, WatcherClient};
use tokio::sync::watch;

/// This example's error: any error, boxed. An Indexer would use its own type.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const EVENTS_PER_USER: u16 = 8;
const USER_PUBKEYS: [&str; 3] = [
    "wws1gjzowkp3aeacjku1mu9dewzor9mr6secjfreeskgz8as48gy",
    "5a1diz4pghi47ywdfyfzpit5f3bdomzt4pugpbmq4rngdd4iub4y",
    "68rkfi1d78baobycj6w4b7dga43o8qtnuhubban5at6qywrieb5y",
];

struct PrintingHandler;

#[async_trait]
impl EventHandler<StreamEvent, BoxError> for PrintingHandler {
    async fn handle(&self, event: &StreamEvent) -> Result<(), BoxError> {
        println!(
            "{} {} cursor={}",
            event.event_type,
            event.uri(),
            event.cursor
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let client = WatcherClient::mainnet()?;
    let hs_pubkey = PublicKey::try_from_z32(STAGING_HOMESERVER)?;
    let users = USER_PUBKEYS
        .into_iter()
        .map(|z32| PublicKey::try_from_z32(z32).map(|pubkey| (pubkey, EventCursor::new(0))))
        .collect::<Result<Vec<_>, _>>()?;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let consumer = Watcher::stream(client, hs_pubkey, users)
        .handler(PrintingHandler)
        .events_limit(EVENTS_PER_USER)
        .path("/pub/")
        .build(shutdown_rx)?;

    let outcome = consumer.run().await?;
    for (user_pubkey, checkpoint) in outcome.checkpoints {
        println!("{}: checkpoint {}", user_pubkey.z32(), checkpoint.id());
    }
    Ok(())
}

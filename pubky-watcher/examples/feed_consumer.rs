//! A Feed consumer: five Runs over one Homeserver's Events feed, built with
//! [`Watcher::feed`].
//!
//! Reads the public staging Homeserver, prints each Event, and reads each `PUT`'s Resource.
//! Each Run returns the Checkpoint to start the next one from; an Indexer would store it.
//! From the workspace root:
//!
//! ```bash
//! cargo run -p pubky-watcher --example feed_consumer
//! ```
//!
//! The crate logs with `tracing`. This example prints instead of installing a subscriber.

use async_trait::async_trait;
use pubky::{EventCursor, PubkyResource, PublicKey};
use pubky_watcher::{EventHandler, EventType, FeedEvent, ResourceReader, Watcher, WatcherClient};
use tokio::sync::watch;

/// This example's error: any error, boxed. An Indexer would use its own type.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const RUNS: usize = 5;
const EVENTS_PER_RUN: u16 = 8;
/// How much of a Resource to print.
const PRINTED_BYTES: usize = 200;

/// Prints each Event, and the start of each written Resource.
struct ResourcePrintingHandler {
    client: WatcherClient,
}

#[async_trait]
impl EventHandler<FeedEvent, BoxError> for ResourcePrintingHandler {
    async fn handle(&self, event: &FeedEvent) -> Result<(), BoxError> {
        println!("{} {}", event.event_type, event.uri);
        if event.event_type != EventType::Put {
            return Ok(());
        }

        let resource: PubkyResource = event.uri.parse()?;
        match self.client.read_resource(&resource).await {
            Ok(body) => {
                let shown = &body[..body.len().min(PRINTED_BYTES)];
                println!("  {} bytes: {}", body.len(), String::from_utf8_lossy(shown));
            }
            // The Resource may be gone by now, or be larger than a JSON Resource may be.
            Err(error) => println!("  {error}"),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let hs_pubkey = PublicKey::try_from_z32(STAGING_HOMESERVER)?;
    let client = WatcherClient::mainnet()?;

    let consumer = Watcher::feed(client.clone(), hs_pubkey)
        .handler(ResourcePrintingHandler { client })
        .events_limit(EVENTS_PER_RUN)
        .build(shutdown_rx)?;

    let mut checkpoint = EventCursor::new(0);
    for n in 1..=RUNS {
        println!("Run {n}/{RUNS}, from cursor {}", checkpoint.id());
        let outcome = consumer.run(checkpoint).await?;
        checkpoint = outcome.checkpoint;
    }

    println!("checkpoint: {}", checkpoint.id());
    Ok(())
}

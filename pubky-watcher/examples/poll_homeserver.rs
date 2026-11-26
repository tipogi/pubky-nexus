//! Five `/events/` polls of one homeserver through the low-level traits:
//! a [`Run`] for one poll, run by a [`Cycle`].
//!
//! This is the path Nexus takes, with its own cursor store, retry queue and backoff. The
//! example keeps its cursor in memory and fails fast on any error. From the workspace root:
//!
//! ```bash
//! cargo run -p pubky-watcher --example poll_homeserver
//! ```
//!
//! The crate logs with `tracing`. This example prints instead of installing a subscriber.

use std::sync::Arc;

use async_trait::async_trait;
use pubky::PublicKey;
use pubky_watcher::{Cycle, FeedBatch, FeedEvent, FeedReader, Run, RunOutcome, WatcherClient};
use tokio::sync::{watch, Mutex};

/// This example's error: any error, boxed. An application would use its own type.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const POLL_TICKS: usize = 5;
const EVENTS_PER_TICK: u16 = 8;

/// One poll of the homeserver: reads the batch after the cursor, prints each event, and
/// moves the cursor once the batch is done.
struct PrintRun {
    hs_pubkey: PublicKey,
    hs_pubkey_z32: String,
    reader: Arc<dyn FeedReader>,
    cursor: Arc<Mutex<u64>>,
}

#[async_trait]
impl Run<FeedEvent, BoxError> for PrintRun {
    fn instance_name(&self) -> &'static str {
        "poll_homeserver"
    }

    fn hs_pubkey(&self) -> &str {
        &self.hs_pubkey_z32
    }

    async fn handle(&self, event: &FeedEvent) -> Result<(), BoxError> {
        println!("{} {}", event.event_type, event.uri);
        Ok(())
    }

    async fn run_internal(self: Arc<Self>) -> Result<RunOutcome, BoxError> {
        let cursor = *self.cursor.lock().await;
        let lines = self
            .reader
            .read_feed(&self.hs_pubkey, cursor, EVENTS_PER_TICK)
            .await?;
        let batch = FeedBatch::split(&lines);
        if !batch.has_events() {
            println!("No new events");
        }

        // `handle_event_line` parses each line with `FeedEvent`'s `FromEventLine`,
        // skips what it does not recognise, and hands the rest to `handle`.
        for line in &batch.event_lines {
            self.handle_event_line(line).await?;
        }

        if let Some(next) = batch.cursor {
            *self.cursor.lock().await = next.parse()?;
            println!("cursor: {next}");
        }
        Ok(RunOutcome::Completed)
    }
}

/// Runs one [`PrintRun`] per tick, for the staging homeserver only.
struct StagingCycle {
    reader: Arc<dyn FeedReader>,
    cursor: Arc<Mutex<u64>>,
    shutdown_rx: watch::Receiver<bool>,
}

#[async_trait]
impl Cycle<FeedEvent, BoxError> for StagingCycle {
    type Error = BoxError;

    fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    async fn build_run(
        &self,
        hs_pubkey: &str,
    ) -> Result<Arc<dyn Run<FeedEvent, BoxError>>, BoxError> {
        Ok(Arc::new(PrintRun {
            hs_pubkey: PublicKey::try_from_z32(hs_pubkey)?,
            hs_pubkey_z32: hs_pubkey.to_string(),
            reader: self.reader.clone(),
            cursor: self.cursor.clone(),
        }))
    }

    async fn homeservers(&self) -> Result<Vec<String>, BoxError> {
        Ok(vec![STAGING_HOMESERVER.to_string()])
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let cycle = StagingCycle {
        reader: Arc::new(WatcherClient::mainnet()?),
        cursor: Arc::new(Mutex::new(0)),
        shutdown_rx,
    };

    for tick in 1..=POLL_TICKS {
        println!("Tick {tick}/{POLL_TICKS}");
        let stats = cycle.run().await?.0;
        if stats.count_ok() != 1 {
            return Err("the poll failed".into());
        }
    }
    Ok(())
}

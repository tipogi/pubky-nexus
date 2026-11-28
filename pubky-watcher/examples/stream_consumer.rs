//! A Stream consumer: three Runs over the Event streams of three User pubkeys on one
//! Homeserver, built once with [`Watcher::stream`].
//!
//! Each Run is given the users, each with the Cursor to read it from, and returns one
//! Checkpoint per User pubkey, in the same shape and order: they are the next Run's users as
//! they are. An Indexer would store them.
//!
//! A Run returns `Ok` with every Checkpoint whatever went wrong with a user or with the
//! Homeserver. Its outcome lists the users whose Batch ended early (`failures`) and says why
//! the Run stopped before reading every user, if it did (`stopped`). From the workspace root:
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
const RUNS: usize = 3;
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let consumer = Watcher::stream(client, hs_pubkey)
        .handler(PrintingHandler)
        .events_limit(EVENTS_PER_USER)
        .path("/pub/")
        .build(shutdown_rx)?;

    // The users of the first Run, each read from the start of their Event stream.
    let mut users = USER_PUBKEYS
        .into_iter()
        .map(|z32| PublicKey::try_from_z32(z32).map(|pubkey| (pubkey, EventCursor::new(0))))
        .collect::<Result<Vec<_>, _>>()?;

    for n in 1..=RUNS {
        println!("Run {n}/{RUNS}");
        // `?` loses no Checkpoint: a Run fails only when it could not be made.
        let outcome = consumer.run(users).await?;

        // A user whose Batch ended early stays at the last Event handled; the others were
        // still read.
        for (user_pubkey, error) in &outcome.failures {
            println!("{}: batch ended early: {error}", user_pubkey.z32());
        }

        // The Checkpoints are the next Run's users as they are.
        users = outcome.checkpoints;

        // A stopped Run did not read every user. An Indexer would back the Homeserver off,
        // unless the reason is its own shutdown.
        if let Some(reason) = outcome.stopped {
            println!("the Run stopped before reading every user: {reason:?}");
            break;
        }
    }

    for (user_pubkey, checkpoint) in &users {
        println!("{}: checkpoint {}", user_pubkey.z32(), checkpoint.id());
    }
    Ok(())
}

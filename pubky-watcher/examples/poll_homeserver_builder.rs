//! Five `/events/` polls through the convenience [`Watcher`] builder.
//!
//! Same staging homeserver behaviour as `poll_homeserver`, with less wiring:
//! pass one homeserver and a handler, then feed each returned cursor into the
//! next `run`.
//! Event lines use the crate's default [`HomeserverEvent`] parser.
//!
//! From the workspace root:
//!
//! ```bash
//! cargo run -p pubky-watcher --example poll_homeserver_builder
//! ```
//!
//! The crate logs with `tracing`. This example uses `println!` instead of
//! installing `tracing-subscriber`.

use async_trait::async_trait;
use pubky::{EventCursor, PublicKey};
use pubky_watcher::{
    EventHandler, EventMethod, HomeserverEvent, PubkyConnector, Watcher, WatcherError,
};
use tokio::sync::watch;

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const POLL_TICKS: usize = 5;
const EVENTS_PER_TICK: u16 = 8;

struct ResourcePrintingHandler;

#[async_trait]
impl EventHandler<HomeserverEvent, WatcherError> for ResourcePrintingHandler {
    async fn handle(&self, event: &HomeserverEvent) -> Result<(), WatcherError> {
        println!("{} {}", event.method, event.uri);

        if event.method != EventMethod::Put {
            return Ok(());
        }

        let response = PubkyConnector::get()?
            .public_storage()
            .get(&event.uri)
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if !content_type.starts_with("text/") && !content_type.contains("json") {
            println!("{status}");
            return Ok(());
        }

        let body = response.text().await?;
        println!("{status} {body}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), WatcherError> {
    PubkyConnector::initialise(None).await?;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let homeserver = STAGING_HOMESERVER.parse::<PublicKey>()?;

    let watcher = Watcher::homeserver(homeserver)
        .handler(ResourcePrintingHandler)
        .events_limit(EVENTS_PER_TICK)
        .build(shutdown_rx)?;

    let mut cursor = EventCursor::new(0);
    for tick in 1..=POLL_TICKS {
        println!("Tick {tick}/{POLL_TICKS}");
        let outcome = watcher.run(cursor).await?;
        cursor = outcome.cursor;
    }

    println!("next cursor: {}", cursor.id());
    Ok(())
}

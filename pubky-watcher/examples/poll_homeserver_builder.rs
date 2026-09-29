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
    read_stream_capped, EventHandler, EventMethod, HomeserverEvent, ResourceReader, Watcher,
    WatcherClient, WatcherError,
};
use tokio::sync::watch;

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const POLL_TICKS: usize = 5;
const EVENTS_PER_TICK: u16 = 8;
const MAX_RESOURCE_BODY: usize = 2 * 1024 * 1024;

struct ResourcePrintingHandler {
    client: WatcherClient,
}

#[async_trait]
impl EventHandler<HomeserverEvent, WatcherError> for ResourcePrintingHandler {
    async fn handle(&self, event: &HomeserverEvent) -> Result<(), WatcherError> {
        println!("{} {}", event.method, event.uri);

        if event.method != EventMethod::Put {
            return Ok(());
        }

        let response = self.client.get_resource(&event.uri).await?;
        let status = response.status;
        let content_type = response
            .content_type
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase();

        if !content_type.starts_with("text/") && !content_type.contains("json") {
            println!("{status}");
            return Ok(());
        }

        let (body, exceeded) = read_stream_capped(response.body, MAX_RESOURCE_BODY).await?;
        if exceeded {
            println!("{status} resource exceeded {MAX_RESOURCE_BODY} bytes");
            return Ok(());
        }
        let body = String::from_utf8_lossy(&body);
        println!("{status} {body}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), WatcherError> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let homeserver = STAGING_HOMESERVER.parse::<PublicKey>()?;
    let client = WatcherClient::mainnet()?;

    let watcher = Watcher::homeserver(client.clone(), homeserver)
        .handler(ResourcePrintingHandler { client })
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

//! Five `/events/` polls through [`TEventProcessorRunner`] and [`TEventProcessor`].
//!
//! Polls the public staging homeserver. From the workspace root:
//!
//! ```bash
//! cargo run -p pubky-watcher --example poll_homeserver
//! ```
//!
//! The crate logs with `tracing`. This example uses `println!` instead of
//! installing `tracing-subscriber`.

use std::sync::Arc;

use async_trait::async_trait;
use pubky::Method;
use pubky_watcher::{
    EventBatch, EventHandler, LineParseOutcome, ParseFromLine, PubkyConnector, TEventProcessor,
    TEventProcessorRunner,
};
use tokio::sync::{watch, Mutex};

const STAGING_HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";
const POLL_TICKS: usize = 5;
const EVENTS_PER_TICK: usize = 8;

type PollError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
struct HomeserverEvent {
    method: String,
    uri: String,
}

impl ParseFromLine for HomeserverEvent {
    type Error = PollError;

    fn parse_line(line: &str) -> Result<LineParseOutcome<Self>, Self::Error> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(LineParseOutcome::Skipped);
        }
        let Some((method, uri)) = line.split_once(' ') else {
            return Ok(LineParseOutcome::Unrecognized {
                reason: format!("expected `<TYPE> <uri>`, got: {line}"),
            });
        };
        Ok(LineParseOutcome::Parsed(HomeserverEvent {
            method: method.to_string(),
            uri: uri.to_string(),
        }))
    }
}

type DynEventHandler = dyn EventHandler<HomeserverEvent, PollError> + Send + Sync;
type DynEventProcessor = dyn TEventProcessor<HomeserverEvent, PollError> + Send + Sync;

struct ResourcePrintingHandler;

#[async_trait]
impl EventHandler<HomeserverEvent, PollError> for ResourcePrintingHandler {
    async fn handle(&self, event: &HomeserverEvent) -> Result<(), PollError> {
        println!("{} {}", event.method, event.uri);

        if event.method != "PUT" {
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

struct HomeserverPollProcessor {
    event_handler: Arc<DynEventHandler>,
    cursor: Arc<Mutex<String>>,
}

#[async_trait]
impl TEventProcessor<HomeserverEvent, PollError> for HomeserverPollProcessor {
    fn event_handler(&self) -> &Arc<DynEventHandler> {
        &self.event_handler
    }

    fn instance_name(&self) -> String {
        "poll_homeserver".to_string()
    }

    async fn run_internal(self: Arc<Self>) -> Result<(), PollError> {
        let cursor = self.cursor.lock().await.clone();
        let body = self
            .poll_events(&cursor)
            .await
            .inspect_err(|error| eprintln!("Polling failed: {error}"))?;

        if let Some(next_cursor) = self.process_event_body(&body).await? {
            *self.cursor.lock().await = next_cursor;
        }

        Ok(())
    }
}

impl HomeserverPollProcessor {
    async fn poll_events(&self, cursor: &str) -> Result<String, PollError> {
        let url = format!(
            "https://{STAGING_HOMESERVER}/events/?cursor={cursor}&limit={EVENTS_PER_TICK}"
        );
        println!("GET {url}");
        let body = PubkyConnector::get()?
            .client()
            .request(Method::GET, &url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        Ok(body)
    }

    async fn process_event_body(&self, body: &str) -> Result<Option<String>, PollError> {
        let batch = EventBatch::from_body(body);

        if !batch.has_events() {
            println!("No new events");
        } else if batch.cursor.is_none() {
            println!("Skipping batch: events present but no cursor line");
            return Ok(None);
        }

        for line in &batch.event_lines {
            self.process_event_line(line).await?;
        }

        if let Some(cursor) = batch.cursor {
            println!("cursor: {cursor}");
        }
        Ok(batch.cursor.map(str::to_owned))
    }
}

struct StagingPollRunner {
    shutdown_rx: watch::Receiver<bool>,
    cursor: Arc<Mutex<String>>,
    event_handler: Arc<DynEventHandler>,
}

#[async_trait]
impl TEventProcessorRunner<HomeserverEvent, PollError> for StagingPollRunner {
    fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    async fn build(
        &self,
        _hs_id: &str,
    ) -> Result<Arc<DynEventProcessor>, PollError> {
        Ok(Arc::new(HomeserverPollProcessor {
            event_handler: self.event_handler.clone(),
            cursor: self.cursor.clone(),
        }))
    }

    async fn pre_run(&self) -> Result<Vec<String>, PollError> {
        Ok(vec![STAGING_HOMESERVER.to_string()])
    }
}

#[tokio::main]
async fn main() -> Result<(), PollError> {
    PubkyConnector::initialise(None).await?;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = StagingPollRunner {
        shutdown_rx,
        cursor: Arc::new(Mutex::new("0".to_string())),
        event_handler: Arc::new(ResourcePrintingHandler),
    };

    for tick in 1..=POLL_TICKS {
        println!("Tick {tick}/{POLL_TICKS}");
        runner.run().await?;
    }
    Ok(())
}

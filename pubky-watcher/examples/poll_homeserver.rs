//! One `/events/` poll through [`TEventProcessorRunner`] and [`TEventProcessor`].
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
    EventBatch, EventHandler, EventMetadata, LineParseOutcome, ParseFromLine, ProcessedStats,
    PubkyConnector, RetryableError, RunAllProcessorsStats, TEventProcessor, TEventProcessorRunner,
};
use tokio::sync::watch;

const HOMESERVER: &str = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy";

#[derive(Debug)]
struct LineEvent {
    line: String,
    event_type: String,
    uri: String,
}

impl EventMetadata for LineEvent {
    fn uri(&self) -> &str {
        &self.uri
    }
    fn event_type_display(&self) -> &str {
        &self.event_type
    }
    fn user_id(&self) -> String {
        String::new()
    }
    fn resource_label(&self) -> String {
        String::new()
    }
    fn resource_id(&self) -> String {
        String::new()
    }
}

impl ParseFromLine for LineEvent {
    type Error = ExampleError;

    fn parse_line(line: &str) -> Result<LineParseOutcome<Self>, Self::Error> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(LineParseOutcome::Skipped);
        }
        let Some((event_type, uri)) = line.split_once(' ') else {
            return Ok(LineParseOutcome::Unrecognized {
                reason: format!("expected `<TYPE> <uri>`, got: {line}"),
            });
        };
        Ok(LineParseOutcome::Parsed(LineEvent {
            line: line.to_string(),
            event_type: event_type.to_string(),
            uri: uri.to_string(),
        }))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct ExampleError(String);

impl RetryableError for ExampleError {
    fn should_not_retry_now(&self) -> bool {
        true
    }
    fn is_missing_dependency(&self) -> bool {
        false
    }
    fn should_enqueue_for_retry(&self) -> bool {
        false
    }
}

struct PrintHandler;

#[async_trait]
impl EventHandler<LineEvent, ExampleError> for PrintHandler {
    async fn handle(&self, event: &LineEvent) -> Result<(), ExampleError> {
        println!("{}", event.line);

        if event.event_type != "PUT" {
            return Ok(());
        }

        let response = PubkyConnector::get()
            .map_err(|e| ExampleError(e.to_string()))?
            .public_storage()
            .get(&event.uri)
            .await
            .map_err(|e| ExampleError(e.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| ExampleError(e.to_string()))?;
        println!("{status} {body}");
        Ok(())
    }
}

struct Processor {
    event_handler: Arc<dyn EventHandler<LineEvent, ExampleError> + Send + Sync>,
}

#[async_trait]
impl TEventProcessor<LineEvent, ExampleError> for Processor {
    fn event_handler(&self) -> &Arc<dyn EventHandler<LineEvent, ExampleError> + Send + Sync> {
        &self.event_handler
    }

    fn instance_name(&self) -> String {
        "poll_homeserver".to_string()
    }

    async fn run_internal(self: Arc<Self>) -> Result<(), ExampleError> {
        let body = self.poll_events().await?;
        self.process_event_body(&body).await
    }
}

impl Processor {
    async fn poll_events(&self) -> Result<String, ExampleError> {
        let url = format!("https://{HOMESERVER}/events/?cursor=0&limit=8");
        println!("GET {url}");
        let body = PubkyConnector::get()
            .map_err(|e| ExampleError(e.to_string()))?
            .client()
            .request(Method::GET, &url)
            .send()
            .await
            .map_err(|e| ExampleError(e.to_string()))?
            .text()
            .await
            .map_err(|e| ExampleError(e.to_string()))?;

        Ok(body)
    }

    async fn process_event_body(&self, body: &str) -> Result<(), ExampleError> {
        let batch = EventBatch::from_body(body);
        if !batch.has_events() {
            println!("No new events");
        }
        for line in &batch.event_lines {
            self.process_event_line(line).await?;
        }
        if let Some(cursor) = batch.cursor {
            println!("cursor: {cursor}");
        }
        Ok(())
    }
}

struct Runner {
    shutdown_rx: watch::Receiver<bool>,
}

#[async_trait]
impl TEventProcessorRunner<LineEvent, ExampleError> for Runner {
    fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    async fn build(
        &self,
        _hs_id: &str,
    ) -> Result<
        Arc<dyn TEventProcessor<LineEvent, ExampleError> + Send + Sync>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(Arc::new(Processor {
            event_handler: Arc::new(PrintHandler),
        }))
    }

    async fn pre_run(&self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(vec![HOMESERVER.to_string()])
    }

    async fn post_run(&self, stats: RunAllProcessorsStats) -> ProcessedStats {
        for run in &stats.stats {
            println!(
                "processor {} {:?} in {}ms",
                run.hs_id,
                run.status,
                run.duration.as_millis()
            );
        }
        ProcessedStats(stats)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    PubkyConnector::initialise(None).await?;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    Runner { shutdown_rx }.run().await?;
    Ok(())
}

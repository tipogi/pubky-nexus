//! Minimal example: poll a homeserver `/events/` feed by public key.
//!
//! # Prerequisites
//!
//! For the default testnet homeserver you need a local testnet running
//! (Pubky DHT + homeserver). Use
//! [pubky-antfarm](https://github.com/tipogi/pubky-antfarm):
//!
//! ```bash
//! git clone https://github.com/tipogi/pubky-antfarm.git
//! cd pubky-antfarm && cargo run
//! ```
//!
//! # Run
//!
//! From the workspace root:
//!
//! ```bash
//! # Default: static testnet homeserver + testnet client
//! cargo run -p pubky-watcher --example poll_homeserver
//!
//! # Custom homeserver key (z32 public key)
//! cargo run -p pubky-watcher --example poll_homeserver -- \
//!   --homeserver 8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo
//!
//! # Mainnet (omit --testnet)
//! cargo run -p pubky-watcher --example poll_homeserver -- \
//!   --homeserver <z32-pubkey> --no-testnet
//!
//! # Resume from a cursor / change batch size
//! cargo run -p pubky-watcher --example poll_homeserver -- \
//!   --cursor 0 --limit 50
//! ```

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use pubky::Method;
use pubky_watcher::{
    EventHandler, EventMetadata, LineParseOutcome, ParseFromLine, ProcessedStats, PubkyConnector,
    RetryableError, TEventProcessor, TEventProcessorRunner,
};
use tokio::sync::watch;
use tracing::{info, warn};

/// Static testnet homeserver public key (z32).
/// See `pubky-testnet` binary / README.
const DEFAULT_TESTNET_HOMESERVER: &str =
    "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";

#[derive(Parser, Debug)]
#[command(
    about = "Poll pubky homeserver /events/ using pubky-watcher",
    long_about = None
)]
struct Args {
    /// Homeserver public key (z32), without a `pk:` / `pubky` prefix
    #[arg(long, default_value = DEFAULT_TESTNET_HOMESERVER)]
    homeserver: String,

    /// Events cursor to resume from (`0` = beginning)
    #[arg(long, default_value = "0")]
    cursor: String,

    /// Max events per poll
    #[arg(long, default_value_t = 100)]
    limit: u16,

    /// How many runner ticks to execute before exiting
    #[arg(long, default_value_t = 1)]
    ticks: u32,

    /// Delay between ticks (milliseconds)
    #[arg(long, default_value_t = 1_000)]
    interval_ms: u64,

    /// Use the mainnet Pubky client instead of testnet
    #[arg(long)]
    no_testnet: bool,

    /// Testnet relay / DHT host passed to [`PubkyConnector::initialise`]
    #[arg(long, default_value = "localhost")]
    testnet_host: String,
}

#[derive(Debug, Clone)]
struct SimpleEvent {
    uri: String,
    event_type: String,
}

impl EventMetadata for SimpleEvent {
    fn uri(&self) -> &str {
        &self.uri
    }

    fn event_type_display(&self) -> &str {
        &self.event_type
    }

    fn user_id(&self) -> String {
        self.uri
            .strip_prefix("pubky://")
            .and_then(|rest| rest.split('/').next())
            .unwrap_or("unknown")
            .to_string()
    }

    fn resource_label(&self) -> String {
        "raw".to_string()
    }

    fn resource_id(&self) -> String {
        self.uri.clone()
    }
}

impl ParseFromLine for SimpleEvent {
    type Error = ExampleError;

    fn parse_line(line: &str) -> Result<LineParseOutcome<Self>, Self::Error> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(LineParseOutcome::Skipped);
        }
        if line.starts_with("cursor: ") {
            return Ok(LineParseOutcome::Skipped);
        }

        let Some((event_type, uri)) = line.split_once(' ') else {
            return Ok(LineParseOutcome::Unrecognized {
                reason: format!("expected `<TYPE> <uri>`, got: {line}"),
            });
        };

        Ok(LineParseOutcome::Parsed(SimpleEvent {
            uri: uri.to_string(),
            event_type: event_type.to_string(),
        }))
    }
}

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error("client: {0}")]
    Client(String),
}

impl RetryableError for ExampleError {
    fn should_not_retry_now(&self) -> bool {
        false
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
impl EventHandler<SimpleEvent, ExampleError> for PrintHandler {
    async fn handle(&self, event: &SimpleEvent) -> Result<(), ExampleError> {
        println!("{} {}", event.event_type, event.uri);
        Ok(())
    }
}

struct ExampleProcessor {
    homeserver_id: String,
    cursor: String,
    limit: u16,
    event_handler: Arc<dyn EventHandler<SimpleEvent, ExampleError> + Send + Sync>,
    shutdown_rx: watch::Receiver<bool>,
}

#[async_trait]
impl TEventProcessor<SimpleEvent, ExampleError> for ExampleProcessor {
    fn event_handler(&self) -> &Arc<dyn EventHandler<SimpleEvent, ExampleError> + Send + Sync> {
        &self.event_handler
    }

    fn instance_name(&self) -> String {
        format!("example-processor:{}", self.homeserver_id)
    }

    fn homeserver_id(&self) -> Option<&str> {
        Some(&self.homeserver_id)
    }

    async fn run_internal(self: Arc<Self>) -> Result<(), ExampleError> {
        let lines = self.poll_events().await?;
        if lines.is_empty() {
            info!("No new events");
            return Ok(());
        }

        info!(count = lines.len(), "Processing event lines");
        for line in &lines {
            if *self.shutdown_rx.borrow() {
                info!("Shutdown detected; stopping batch");
                break;
            }

            if let Some(cursor) = line.strip_prefix("cursor: ") {
                info!(%cursor, "Homeserver returned next cursor");
                continue;
            }

            self.process_event_line(line).await?;
        }

        Ok(())
    }
}

impl ExampleProcessor {
    async fn poll_events(&self) -> Result<Vec<String>, ExampleError> {
        let pubky = PubkyConnector::get().map_err(|e| ExampleError::Client(e.to_string()))?;
        let url = format!(
            "https://{}/events/?cursor={}&limit={}",
            self.homeserver_id, self.cursor, self.limit
        );

        info!(%url, "GET /events/");
        let response = pubky
            .client()
            .request(Method::GET, &url)
            .send()
            .await
            .map_err(|e| ExampleError::Client(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ExampleError::Client(format!(
                "homeserver returned HTTP {}",
                response.status()
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| ExampleError::Client(e.to_string()))?;

        Ok(body.trim().lines().map(String::from).collect())
    }
}

struct ExampleRunner {
    homeserver_id: String,
    cursor: String,
    limit: u16,
    shutdown_rx: watch::Receiver<bool>,
}

#[async_trait]
impl TEventProcessorRunner<SimpleEvent, ExampleError> for ExampleRunner {
    fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    async fn build(
        &self,
        hs_id: &str,
    ) -> Result<
        Arc<dyn TEventProcessor<SimpleEvent, ExampleError> + Send + Sync>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(Arc::new(ExampleProcessor {
            homeserver_id: hs_id.to_string(),
            cursor: self.cursor.clone(),
            limit: self.limit,
            event_handler: Arc::new(PrintHandler),
            shutdown_rx: self.shutdown_rx.clone(),
        }))
    }

    async fn pre_run(&self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(vec![self.homeserver_id.clone()])
    }

    async fn post_run(&self, stats: pubky_watcher::RunAllProcessorsStats) -> ProcessedStats {
        for run in &stats.stats {
            info!(
                hs_id = %run.hs_id,
                ?run.status,
                elapsed_ms = run.duration.as_millis(),
                "Processor finished"
            );
        }
        ProcessedStats(stats)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let testnet = !args.no_testnet;
    let testnet_host = testnet.then_some(args.testnet_host.as_str());

    info!(
        homeserver = %args.homeserver,
        cursor = %args.cursor,
        limit = args.limit,
        testnet,
        "Starting poll_homeserver example"
    );

    PubkyConnector::initialise(testnet_host).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runner = ExampleRunner {
        homeserver_id: args.homeserver,
        cursor: args.cursor,
        limit: args.limit,
        shutdown_rx,
    };

    for tick in 1..=args.ticks {
        info!(tick, "Runner tick");
        match runner.run().await {
            Ok(_) => {}
            Err(e) => warn!(error = %e, "Runner tick failed"),
        }

        if tick < args.ticks {
            tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
        }
    }

    let _ = shutdown_tx.send(true);
    Ok(())
}

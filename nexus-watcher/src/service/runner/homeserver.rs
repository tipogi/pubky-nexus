use super::Cycle;
use crate::errors::EventProcessorError;
use crate::events::retry::RetryScheduler;
use crate::events::{Event, EventHandler};
use crate::service::indexer::{HsEventProcessor, NexusEventProcessor};
use crate::service::stats::{CycleReport, CycleStats};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::user::PkdnsHomeserverResolver;
use nexus_common::types::DynError;
use nexus_common::WatcherConfig;
use pubky_social_specs::PubkyId;
use pubky_watcher::FeedReader;
use std::sync::Arc;
use tokio::sync::watch::Receiver;
use tracing::debug;

pub struct HsEventProcessorRunner {
    /// See [WatcherConfig::events_limit]
    pub limit: u16,

    pub event_handler: Arc<dyn EventHandler>,
    /// Client the processors this runner builds poll events through
    pub client: Arc<dyn FeedReader>,
    /// PKDNS lookup the processors this runner builds resolve users' homeservers through
    pub resolver: Arc<dyn PkdnsHomeserverResolver>,
    pub shutdown_rx: Receiver<bool>,

    /// See [WatcherConfig::homeserver]
    pub primary_homeserver: PubkyId,

    /// Scheduler shared with every processor this runner builds
    pub retry_scheduler: Arc<RetryScheduler>,
}

impl HsEventProcessorRunner {
    /// Creates a runner configured by `config`, sharing the given client, resolver, handler
    /// and scheduler
    pub fn new(
        config: &WatcherConfig,
        client: Arc<dyn FeedReader>,
        resolver: Arc<dyn PkdnsHomeserverResolver>,
        event_handler: Arc<dyn EventHandler>,
        retry_scheduler: Arc<RetryScheduler>,
        shutdown_rx: Receiver<bool>,
    ) -> Self {
        Self {
            limit: config.events_limit,
            event_handler,
            client,
            resolver,
            shutdown_rx,
            primary_homeserver: config.homeserver.clone(),
            retry_scheduler,
        }
    }

    pub fn primary_homeserver(&self) -> &str {
        &self.primary_homeserver
    }
}

#[async_trait::async_trait]
impl Cycle<Event, EventProcessorError> for HsEventProcessorRunner {
    type Error = DynError;

    fn shutdown_rx(&self) -> Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Creates and returns a new event processor instance for the specified homeserver
    async fn build_run(&self, homeserver_id: &str) -> Result<Arc<NexusEventProcessor>, DynError> {
        let homeserver_id = PubkyId::try_from(homeserver_id)?;
        let homeserver = Homeserver::get_by_id(homeserver_id)
            .await?
            .ok_or("Homeserver not found")?;

        Ok(Arc::new(HsEventProcessor {
            homeserver,
            limit: self.limit,
            event_handler: self.event_handler.clone(),
            client: self.client.clone(),
            resolver: self.resolver.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            retry_scheduler: self.retry_scheduler.clone(),
            hs_mapping_cache: Default::default(),
        }))
    }

    async fn homeservers(&self) -> Result<Vec<String>, DynError> {
        Ok(vec![self.primary_homeserver.to_string()])
    }

    async fn after_cycle(&self, stats: CycleStats) -> CycleReport {
        for individual_run_stat in &stats.turns {
            let hs_id = &individual_run_stat.hs_pubkey;
            let duration = individual_run_stat.duration;
            let status = &individual_run_stat.status;
            debug!(homeserver = %hs_id, ?duration, ?status, "Primary homeserver run completed");
        }

        CycleReport(stats)
    }
}

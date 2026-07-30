use super::TEventProcessorRunner;
use crate::errors::EventProcessorError;
use crate::events::{DefaultEventHandler, DynEventHandler, Event};
use crate::events::retry::RetryScheduler;
use crate::service::indexer::{DynEventProcessor, HsEventProcessor};
use pubky_watcher::EventRetryScheduler;
use nexus_common::models::homeserver::Homeserver;
use nexus_common::types::DynError;
use nexus_common::WatcherConfig;
use pubky_app_specs::PubkyId;
use std::sync::Arc;
use tokio::sync::watch::Receiver;

pub struct HsEventProcessorRunner {
    /// See [WatcherConfig::events_limit]
    pub limit: u16,

    pub event_handler: Arc<DynEventHandler>,
    pub shutdown_rx: Receiver<bool>,

    /// See [WatcherConfig::homeserver]
    pub primary_homeserver: PubkyId,

    /// Scheduler shared with every processor this runner builds
    pub retry_scheduler: Arc<dyn EventRetryScheduler<Event, EventProcessorError> + Send + Sync>,
}

impl HsEventProcessorRunner {
    /// Creates a new instance from the provided configuration
    pub fn from_config(config: &WatcherConfig, shutdown_rx: Receiver<bool>) -> Self {
        Self {
            limit: config.events_limit,
            event_handler: Arc::new(DefaultEventHandler::from_config(config)),
            shutdown_rx,
            primary_homeserver: config.homeserver.clone(),
            retry_scheduler: Arc::new(RetryScheduler::from_config(config)),
        }
    }

    pub fn primary_homeserver(&self) -> &str {
        &self.primary_homeserver
    }
}

#[async_trait::async_trait]
impl TEventProcessorRunner<Event, EventProcessorError> for HsEventProcessorRunner {
    fn shutdown_rx(&self) -> Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Creates and returns a new event processor instance for the specified homeserver
    async fn build(&self, homeserver_id: &str) -> Result<Arc<DynEventProcessor>, DynError> {
        let homeserver_id = PubkyId::try_from(homeserver_id)?;
        let homeserver = Homeserver::get_by_id(homeserver_id)
            .await?
            .ok_or("Homeserver not found")?;

        Ok(Arc::new(HsEventProcessor {
            homeserver,
            limit: self.limit,
            event_handler: self.event_handler.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            retry_scheduler: self.retry_scheduler.clone(),
            hs_mapping_cache: Default::default(),
        }))
    }

    async fn pre_run(&self) -> Result<Vec<String>, DynError> {
        Ok(vec![self.primary_homeserver.to_string()])
    }
}

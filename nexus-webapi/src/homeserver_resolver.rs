use std::sync::Arc;

use nexus_common::models::error::{ModelError, ModelResult};
use nexus_common::models::user::UserHomeserverResolver;
use pubky_app_specs::PubkyId;
use pubky_watcher::{HomeserverResolver, PubkyConnectorResolver};

/// Adapts the generic pubky-watcher resolver to Nexus domain types.
pub struct PubkyHomeserverResolver {
    inner: PubkyConnectorResolver,
}

impl PubkyHomeserverResolver {
    pub fn new() -> Self {
        Self {
            inner: PubkyConnectorResolver,
        }
    }
}

impl Default for PubkyHomeserverResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl UserHomeserverResolver for PubkyHomeserverResolver {
    async fn resolve_homeserver_id(&self, user_id: &PubkyId) -> ModelResult<Option<String>> {
        let hs_pk = self
            .inner
            .resolve_homeserver(&user_id.to_public_key())
            .await
            .map_err(ModelError::from_generic)?;
        Ok(hs_pk.map(|pk| pk.into_inner().to_z32()))
    }
}

pub fn default_homeserver_resolver() -> Arc<dyn UserHomeserverResolver> {
    Arc::new(PubkyHomeserverResolver::new())
}

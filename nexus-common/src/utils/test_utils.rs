//! # Test Utilities
//!
//! Shared helpers for unit and integration tests.

use std::sync::Arc;

use pubky::{Keypair, PublicKey};
use pubky_app_specs::PubkyId;

use crate::models::error::ModelResult;
use crate::models::user::{UserHomeserverResolver, UserIngestor};

/// Generates a random public key.
pub fn random_pk() -> PublicKey {
    Keypair::random().public_key()
}

/// Generates a random z32-encoded public key, usable as a user or HS ID.
pub fn random_pubky_id() -> PubkyId {
    PubkyId::from(random_pk())
}

/// Resolver stub for tests that do not need PKDNS/DHT lookup.
pub struct MockUserHomeserverResolver {
    hs_id: Option<String>,
}

impl MockUserHomeserverResolver {
    pub fn new(hs_id: Option<String>) -> Self {
        Self { hs_id }
    }
}

#[async_trait::async_trait]
impl UserHomeserverResolver for MockUserHomeserverResolver {
    async fn resolve_homeserver_id(&self, _user_id: &PubkyId) -> ModelResult<Option<String>> {
        Ok(self.hs_id.clone())
    }
}

pub fn mock_homeserver_resolver(hs_id: Option<String>) -> Arc<dyn UserHomeserverResolver> {
    Arc::new(MockUserHomeserverResolver::new(hs_id))
}

/// Builds a user ingestor for tests with an empty HS blacklist.
pub fn default_ingestor_tests(resolver: Arc<dyn UserHomeserverResolver>) -> Arc<UserIngestor> {
    Arc::new(UserIngestor::new([], resolver))
}

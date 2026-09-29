//! # Test Utilities
//!
//! Shared helpers for unit and integration tests.

use std::sync::Arc;

use pubky::{Keypair, PublicKey};
use pubky_app_specs::PubkyId;
use pubky_watcher::{ClientResult, HomeserverResolver};

use crate::models::user::UserIngestor;

struct NoopHomeserverResolver;

#[async_trait::async_trait]
impl HomeserverResolver for NoopHomeserverResolver {
    async fn resolve_homeserver(&self, _user: &PublicKey) -> ClientResult<Option<PublicKey>> {
        Ok(None)
    }
}

/// Generates a random public key.
pub fn random_pk() -> PublicKey {
    Keypair::random().public_key()
}

/// Generates a random z32-encoded public key, usable as a user or HS ID.
pub fn random_pubky_id() -> PubkyId {
    PubkyId::from(random_pk())
}

/// Builds a user ingestor for tests with an empty HS blacklist.
pub fn default_ingestor_tests() -> Arc<UserIngestor> {
    Arc::new(UserIngestor::new([], Arc::new(NoopHomeserverResolver)))
}

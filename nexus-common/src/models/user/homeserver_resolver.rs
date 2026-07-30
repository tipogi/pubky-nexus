//! Homeserver resolution for [`super::UserIngestor`].
//!
//! [`UserHomeserverResolver`] is an injection seam: [`super::UserIngestor`] needs PKDNS/DHT
//! lookup but must not call [`pubky_watcher::PubkyConnector`] (or depend on `pubky-watcher`).
//! Wiring happens in each binary crate (`nexus-watcher`, `nexus-webapi`) that constructs an
//! ingestor. Tests use [`crate::utils::test_utils::MockUserHomeserverResolver`] instead of
//! hitting the network.

use pubky_app_specs::PubkyId;

use crate::models::error::ModelResult;

/// Resolves a user's published homeserver for ingestion and blacklist checks.
#[async_trait::async_trait]
pub trait UserHomeserverResolver: Send + Sync {
    /// Returns the z32 HS id if the user publishes one.
    ///
    /// Returns `None` if the user has no published HS or if `user_id` is an HS PK itself.
    async fn resolve_homeserver_id(&self, user_id: &PubkyId) -> ModelResult<Option<String>>;
}

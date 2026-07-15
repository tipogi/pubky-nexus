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

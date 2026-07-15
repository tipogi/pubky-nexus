//! PKDNS/DHT homeserver resolution via the shared pubky client.
//!
//! [`HomeserverResolver`] operates on [`pubky::PublicKey`] and [`super::ClientError`].
//! Nexus crates adapt this to `PubkyId` / `ModelResult` via `UserHomeserverResolver` in
//! `nexus-common`.

use super::{ClientResult, PubkyConnector};
use pubky::PublicKey;

/// Resolves a user's currently published homeserver from PKDNS/DHT.
///
/// Abstracted so resolver loops and ingestor wiring can use a mock in tests instead of the
/// network. Production code uses [`PubkyConnectorResolver`].
#[async_trait::async_trait]
pub trait HomeserverResolver: Send + Sync {
    /// Returns the HS published for `user_pk`, if any is currently published.
    async fn resolve_homeserver(&self, user_pk: &PublicKey) -> ClientResult<Option<PublicKey>>;
}

/// Production resolver backed by the shared [`PubkyConnector`].
pub struct PubkyConnectorResolver;

#[async_trait::async_trait]
impl HomeserverResolver for PubkyConnectorResolver {
    async fn resolve_homeserver(&self, user_pk: &PublicKey) -> ClientResult<Option<PublicKey>> {
        let pubky = PubkyConnector::get()?;
        Ok(pubky.get_homeserver_of(user_pk).await)
    }
}

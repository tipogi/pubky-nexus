use std::{pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use pubky::{Event, EventCursor, Method, Pubky, PubkyHttpClient, PublicKey, StatusCode};

use super::{ClientError, ClientResult};

const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn homeserver_events_url(homeserver: &PublicKey, cursor: EventCursor, limit: u16) -> String {
    format!(
        "https://{}/events/?cursor={}&limit={limit}",
        homeserver.z32(),
        cursor.id()
    )
}

/// Byte stream returned by Pubky HTTP operations.
pub type ResponseBody = Pin<Box<dyn Stream<Item = ClientResult<Bytes>> + Send + 'static>>;

/// Event stream returned by a homeserver `/events-stream` subscription.
pub type KeyEventStream = Pin<Box<dyn Stream<Item = ClientResult<Event>> + Send + 'static>>;

/// Transport-neutral HTTP response used by event and resource clients.
pub struct ClientResponse {
    pub status: StatusCode,
    pub content_length: Option<u64>,
    pub content_type: Option<String>,
    pub body: ResponseBody,
}

impl ClientResponse {
    fn from_reqwest(response: reqwest::Response) -> Self {
        let status = response.status();
        let content_length = response.content_length();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response.bytes_stream().map(|result| {
            result.map_err(|error| ClientError::TransportFailed {
                message: error.to_string(),
            })
        });

        Self {
            status,
            content_length,
            content_type,
            body: Box::pin(body),
        }
    }
}

/// Fetches one homeserver-wide `/events/` response.
#[async_trait]
pub trait HomeserverEventSource: Send + Sync + 'static {
    async fn fetch_homeserver_events(
        &self,
        homeserver: &PublicKey,
        cursor: EventCursor,
        limit: u16,
    ) -> ClientResult<ClientResponse>;
}

/// Opens one finite user `/events-stream`.
#[async_trait]
pub trait KeyEventSource: Send + Sync + 'static {
    async fn key_event_stream(
        &self,
        homeserver: &PublicKey,
        user: &PublicKey,
        cursor: EventCursor,
        limit: u16,
        path: &str,
    ) -> ClientResult<KeyEventStream>;
}

/// Reads a resource from Pubky public storage.
#[async_trait]
pub trait ResourceReader: Send + Sync + 'static {
    async fn get_resource(&self, uri: &str) -> ClientResult<ClientResponse>;
}

/// Resolves the homeserver currently published for a user.
#[async_trait]
pub trait HomeserverResolver: Send + Sync + 'static {
    async fn resolve_homeserver(&self, user: &PublicKey) -> ClientResult<Option<PublicKey>>;
}

#[async_trait]
impl<T> HomeserverEventSource for Arc<T>
where
    T: HomeserverEventSource + ?Sized,
{
    async fn fetch_homeserver_events(
        &self,
        homeserver: &PublicKey,
        cursor: EventCursor,
        limit: u16,
    ) -> ClientResult<ClientResponse> {
        (**self)
            .fetch_homeserver_events(homeserver, cursor, limit)
            .await
    }
}

#[async_trait]
impl<T> KeyEventSource for Arc<T>
where
    T: KeyEventSource + ?Sized,
{
    async fn key_event_stream(
        &self,
        homeserver: &PublicKey,
        user: &PublicKey,
        cursor: EventCursor,
        limit: u16,
        path: &str,
    ) -> ClientResult<KeyEventStream> {
        (**self)
            .key_event_stream(homeserver, user, cursor, limit, path)
            .await
    }
}

#[async_trait]
impl<T> ResourceReader for Arc<T>
where
    T: ResourceReader + ?Sized,
{
    async fn get_resource(&self, uri: &str) -> ClientResult<ClientResponse> {
        (**self).get_resource(uri).await
    }
}

#[async_trait]
impl<T> HomeserverResolver for Arc<T>
where
    T: HomeserverResolver + ?Sized,
{
    async fn resolve_homeserver(&self, user: &PublicKey) -> ClientResult<Option<PublicKey>> {
        (**self).resolve_homeserver(user).await
    }
}

/// Standard Pubky-backed implementation of all watcher client capabilities.
#[derive(Clone)]
pub struct WatcherClient {
    pubky: Arc<Pubky>,
}

impl WatcherClient {
    /// Builds a client for the public Pubky network.
    pub fn mainnet() -> ClientResult<Self> {
        Self::build(None)
    }

    /// Builds a client for a testnet relay.
    pub fn testnet(host: &str) -> ClientResult<Self> {
        Self::build(Some(host))
    }

    /// Wraps an existing SDK client, primarily for tests and embedding.
    pub fn from_pubky(pubky: Pubky) -> Self {
        Self {
            pubky: Arc::new(pubky),
        }
    }

    /// Wraps an existing shared SDK client.
    pub fn from_shared(pubky: Arc<Pubky>) -> Self {
        Self { pubky }
    }

    /// Returns the wrapped SDK client for operations outside watcher capabilities.
    pub fn sdk(&self) -> Arc<Pubky> {
        self.pubky.clone()
    }

    fn build(testnet_host: Option<&str>) -> ClientResult<Self> {
        let mut builder = PubkyHttpClient::builder();
        builder.request_timeout(HTTP_REQUEST_TIMEOUT);

        if let Some(host) = testnet_host {
            builder
                .testnet_with_host(host)
                .pkarr(|pkarr| pkarr.dht(|dht| dht.port(0)));
        }

        Ok(Self::from_pubky(Pubky::with_client(builder.build()?)))
    }
}

#[async_trait]
impl HomeserverEventSource for WatcherClient {
    async fn fetch_homeserver_events(
        &self,
        homeserver: &PublicKey,
        cursor: EventCursor,
        limit: u16,
    ) -> ClientResult<ClientResponse> {
        let url = homeserver_events_url(homeserver, cursor, limit);
        let response = self
            .pubky
            .client()
            .request(Method::GET, &url)
            .send()
            .await
            .map_err(|error| ClientError::TransportFailed {
                message: error.to_string(),
            })?;

        Ok(ClientResponse::from_reqwest(response))
    }
}

#[async_trait]
impl KeyEventSource for WatcherClient {
    async fn key_event_stream(
        &self,
        homeserver: &PublicKey,
        user: &PublicKey,
        cursor: EventCursor,
        limit: u16,
        path: &str,
    ) -> ClientResult<KeyEventStream> {
        let stream = self
            .pubky
            .event_stream_for(homeserver)
            .add_users([(user, Some(cursor))])?
            .limit(limit)
            .path(path)
            .subscribe()
            .await?
            .map(|result| {
                result.map_err(|error| ClientError::TransportFailed {
                    message: error.to_string(),
                })
            });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl ResourceReader for WatcherClient {
    async fn get_resource(&self, uri: &str) -> ClientResult<ClientResponse> {
        let response = self.pubky.public_storage().get(uri).await?;
        Ok(ClientResponse::from_reqwest(response))
    }
}

#[async_trait]
impl HomeserverResolver for WatcherClient {
    async fn resolve_homeserver(&self, user: &PublicKey) -> ClientResult<Option<PublicKey>> {
        Ok(self.pubky.get_homeserver_of(user).await)
    }
}

#[cfg(test)]
mod tests {
    use pubky::{EventCursor, Keypair};

    use super::homeserver_events_url;

    #[test]
    fn homeserver_url_uses_raw_z32_key() {
        let homeserver = Keypair::random().public_key();
        let url = homeserver_events_url(&homeserver, EventCursor::new(7), 50);

        assert_eq!(
            url,
            format!("https://{}/events/?cursor=7&limit=50", homeserver.z32())
        );
        assert!(!url.starts_with("https://pubky"));
    }
}

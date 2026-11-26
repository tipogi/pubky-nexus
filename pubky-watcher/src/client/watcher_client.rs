use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, Stream, StreamExt};
use pubky::pkarr::errors::ResolveError as PkarrResolveError;
use pubky::pkarr::{ResolvePolicy, SignedPacket};
use pubky::{EventCursor, Method, Pubky, PubkyHttpClient, PubkyResource, PublicKey};
use tracing::{error, trace, warn};

use super::fetch::{
    fetch_capped, read_stream_capped, MAX_ERROR_BODY, MAX_EVENTS_BODY, MAX_RESOURCE_SIZE,
};
use super::{ClientError, EventType, HsError, HsErrorKind, HsOp, PkdnsLookup, PkdnsLookupError};

/// The blob cap of a Client that sets none ([`WatcherClient::with_max_file_size`]): 100 MiB,
/// the Homeserver's own limit.
pub const DEFAULT_MAX_FILE_SIZE: u64 = 100 * (1 << 20);

/// How long a request of a Client that sets no timeout
/// ([`WatcherClientBuilder::request_timeout`]) may take, reading the response body included,
/// so that it can download a blob of [`DEFAULT_MAX_FILE_SIZE`].
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Waits between the retries of a rate-limited Read of an Event stream.
const READ_STREAM_429_BACKOFF_SECS: [u64; 3] = [1, 2, 3];

/// A Stream event: an Event read from an Event stream. Besides what every Event names, it
/// carries its User pubkey and its own Cursor.
/// Returned by [`StreamReader::read_stream`].
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// Whether the Event wrote or deleted the Resource ([`EventType`]).
    pub event_type: EventType,
    /// The Event's own Cursor: its position in the Event stream, after which the next Read
    /// resumes. Store it as an [`EventCursor`](pubky::EventCursor).
    pub cursor: u64,
    user_pubkey: PublicKey,
    uri: String,
}

impl StreamEvent {
    /// The Event `event_type` on `resource` at `cursor`; [`Self::user_pubkey`] is the User
    /// pubkey that owns the Resource.
    pub fn new(event_type: EventType, resource: &PubkyResource, cursor: u64) -> Self {
        Self {
            event_type,
            cursor,
            user_pubkey: resource.owner.clone(),
            uri: resource.to_pubky_url(),
        }
    }

    /// The User pubkey that owns [`Self::uri`], checked against the one whose Event stream
    /// the Event is on. Both come from one Resource, so they cannot disagree.
    pub fn user_pubkey(&self) -> &PublicKey {
        &self.user_pubkey
    }

    /// The Resource's `pubky://` URI, which [`Self::user_pubkey`] owns.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl From<pubky::Event> for StreamEvent {
    fn from(event: pubky::Event) -> Self {
        Self::new(event.event_type.into(), &event.resource, event.cursor.id())
    }
}

/// The Read of a Homeserver's Events feed (`/events/`), which covers every user it hosts
/// under one Cursor.
///
/// A failure is labelled with `hs_pubkey`, the Homeserver read.
/// [`WatcherClient`] implements it, and a Feed consumer
/// ([`Watcher::feed`](crate::Watcher::feed)) reads through it.
#[async_trait::async_trait]
pub trait FeedReader: Send + Sync + 'static {
    /// `GET /events/?cursor=&limit=`: the Batch after `cursor`, of at most `limit` Events.
    /// Returns its lines as the Homeserver sent them, the cursor line included; an empty body
    /// has none. A body over the cap for a Batch fails with [`HsErrorKind::TooLarge`].
    async fn read_feed(
        &self,
        hs_pubkey: &PublicKey,
        cursor: u64,
        limit: u16,
    ) -> Result<Vec<String>, HsError>;
}

/// The Read of a User pubkey's Event stream (`/events-stream`) on a Homeserver, with one
/// Cursor per User pubkey.
///
/// A failure is labelled with `hs_pubkey`, the Homeserver read.
/// [`WatcherClient`] implements it, and a Stream consumer
/// ([`Watcher::stream`](crate::Watcher::stream)) reads through it.
#[async_trait::async_trait]
pub trait StreamReader: Send + Sync + 'static {
    /// `GET /events-stream?user=<pk>:<cursor>&limit=&path=<path>`, not live: the Events of
    /// `user_pubkey` after `cursor` whose Resource path starts with `path` (e.g. `/pub/`).
    /// The Batch holds at most `limit` Events, each a [`StreamEvent`]. A rate-limited request
    /// is retried.
    async fn read_stream(
        &self,
        hs_pubkey: &PublicKey,
        user_pubkey: &PublicKey,
        cursor: u64,
        limit: u16,
        path: &str,
    ) -> Result<Vec<StreamEvent>, HsError>;
}

/// The Read of the Resource, or the blob, a `PUT` Event names, under a size cap so that one
/// large Resource cannot exhaust memory.
///
/// A failure is labelled with the User pubkey that owns the Resource.
/// [`WatcherClient`] implements it.
#[async_trait::async_trait]
pub trait ResourceReader: Send + Sync + 'static {
    /// The JSON Resource at `resource`, capped at 2 MiB, from the Homeserver its User
    /// pubkey's Homeserver record names. A larger one fails with [`HsErrorKind::TooLarge`].
    async fn read_resource(&self, resource: &PubkyResource) -> Result<Vec<u8>, HsError>;

    /// The blob at `resource`, capped at the Client's `max_file_size`
    /// ([`WatcherClient::with_max_file_size`], [`DEFAULT_MAX_FILE_SIZE`] by default), from the
    /// Homeserver its User pubkey's Homeserver record names.
    async fn read_blob(&self, resource: &PubkyResource) -> Result<Vec<u8>, HsError>;
}

/// The Client: the four Reads over the Pubky SDK, [`FeedReader`], [`StreamReader`],
/// [`ResourceReader`] and [`PkdnsLookup`]. It never writes. Cloning it shares the SDK
/// underneath.
#[derive(Clone)]
pub struct WatcherClient {
    pubky: Arc<Pubky>,
    max_file_size: u64,
}

/// Builds a [`WatcherClient`] from plain values, so that an Indexer passes its own settings
/// without setting up the SDK itself. Start it with [`WatcherClient::builder`].
///
/// ```no_run
/// use std::time::Duration;
/// use pubky_watcher::WatcherClient;
///
/// let client = WatcherClient::builder()
///     .testnet("localhost")
///     .request_timeout(Duration::from_secs(30))
///     .max_file_size(10 * (1 << 20))
///     .build()?;
/// # Ok::<_, pubky_watcher::ClientError>(())
/// ```
#[derive(Debug, Clone)]
#[must_use = "a builder does nothing until `build` is called"]
pub struct WatcherClientBuilder {
    testnet_host: Option<String>,
    request_timeout: Duration,
    max_file_size: u64,
}

/// A Client for the public Pubky network, with [`DEFAULT_REQUEST_TIMEOUT`] and
/// [`DEFAULT_MAX_FILE_SIZE`].
impl Default for WatcherClientBuilder {
    fn default() -> Self {
        Self {
            testnet_host: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        }
    }
}

impl WatcherClientBuilder {
    /// Builds for the Pubky testnet whose relay and Homeservers run on `host` (e.g.
    /// `"localhost"`), instead of the public network. [`WatcherClient::testnet`] is this
    /// with every other setting left at its default.
    pub fn testnet(mut self, host: impl Into<String>) -> Self {
        self.testnet_host = Some(host.into());
        self
    }

    /// How long a request may take, reading the response body included
    /// ([`DEFAULT_REQUEST_TIMEOUT`] by default). A Read that runs out of it fails with
    /// [`HsErrorKind::Timeout`].
    pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Reads blobs of at most `max_file_size` bytes ([`DEFAULT_MAX_FILE_SIZE`] by default),
    /// as [`WatcherClient::with_max_file_size`] does on a built Client.
    pub fn max_file_size(mut self, max_file_size: u64) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    /// Sets up the SDK and builds the [`WatcherClient`] over it.
    /// Fails with a [`ClientError`] when the SDK cannot build its HTTP or PKARR client.
    pub fn build(self) -> Result<WatcherClient, ClientError> {
        // TODO: set pkarr's `minimum_ttl` / `maximum_ttl` and its DHT and relay request
        // timeouts explicitly; `request_timeout` bounds only the HTTP client, not pkarr.
        let mut builder = PubkyHttpClient::builder();
        builder.request_timeout(self.request_timeout);
        if let Some(host) = &self.testnet_host {
            builder
                .testnet_with_host(host)
                // An ephemeral DHT port, so the Client does not compete with a local testnet
                // for its port
                .pkarr(|p| {
                    p.dht(|c| {
                        c.port = Some(0);
                        c
                    })
                });
        }
        let pubky = Pubky::with_client(builder.build().map_err(ClientError::build)?);
        Ok(WatcherClient::from_pubky(pubky).with_max_file_size(self.max_file_size))
    }
}

/// Names the blob cap; the SDK underneath is not printed.
impl std::fmt::Debug for WatcherClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatcherClient")
            .field("max_file_size", &self.max_file_size)
            .finish_non_exhaustive()
    }
}

impl WatcherClient {
    /// A [`WatcherClientBuilder`], to choose the network, the request timeout and the blob
    /// cap. It builds a Client for the public Pubky network unless told otherwise.
    pub fn builder() -> WatcherClientBuilder {
        WatcherClientBuilder::default()
    }

    /// A Client for the public Pubky network, with the builder's defaults
    /// ([`Self::builder`]).
    /// Fails with a [`ClientError`].
    pub fn mainnet() -> Result<Self, ClientError> {
        Self::builder().build()
    }

    /// A Client for the Pubky testnet whose relay and Homeservers run on `host` (e.g.
    /// `"localhost"`), with the builder's defaults ([`Self::builder`]).
    /// Fails with a [`ClientError`].
    pub fn testnet(host: &str) -> Result<Self, ClientError> {
        Self::builder().testnet(host).build()
    }

    /// A Client over an SDK you set up, for a setting [`Self::builder`] does not offer.
    /// The same as [`Self::from_shared`] over a new [`Arc`].
    pub fn from_pubky(pubky: Pubky) -> Self {
        Self::from_shared(Arc::new(pubky))
    }

    /// A Client over an SDK you share with the rest of your Indexer.
    /// Blobs are capped at [`DEFAULT_MAX_FILE_SIZE`] until [`Self::with_max_file_size`].
    pub fn from_shared(pubky: Arc<Pubky>) -> Self {
        Self {
            pubky,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        }
    }

    /// Reads blobs of at most `max_file_size` bytes ([`DEFAULT_MAX_FILE_SIZE`] by default).
    pub fn with_max_file_size(mut self, max_file_size: u64) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    /// The SDK underneath, for what this Client does not read.
    /// It is the one given to [`Self::from_pubky`] or [`Self::from_shared`], or the one
    /// [`WatcherClientBuilder::build`] built.
    pub fn sdk(&self) -> &Arc<Pubky> {
        &self.pubky
    }

    /// GETs `resource`, returning the response for the caller to read under its cap.
    ///
    /// The SDK reads at most 4 KiB of a non-2xx body (its default `max_error_body_bytes`),
    /// the same amount [`HsErrorKind::from_sdk`] keeps.
    async fn get(&self, resource: &PubkyResource) -> Result<reqwest::Response, HsErrorKind> {
        // TODO: read from the Homeserver that delivered the Event, through `request_async`
        // with a Homeserver-addressed storage URL, instead of the one the User pubkey's
        // Homeserver record names.
        self.pubky
            .public_storage()
            .get(resource)
            .await
            .map_err(HsErrorKind::from_sdk)
    }

    /// One Read of an Event stream, without retries.
    async fn fetch_stream_events(
        &self,
        hs_pubkey: &PublicKey,
        user_pubkey: &PublicKey,
        cursor: u64,
        limit: u16,
        path: &str,
    ) -> Result<Vec<StreamEvent>, HsErrorKind> {
        // We are building the stream without the live flag, so it performs an HTTP GET and closes.
        // See rustdoc of EventStreamBuilder::live()
        let mut stream = self
            .pubky
            .event_stream_for(hs_pubkey)
            .add_users(vec![(user_pubkey, Some(EventCursor::new(cursor)))])
            .map_err(HsErrorKind::from_sdk)?
            .limit(limit)
            .path(path)
            .subscribe()
            .await
            .map_err(HsErrorKind::from_sdk)
            .inspect_err(|e| error!(error = %e, "Failed to subscribe to event stream"))?;

        read_stream_events(&mut stream, limit, hs_pubkey, user_pubkey).await
    }
}

/// Reads at most `limit` Events of an Event stream's response. A Homeserver that holds
/// the connection open once it has sent them is not waited for; one that has already sent
/// more than asked is logged, and the excess dropped. Pubky uses SSE framing regardless of
/// `EventStreamBuilder::live()`, so a failure after the response headers arrives as a
/// stream item, which fails the Read as a transport error.
async fn read_stream_events<S>(
    stream: &mut S,
    limit: u16,
    hs_pubkey: &PublicKey,
    user_pubkey: &PublicKey,
) -> Result<Vec<StreamEvent>, HsErrorKind>
where
    S: Stream<Item = pubky::Result<pubky::Event>> + Unpin,
{
    let limit = limit as usize;
    let mut events = Vec::with_capacity(limit);
    while events.len() < limit {
        let Some(result) = stream.next().await else {
            break;
        };
        let event = result.map_err(HsErrorKind::transport)?;
        events.push(StreamEvent::from(event));
    }

    // Checked once, never awaited: only an Event that has already arrived counts.
    let sent_more =
        events.len() == limit && matches!(stream.next().now_or_never(), Some(Some(Ok(_))));
    if sent_more {
        error!(
            user_pk = %user_pubkey.z32(),
            hs_pk = %hs_pubkey.z32(),
            limit,
            "Event stream returned more than the requested limit; ignoring the excess"
        );
    }

    Ok(events)
}

/// The PKDNS lookup behind `Pubky::get_homeserver_of`, which folds a missing packet and a
/// packet naming no Homeserver into one `None` and fails on a malformed `_pubky` record.
#[async_trait::async_trait]
impl PkdnsLookup for WatcherClient {
    async fn lookup_homeserver_record(
        &self,
        user_pubkey: &PublicKey,
    ) -> Result<Option<SignedPacket>, PkdnsLookupError> {
        match self
            .pubky
            .client()
            .pkarr()
            .resolve(user_pubkey, ResolvePolicy::CacheFirst)
            .await
        {
            Ok(packet) => Ok(Some(packet)),
            Err(PkarrResolveError::NotFound) => Ok(None),
            Err(e) => Err(PkdnsLookupError::new(e)),
        }
    }
}

#[async_trait::async_trait]
impl FeedReader for WatcherClient {
    async fn read_feed(
        &self,
        hs_pubkey: &PublicKey,
        cursor: u64,
        limit: u16,
    ) -> Result<Vec<String>, HsError> {
        // TODO: move to `request_async`, which falls back to the Homeserver's ICANN domain
        // when its direct endpoint is unreachable.
        let send = self
            .pubky
            .client()
            .request(Method::GET, &events_url(hs_pubkey, cursor, limit))
            .send();
        read_feed_with(hs_pubkey, send).await
    }
}

#[async_trait::async_trait]
impl StreamReader for WatcherClient {
    async fn read_stream(
        &self,
        hs_pubkey: &PublicKey,
        user_pubkey: &PublicKey,
        cursor: u64,
        limit: u16,
        path: &str,
    ) -> Result<Vec<StreamEvent>, HsError> {
        read_stream_with(hs_pubkey, user_pubkey, || {
            self.fetch_stream_events(hs_pubkey, user_pubkey, cursor, limit, path)
        })
        .await
    }
}

#[async_trait::async_trait]
impl ResourceReader for WatcherClient {
    async fn read_resource(&self, resource: &PubkyResource) -> Result<Vec<u8>, HsError> {
        read_resource_with(resource, self.get(resource)).await
    }

    async fn read_blob(&self, resource: &PubkyResource) -> Result<Vec<u8>, HsError> {
        read_blob_with(resource, self.max_file_size, self.get(resource)).await
    }
}

/// Reads the lines of the Events feed request `send` makes, labelling a failure as a Read of
/// `hs_pubkey`'s Events feed.
async fn read_feed_with(
    hs_pubkey: &PublicKey,
    send: impl Future<Output = Result<reqwest::Response, reqwest::Error>>,
) -> Result<Vec<String>, HsError> {
    let error = |kind| HsError::new(hs_pubkey, HsOp::ReadFeed, kind);
    let response = send
        .await
        .map_err(|e| error(HsErrorKind::from_reqwest(&e)))?;
    event_lines_from_response(response).await.map_err(error)
}

/// Reads a User pubkey's Event stream with `fetch`, retrying while rate limited, and labels
/// a failure as a Read of the Event stream, which decides that it stops the Homeserver's Run.
async fn read_stream_with<F, Fut>(
    hs_pubkey: &PublicKey,
    user_pubkey: &PublicKey,
    fetch: F,
) -> Result<Vec<StreamEvent>, HsError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Vec<StreamEvent>, HsErrorKind>>,
{
    retry_rate_limited(user_pubkey, fetch)
        .await
        .map_err(|kind| HsError::new(hs_pubkey, HsOp::ReadStream, kind))
}

/// Reads the body of a Resource's response, at most [`MAX_RESOURCE_SIZE`], labelling a
/// failure as a Read of a Resource, which keeps it one Event's failure.
async fn read_resource_with(
    resource: &PubkyResource,
    fetch: impl Future<Output = Result<reqwest::Response, HsErrorKind>>,
) -> Result<Vec<u8>, HsError> {
    read_capped(fetch, MAX_RESOURCE_SIZE as u64)
        .await
        .map_err(|kind| owner_error(resource, HsOp::ReadResource, kind))
}

/// Reads the body of a blob's response, at most `max` bytes, labelling a failure as a Read
/// of a blob, which keeps it one Event's failure.
async fn read_blob_with(
    resource: &PubkyResource,
    max: u64,
    fetch: impl Future<Output = Result<reqwest::Response, HsErrorKind>>,
) -> Result<Vec<u8>, HsError> {
    read_capped(fetch, max)
        .await
        .map_err(|kind| owner_error(resource, HsOp::ReadBlob, kind))
}

async fn read_capped(
    fetch: impl Future<Output = Result<reqwest::Response, HsErrorKind>>,
    max: u64,
) -> Result<Vec<u8>, HsErrorKind> {
    fetch_capped(fetch.await?, max).await
}

/// A failed Read of a Resource or a blob, labelled with its User pubkey: nothing is charged
/// to a Homeserver yet, and a PKDNS lookup of the one that User pubkey publishes would cost
/// a PKARR query on every failure.
fn owner_error(resource: &PubkyResource, op: HsOp, kind: HsErrorKind) -> HsError {
    HsError::new(&resource.owner, op, kind)
}

/// Calls `fetch` and, while it is rate limited, again after each of
/// [`READ_STREAM_429_BACKOFF_SECS`]. Returns the last 429 once they run out.
async fn retry_rate_limited<T, F, Fut>(
    user_pubkey: &PublicKey,
    mut fetch: F,
) -> Result<T, HsErrorKind>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, HsErrorKind>>,
{
    let mut backoffs = READ_STREAM_429_BACKOFF_SECS.into_iter();
    loop {
        let kind = match fetch().await {
            Ok(value) => return Ok(value),
            Err(kind) => kind,
        };
        let (HsErrorKind::RateLimited(_), Some(backoff_secs)) = (&kind, backoffs.next()) else {
            return Err(kind);
        };

        warn!(
            user_id = %user_pubkey.z32(), retry_after_secs = backoff_secs,
            "Homeserver rate-limited user event fetch; retrying",
        );
        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
    }
}

/// The URL of `hs_pubkey`'s Events feed (`/events/`), from `cursor` on, for at most `limit`
/// Events.
fn events_url(hs_pubkey: &PublicKey, cursor: u64, limit: u16) -> String {
    format!(
        "https://{}/events/?cursor={cursor}&limit={limit}",
        hs_pubkey.z32()
    )
}

/// Turns an Events feed's response into the lines it carries.
///
/// The status is checked before the body is read: an error page has lines and
/// no `cursor:` line, so parsed as Event lines it would be rejected as a stalled
/// Cursor. A non-2xx instead fails the Read with the status and a capped body.
async fn event_lines_from_response(
    response: reqwest::Response,
) -> Result<Vec<String>, HsErrorKind> {
    let status = response.status();
    if !status.is_success() {
        let (body, _) = read_stream_capped(response.bytes_stream(), MAX_ERROR_BODY)
            .await
            .unwrap_or_default();
        return Err(HsErrorKind::from_status(status, &body));
    }

    let (buf, exceeded) = read_stream_capped(response.bytes_stream(), MAX_EVENTS_BODY)
        .await
        .map_err(|e| HsErrorKind::from_reqwest(&e))?;
    if exceeded {
        return Err(HsErrorKind::TooLarge {
            got: buf.len() as u64,
            max: MAX_EVENTS_BODY as u64,
        });
    }

    let lines: Vec<String> = String::from_utf8_lossy(&buf)
        .trim()
        .lines()
        .map(String::from)
        .collect();
    trace!(?lines, "Homeserver response lines");

    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::super::error::transport_error;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Instant;

    const HS_PUBKEY: &str = "1hb71xx9km3f4pw5izsy1gn19ff1uuuqonw4mcygzobwkryujoiy";

    fn hs_pubkey() -> PublicKey {
        PublicKey::try_from_z32(HS_PUBKEY).expect("valid test key")
    }

    fn response(status: u16, body: impl Into<reqwest::Body>) -> reqwest::Response {
        let response = http::Response::builder()
            .status(status)
            .body(body.into())
            .expect("valid response");
        reqwest::Response::from(response)
    }

    // --- WatcherClientBuilder ---

    #[tokio::test]
    async fn builder_carries_the_blob_cap_to_the_client() {
        let client = WatcherClient::builder()
            .testnet("localhost")
            .max_file_size(7)
            .build()
            .expect("a testnet client builds");

        assert_eq!(client.max_file_size, 7);
    }

    #[tokio::test]
    async fn shortcut_client_keeps_the_default_blob_cap() {
        let client = WatcherClient::testnet("localhost").expect("a testnet client builds");

        assert_eq!(client.max_file_size, DEFAULT_MAX_FILE_SIZE);
    }

    /// A server that accepts the connection and never answers: the request ends at the
    /// builder's timeout, far below [`DEFAULT_REQUEST_TIMEOUT`].
    #[tokio::test]
    async fn builder_request_timeout_bounds_a_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free local port");
        let url = format!("http://{}/", listener.local_addr().expect("bound"));
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let client = WatcherClient::builder()
            .testnet("localhost")
            .request_timeout(Duration::from_millis(200))
            .build()
            .expect("a testnet client builds");

        let request = client
            .sdk()
            .client()
            .request(reqwest::Method::GET, &url)
            .send();
        let error = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("the request ends at the builder's timeout")
            .expect_err("the server never answers");
        server.abort();

        let kind = HsErrorKind::from_reqwest(&error);
        assert!(matches!(kind, HsErrorKind::Timeout), "{kind:?}");
    }

    // --- events_url ---

    #[test]
    fn events_url_carries_the_cursor_and_limit() {
        assert_eq!(
            events_url(&hs_pubkey(), 42, 17),
            format!("https://{HS_PUBKEY}/events/?cursor=42&limit=17")
        );
    }

    // --- read_feed_with ---

    /// The incident shape: a proxy error page. Before the status check this
    /// read as "Events with no cursor line" and was counted as a stalled Cursor.
    #[tokio::test]
    async fn error_page_fails_the_feed_read_with_status_and_body() {
        let html = "<html>\n<head><title>502 Bad Gateway</title></head>\n<body>bad gateway</body>\n</html>";

        let error = read_feed_with(&hs_pubkey(), async { Ok(response(502, html)) })
            .await
            .unwrap_err();

        assert_eq!(error.hs, HS_PUBKEY);
        assert_eq!(error.op, HsOp::ReadFeed);
        assert!(
            matches!(error.kind, HsErrorKind::ServerError(502, _)),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains(HS_PUBKEY), "names the HS: {message}");
        assert!(
            message.contains("HTTP 502"),
            "carries the status: {message}"
        );
        assert!(
            message.contains("502 Bad Gateway"),
            "carries the body: {message}"
        );
    }

    /// A Read that gets no response is labelled as a Read of the Homeserver's Events feed
    /// too, as a transport failure.
    #[tokio::test]
    async fn unanswered_feed_read_is_a_transport_failure_of_the_homeserver() {
        let error = read_feed_with(&hs_pubkey(), async { Err(transport_error().await) })
            .await
            .unwrap_err();

        assert_eq!(error.hs, HS_PUBKEY);
        assert_eq!(error.op, HsOp::ReadFeed);
        assert!(matches!(error.kind, HsErrorKind::Transport(_)), "{error:?}");
    }

    // --- event_lines_from_response ---

    /// Each status lands on its own kind, so a 429 is recognisable to callers
    /// that back off on rate limiting.
    #[tokio::test]
    async fn non_2xx_statuses_are_classified() {
        for status in [404u16, 429, 503, 400] {
            let kind = event_lines_from_response(response(status, "nope"))
                .await
                .unwrap_err();

            let classified_right = matches!(
                (status, &kind),
                (404, HsErrorKind::NotFound(_))
                    | (429, HsErrorKind::RateLimited(_))
                    | (503, HsErrorKind::ServerError(503, _))
                    | (400, HsErrorKind::Rejected(400, _))
            );
            assert!(classified_right, "HTTP {status} classified as {kind:?}");
        }
    }

    /// An oversized error body is truncated into the message, not rejected as
    /// a size violation: the size cap exists for Batches.
    #[tokio::test]
    async fn error_body_is_capped() {
        let body = "x".repeat(MAX_ERROR_BODY * 4);

        let kind = event_lines_from_response(response(500, body))
            .await
            .unwrap_err();

        let message = kind.to_string();
        assert!(matches!(kind, HsErrorKind::ServerError(500, _)), "{kind:?}");
        assert!(message.ends_with("… (truncated)"), "{message}");
        assert!(message.len() < MAX_ERROR_BODY * 2, "{}", message.len());
    }

    /// A non-2xx response whose body cannot be read still fails on the status.
    #[tokio::test]
    async fn unreadable_error_body_still_fails_on_status() {
        let body = reqwest::Body::wrap_stream(futures::stream::iter(vec![Err::<bytes::Bytes, _>(
            std::io::Error::other("connection reset"),
        )]));

        let kind = event_lines_from_response(response(502, body))
            .await
            .unwrap_err();

        assert!(matches!(kind, HsErrorKind::ServerError(502, _)), "{kind:?}");
    }

    /// The happy path is unchanged: a 2xx body is split into lines as before.
    #[tokio::test]
    async fn success_body_is_split_into_lines() {
        let body = "PUT pubky://a/pub/x\nDEL pubky://b/pub/y\ncursor: 42\n";

        let lines = event_lines_from_response(response(200, body))
            .await
            .unwrap();

        assert_eq!(
            lines,
            vec![
                "PUT pubky://a/pub/x".to_string(),
                "DEL pubky://b/pub/y".to_string(),
                "cursor: 42".to_string(),
            ]
        );
    }

    /// An idle Homeserver answers 2xx with nothing in it.
    #[tokio::test]
    async fn empty_success_body_is_no_events() {
        let lines = event_lines_from_response(response(200, "")).await.unwrap();

        assert!(lines.is_empty());
    }

    /// A 2xx body over the cap for a Batch is still a size violation.
    #[tokio::test]
    async fn oversized_success_body_is_rejected() {
        let body = "x".repeat(MAX_EVENTS_BODY + 1);

        let kind = event_lines_from_response(response(200, body))
            .await
            .unwrap_err();

        assert!(matches!(kind, HsErrorKind::TooLarge { .. }), "{kind:?}");
    }

    // --- read_stream_events ---

    fn stream_event(cursor: u64) -> pubky::Result<pubky::Event> {
        let owner = pubky::Keypair::random().public_key();
        Ok(pubky::Event {
            event_type: pubky::EventType::Delete,
            resource: PubkyResource::new(owner, "/pub/pubky.app/profile.json").expect("valid path"),
            cursor: EventCursor::new(cursor),
        })
    }

    /// The Read ends once it holds exactly `limit` Events: a Homeserver that then holds the
    /// connection open is neither waited for nor logged. Time is paused, so a Read that
    /// waits fails at once.
    #[tokio::test(start_paused = true)]
    #[tracing_test::traced_test]
    async fn stream_read_ends_at_the_limit_without_waiting_for_more() {
        let user = pubky::Keypair::random().public_key();
        let mut stream = futures::stream::iter([stream_event(1), stream_event(2)])
            .chain(futures::stream::pending());

        let events = tokio::time::timeout(
            Duration::from_secs(1),
            read_stream_events(&mut stream, 2, &hs_pubkey(), &user),
        )
        .await
        .expect("the read must end at the limit")
        .unwrap();

        let cursors: Vec<u64> = events.iter().map(|event| event.cursor).collect();
        assert_eq!(cursors, [1, 2]);
        assert!(!logs_contain("more than the requested limit"));
    }

    /// A full Batch is normal: a Homeserver that sends exactly `limit` Events and ends the
    /// stream is not logged.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn stream_events_exactly_at_the_limit_are_not_logged() {
        let user = pubky::Keypair::random().public_key();
        let mut stream = futures::stream::iter([stream_event(1), stream_event(2)]);

        let events = read_stream_events(&mut stream, 2, &hs_pubkey(), &user)
            .await
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(!logs_contain("more than the requested limit"));
    }

    /// A Homeserver that has already sent more than the `limit` asked for is logged, and
    /// only the Events asked for are returned.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn stream_events_past_the_limit_are_logged_and_dropped() {
        let user = pubky::Keypair::random().public_key();
        let mut stream = futures::stream::iter([stream_event(1), stream_event(2), stream_event(3)]);

        let events = read_stream_events(&mut stream, 2, &hs_pubkey(), &user)
            .await
            .unwrap();

        let cursors: Vec<u64> = events.iter().map(|event| event.cursor).collect();
        assert_eq!(cursors, [1, 2]);
        assert!(logs_contain("more than the requested limit"));
    }

    /// A failure after the response headers fails the Read as a transport error, which
    /// aborts the Homeserver's Run.
    #[tokio::test]
    async fn event_stream_error_is_a_transport_failure() {
        let error = pubky::Error::from(pubky::errors::RequestError::Validation {
            message: "SSE stream error: unexpected EOF".into(),
        });
        let user = pubky::Keypair::random().public_key();
        let mut stream = futures::stream::iter([stream_event(1), Err(error)]);

        let kind = read_stream_events(&mut stream, 10, &hs_pubkey(), &user)
            .await
            .unwrap_err();

        assert!(matches!(kind, HsErrorKind::Transport(_)), "{kind:?}");
    }

    // --- read_stream_with ---

    /// A 5xx or a transport failure reading an Event stream is labelled as that Read's,
    /// which is what makes it stop the Homeserver's Run, unlike the same failure reading a
    /// Resource.
    #[tokio::test]
    async fn event_stream_failures_are_labelled_as_the_stream_request() {
        let user = pubky::Keypair::random().public_key();
        for kind in [
            HsErrorKind::ServerError(530, "".into()),
            HsErrorKind::transport("connection reset"),
        ] {
            let error = read_stream_with(&hs_pubkey(), &user, || {
                let kind = kind.clone();
                async move { Err(kind) }
            })
            .await
            .unwrap_err();

            assert_eq!(error.hs, HS_PUBKEY);
            assert_eq!(error.op, HsOp::ReadStream);
            assert!(
                matches!(
                    error.kind,
                    HsErrorKind::ServerError(530, _) | HsErrorKind::Transport(_)
                ),
                "{error:?}"
            );
        }
    }

    /// A rate-limited Read of an Event stream is retried, and only a 429 that outlasts the
    /// retries stops the Homeserver's Run.
    #[tokio::test(start_paused = true)]
    async fn stream_reads_retry_a_rate_limited_request() {
        let user = pubky::Keypair::random().public_key();
        let fetch_failing = |failures: usize, calls: &AtomicUsize| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            let outcome = if call < failures {
                Err(rate_limited())
            } else {
                Ok(vec![])
            };
            async move { outcome }
        };

        let calls = AtomicUsize::new(0);
        let result = read_stream_with(&hs_pubkey(), &user, || fetch_failing(2, &calls)).await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let calls = AtomicUsize::new(0);
        let error = read_stream_with(&hs_pubkey(), &user, || fetch_failing(usize::MAX, &calls))
            .await
            .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(error.op, HsOp::ReadStream);
        assert!(
            matches!(error.kind, HsErrorKind::RateLimited(_)),
            "{error:?}"
        );
    }

    // --- read_resource_with / read_blob_with ---

    /// A 5xx or a transport failure reading a Resource or a blob, which any user can point
    /// at any host, is labelled as that Read's, with its User pubkey, which is what keeps it
    /// one Event's failure.
    #[tokio::test]
    async fn resource_and_blob_failures_are_labelled_as_the_fetch() {
        let owner = pubky::Keypair::random().public_key();
        let resource: PubkyResource = format!("pubky://{}/pub/pubky.app/files/0001", owner.z32())
            .parse()
            .expect("valid resource");
        let failures = || {
            [
                HsErrorKind::ServerError(500, "".into()),
                HsErrorKind::transport("connection refused"),
            ]
        };

        // The Read fails before any body is read, so the blob cap plays no part.
        let unused_cap = 1024;
        let mut errors = Vec::new();
        for kind in failures() {
            errors.push(read_resource_with(&resource, async { Err(kind) }).await);
        }
        for kind in failures() {
            errors.push(read_blob_with(&resource, unused_cap, async { Err(kind) }).await);
        }

        let ops = [
            HsOp::ReadResource,
            HsOp::ReadResource,
            HsOp::ReadBlob,
            HsOp::ReadBlob,
        ];
        for (error, op) in errors.into_iter().zip(ops) {
            let error = error.unwrap_err();
            assert_eq!(error.hs, owner.z32());
            assert_eq!(error.op, op);
            assert!(
                matches!(
                    error.kind,
                    HsErrorKind::ServerError(500, _) | HsErrorKind::Transport(_)
                ),
                "{error:?}"
            );
        }
    }

    /// A JSON Resource is read up to [`MAX_RESOURCE_SIZE`] bytes and no further.
    #[tokio::test]
    async fn resources_are_capped_at_max_resource_size() {
        let owner = pubky::Keypair::random().public_key();
        let resource: PubkyResource = format!("pubky://{}/pub/pubky.app/posts/0001", owner.z32())
            .parse()
            .expect("valid resource");

        let at_cap = response(200, vec![0u8; MAX_RESOURCE_SIZE]);
        let body = read_resource_with(&resource, async { Ok(at_cap) })
            .await
            .expect("a body at the cap is accepted");
        assert_eq!(body.len(), MAX_RESOURCE_SIZE);

        let over_cap = response(200, vec![0u8; MAX_RESOURCE_SIZE + 1]);
        let error = read_resource_with(&resource, async { Ok(over_cap) })
            .await
            .unwrap_err();
        assert!(
            matches!(error.kind, HsErrorKind::TooLarge { max, .. } if max == MAX_RESOURCE_SIZE as u64),
            "{error:?}"
        );
    }

    // --- retry_rate_limited ---

    /// Calls `retry_rate_limited` over a fetch that fails with `failures` in order,
    /// then succeeds, and returns its result and the number of calls it made.
    async fn retry_over(failures: Vec<HsErrorKind>) -> (Result<(), HsErrorKind>, usize) {
        let user = pubky::Keypair::random().public_key();
        let calls = AtomicUsize::new(0);

        let result = retry_rate_limited(&user, || {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            let outcome = failures.get(call).cloned().map_or(Ok(()), Err);
            async move { outcome }
        })
        .await;

        (result, calls.load(Ordering::SeqCst))
    }

    fn rate_limited() -> HsErrorKind {
        HsErrorKind::RateLimited("slow down".into())
    }

    /// A 429 is retried after 1 s, then 2 s, and the fetch succeeds on the third call.
    #[tokio::test(start_paused = true)]
    async fn rate_limited_fetch_is_retried_with_backoff() {
        let started = Instant::now();

        let (result, calls) = retry_over(vec![rate_limited(), rate_limited()]).await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(calls, 3);
        assert_eq!(started.elapsed(), Duration::from_secs(1 + 2));
    }

    /// Once the three retries are used up, the last 429 is returned.
    #[tokio::test(start_paused = true)]
    async fn rate_limited_fetch_gives_up_after_three_retries() {
        let (result, calls) = retry_over(vec![rate_limited(); 5]).await;

        assert!(
            matches!(result, Err(HsErrorKind::RateLimited(_))),
            "{result:?}"
        );
        assert_eq!(calls, 4, "the first call and three retries");
    }

    /// Any other failure is returned at once.
    #[tokio::test(start_paused = true)]
    async fn other_failures_are_not_retried() {
        let (result, calls) = retry_over(vec![HsErrorKind::ServerError(530, String::new())]).await;

        assert!(
            matches!(result, Err(HsErrorKind::ServerError(530, _))),
            "{result:?}"
        );
        assert_eq!(calls, 1);
    }
}

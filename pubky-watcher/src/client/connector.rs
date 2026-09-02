//! Process-wide [pubky](https://crates.io/crates/pubky) SDK client.
//!
//! [`PubkyConnector`] holds one [`pubky::Pubky`] for the process. It is not a
//! homeserver.

// TODO: Decide public name — `PubkyConnector` is Nexus legacy; consider `SharedClient` (see rename-pubky-connector).
use super::{ClientError, ClientResult};
use pubky::{Pubky, PubkyHttpClient};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::debug;

static PUBKY_SINGLETON: OnceCell<Arc<Pubky>> = OnceCell::const_new();

/// Native HTTP timeout, including response-body reads but not PKARR/DHT operations.
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-wide [pubky](https://crates.io/crates/pubky) SDK client ([`pubky::Pubky`]).
///
/// The first successful [`Self::initialise`] or [`Self::init_from`] wins; later
/// calls keep the existing instance.
pub struct PubkyConnector;

impl PubkyConnector {
    /// Builds the SDK client and stores it if none is set yet.
    ///
    /// Pass `None` for mainnet or `Some(host)` for testnet. Native HTTP requests
    /// use a 30-second timeout; testnet clients bind their local DHT socket to an
    /// ephemeral port. A failed attempt can be retried.
    pub async fn initialise(testnet_host: Option<&str>) -> ClientResult<()> {
        PUBKY_SINGLETON
            .get_or_try_init(|| async {
                let mode = testnet_host
                    .map(|host| format!("testnet with host '{host}'"))
                    .unwrap_or_else(|| "mainnet".to_string());
                debug!(
                    ?HTTP_REQUEST_TIMEOUT,
                    "Initialising Pubky singleton in {mode} mode"
                );

                let mut client_builder = PubkyHttpClient::builder();
                client_builder.request_timeout(HTTP_REQUEST_TIMEOUT);

                if let Some(host) = testnet_host {
                    client_builder
                        .testnet_with_host(host)
                        // Avoid competing with `StaticTestnet` for DHT port 6881.
                        .pkarr(|p| p.dht(|d| d.port(0)));
                }

                let client = client_builder.build()?;
                Ok(Arc::new(Pubky::with_client(client)))
            })
            .await
            .map(|_| ())
    }

    /// Returns the shared [`pubky::Pubky`] client.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::NotInitialized`] if [`Self::initialise`] or
    /// [`Self::init_from`] has not succeeded yet.
    pub fn get() -> ClientResult<Arc<Pubky>> {
        PUBKY_SINGLETON
            .get()
            .cloned()
            .ok_or(ClientError::NotInitialized)
    }

    /// Stores an existing [`pubky::Pubky`] client if none is set yet.
    ///
    /// For tests that inject a preconfigured SDK instance. Does not replace an
    /// instance that is already initialized.
    pub async fn init_from(sdk: Pubky) -> ClientResult<()> {
        PUBKY_SINGLETON
            .get_or_try_init(|| async { Ok(Arc::new(sdk)) })
            .await
            .map(|_| ())
    }
}

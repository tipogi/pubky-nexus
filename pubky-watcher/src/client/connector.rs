// TODO: Decide public name — `PubkyConnector` is Nexus legacy; consider `SharedClient` (see rename-pubky-connector).
use super::{ClientError, ClientResult};
use pubky::{Pubky, PubkyHttpClient};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::debug;

static PUBKY_SINGLETON: OnceCell<Arc<Pubky>> = OnceCell::const_new();

pub struct PubkyConnector;

impl PubkyConnector {
    /// Initializes the `Pubky` singleton.
    ///
    /// - For mainnet, pass `None`.
    /// - For testnet, pass `Some(hostname)` (e.g., "localhost" or "homeserver").
    pub async fn initialise(testnet_host: Option<&str>) -> ClientResult<()> {
        PUBKY_SINGLETON
            .get_or_try_init(|| async {
                let mode = testnet_host
                    .map(|host| format!("testnet with host '{host}'"))
                    .unwrap_or_else(|| "mainnet".to_string());
                debug!("Initialising Pubky singleton in {mode} mode");

                let client = match testnet_host {
                    Some(host) => PubkyHttpClient::builder()
                        .testnet_with_host(host)
                        // Force pkarr/mainline DHT to bind an ephemeral local port instead of default behavior
                        // We do this to prevent the client DHT from competing with `StaticTestnet` for port 6881
                        .pkarr(|p| p.dht(|d| d.port(0)))
                        .build(),
                    None => PubkyHttpClient::new(),
                }
                .map_err(|e| ClientError::from(pubky::Error::from(e)))?;
                Ok(Arc::new(Pubky::with_client(client)))
            })
            .await
            .map(|_| ())
    }

    /// Retrieves the instance of `Pubky`
    pub fn get() -> ClientResult<Arc<Pubky>> {
        PUBKY_SINGLETON
            .get()
            .cloned()
            .ok_or(ClientError::NotInitialized)
    }

    /// Initializes `PUBKY_SINGLETON` with a provided `Pubky` instance.
    ///
    /// # Usage:
    /// - This function is primarily intended for **watcher tests** where a controlled `Pubky` instance
    ///   needs to be injected instead of relying on environment-based initialization
    pub async fn init_from(sdk: Pubky) -> ClientResult<()> {
        PUBKY_SINGLETON
            .get_or_try_init(|| async { Ok(Arc::new(sdk)) })
            .await
            .map(|_| ())
    }
}

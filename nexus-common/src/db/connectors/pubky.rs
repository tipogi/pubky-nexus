use pubky_watcher::{ClientError, WatcherClient};
use tracing::debug;

use crate::config::NetConfig;

/// Builds the Pubky client the `[stack.net]` settings describe: for the testnet relay on
/// [`NetConfig::testnet_host`] when [`NetConfig::testnet`] is set, otherwise for mainnet, with
/// every HTTP request, its response body included, bounded by
/// [`NetConfig::pubky_http_request_timeout_secs`].
///
/// `pubky-watcher` builds the client and makes every call on it; no Nexus crate drives the
/// Pubky SDK client itself. Cloning the client shares the SDK client underneath.
pub fn build_pubky_client(net: &NetConfig) -> Result<WatcherClient, ClientError> {
    let testnet_host = net.pubky_client_testnet_host();
    let http_request_timeout = net.pubky_client_http_request_timeout();

    let mode = testnet_host
        .map(|host| format!("testnet with host '{host}'"))
        .unwrap_or_else(|| "mainnet".to_string());
    debug!(
        ?http_request_timeout,
        "Building the Pubky client in {mode} mode"
    );

    let mut builder = WatcherClient::builder().request_timeout(http_request_timeout);
    if let Some(host) = testnet_host {
        builder = builder.testnet(host);
    }
    builder.build()
}

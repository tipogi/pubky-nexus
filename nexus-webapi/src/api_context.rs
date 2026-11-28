use std::path::PathBuf;
use std::sync::Arc;

use nexus_common::db::build_pubky_client;
use nexus_common::models::user::UserIngestor;
use nexus_common::{types::DynError, ApiConfig};
use pubky::pkarr::{self, Keypair};
use pubky_watcher::WatcherClient;

#[derive(Debug, Clone)]
pub struct ApiContext {
    pub(crate) api_config: ApiConfig,
    pub(crate) keypair: pkarr::Keypair,
    pub(crate) pkarr_client: pkarr::Client,
    /// Ingests unknown users for the ingest-user route, looking their homeservers up on
    /// PKDNS through the API's Pubky client.
    pub(crate) ingestor: Arc<UserIngestor>,
}

pub struct ApiContextBuilder {
    api_config: ApiConfig,
    secret_dir: PathBuf,
    pkarr_builder: Option<pkarr::ClientBuilder>,
    pubky_client: Option<WatcherClient>,
}

impl ApiContextBuilder {
    /// `secret_dir` holds the API's `secret` key file, which is created if missing
    pub fn new(api_config: ApiConfig, secret_dir: PathBuf) -> Self {
        Self {
            api_config,
            secret_dir,
            pkarr_builder: None,
            pubky_client: None,
        }
    }

    /// The Pubky client to share with the rest of the process; a clone shares the SDK client
    /// underneath. Without one, the context builds its own from the `[stack.net]` settings.
    pub fn pubky_client(mut self, pubky_client: WatcherClient) -> Self {
        self.pubky_client = Some(pubky_client);

        self
    }

    pub fn pkarr_builder(mut self, pkarr_builder: pkarr::ClientBuilder) -> Self {
        self.pkarr_builder = Some(pkarr_builder);

        self
    }

    /// Fails when the Pubky client, the pkarr client or the secret key cannot be set up.
    pub fn try_build(&self) -> Result<ApiContext, DynError> {
        // Ensure the dir exists, so a missing secret file can be created in it
        std::fs::create_dir_all(self.secret_dir.clone())?;

        let pubky_client = match &self.pubky_client {
            Some(pubky_client) => pubky_client.clone(),
            None => build_pubky_client(&self.api_config.stack.net)?,
        };
        let ingestor = UserIngestor::from_config(&self.api_config.stack, Arc::new(pubky_client));

        let pkarr_builder = self.pkarr_builder.clone().unwrap_or_default();
        let pkarr_client = pkarr_builder.build()?;

        let keypair = self.read_or_create_keypair()?;

        Ok(ApiContext {
            api_config: self.api_config.clone(),
            keypair,
            pkarr_client,
            ingestor: Arc::new(ingestor),
        })
    }

    /// Reads the secret file. Creates a new secret file if it doesn't exist.
    fn read_or_create_keypair(&self) -> Result<Keypair, DynError> {
        let secret_file_path = self.secret_dir.join("secret");

        if !secret_file_path.exists() {
            Keypair::random().write_secret_key_file(&secret_file_path)?;
        }

        let keypair =
            Keypair::from_secret_key_file(&secret_file_path).map_err(|e| e.to_string())?;

        Ok(keypair)
    }
}

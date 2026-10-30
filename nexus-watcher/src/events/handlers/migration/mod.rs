//! The internal migration: at the flip, what Nexus already holds for a user under `pubky.app`
//! is rewritten to its `social/v1` form, derived from the graph, before the user's epoch moves
//! to `v1`.
//!
//! Each step pages through the user's data in batches, each batch its own transaction, and a
//! rerun over partly migrated data gives the same result. A failed step is logged with the user
//! and the step and returned as the transient graph or Redis error it is, so the flip goes
//! through the normal retry, which runs it whole again while the user is still `v0`.

pub mod posts;

use crate::events::EventProcessorError;
use pubky_social_specs::PubkyId;
use std::future::Future;
use tracing::error;

/// Runs every step of `user_id`'s migration, in order.
#[tracing::instrument(name = "user.migrate", skip_all, fields(user_id = %user_id))]
pub async fn run(user_id: &PubkyId) -> Result<(), EventProcessorError> {
    step(user_id, "posts", posts::migrate(user_id, posts::BATCH_SIZE)).await
}

/// Runs one step, logging a failure with the user and the step before returning it.
async fn step(
    user_id: &PubkyId,
    name: &'static str,
    run: impl Future<Output = Result<(), EventProcessorError>>,
) -> Result<(), EventProcessorError> {
    run.await
        .inspect_err(|e| error!(%user_id, step = %name, error = %e, "Internal migration failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";

    /// A failed step is logged with the user and the step, and returned as it is.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_failed_step_is_logged_with_the_user_and_the_step() {
        let user_id = PubkyId::try_from(USER).unwrap();
        let failure = EventProcessorError::GraphQueryFailed(false, "boom".to_string());

        let result = step(&user_id, "posts", async { Err(failure) }).await;

        assert!(
            matches!(result, Err(EventProcessorError::GraphQueryFailed(false, ref m)) if m == "boom"),
            "{result:?}"
        );
        assert!(logs_contain("Internal migration failed"));
        assert!(logs_contain(&format!("user_id={USER}")));
        assert!(logs_contain("step=posts"));
        assert!(logs_contain("boom"));
    }

    /// A step that passes logs nothing.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_passing_step_logs_no_error() {
        let user_id = PubkyId::try_from(USER).unwrap();

        step(&user_id, "posts", async { Ok(()) }).await.unwrap();

        assert!(!logs_contain("Internal migration failed"));
    }
}

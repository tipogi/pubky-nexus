use super::utils::set_user_epoch;
use crate::event_processor::posts::utils::{find_post_details, short_post, test_user};
use crate::event_processor::utils::watcher::{retrieve_and_handle_event_line, WatcherTest};
use anyhow::Result;
use nexus_common::models::event::EventLine;
use pubky::Keypair;
use pubky_social_specs::legacy_v0::post_uri_builder;

/// Whether the shared event log holds `line`.
async fn event_line_stored(line: &str) -> Result<bool> {
    // Uncapped: the shared event log is append-only and unbounded.
    let (lines, _) = EventLine::get_from_index(None, usize::MAX).await?;
    Ok(lines.iter().any(|stored| stored == line))
}

/// A `pubky.app` post PUT of a user at `v1` is ignored: the post is not indexed and the event
/// line is not stored.
#[tokio_shared_rt::test(shared)]
async fn test_v1_user_pubky_app_post_put_is_ignored() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &test_user("Watcher:EpochGate:Put", "epoch_gate"))
        .await?;
    set_user_epoch(&user_id, "v1").await?;

    let (post_id, post_path) = test
        .create_post(&user_kp, &short_post("Watcher:EpochGate:Put:Post"))
        .await?;

    assert!(
        find_post_details(&user_id, &post_id).await.is_err(),
        "a v1 user's pubky.app post must not be indexed"
    );
    let uri = post_uri_builder(user_id.clone(), post_id);
    assert!(
        !event_line_stored(&format!("PUT {uri}")).await?,
        "an ignored event must store no event line"
    );

    // Ignored as well: there is no post to delete
    test.cleanup_post(&user_kp, &post_path).await?;
    // Back to `v0`, so the gate lets the cleanup through
    set_user_epoch(&user_id, "v0").await?;
    test.cleanup_user(&user_kp).await?;
    Ok(())
}

/// A `pubky.app` DEL of a user at `v1` is ignored: a post indexed before stays in place.
#[tokio_shared_rt::test(shared)]
async fn test_v1_user_pubky_app_post_del_is_ignored() -> Result<()> {
    let mut test = WatcherTest::setup(None).await?;

    let user_kp = Keypair::random();
    let user_id = test
        .create_user(&user_kp, &test_user("Watcher:EpochGate:Del", "epoch_gate"))
        .await?;
    let (post_id, post_path) = test
        .create_post(&user_kp, &short_post("Watcher:EpochGate:Del:Post"))
        .await?;
    set_user_epoch(&user_id, "v1").await?;

    test.cleanup_post(&user_kp, &post_path).await?;

    let post = find_post_details(&user_id, &post_id).await?;
    assert_eq!(post.content, "Watcher:EpochGate:Del:Post");
    let uri = post_uri_builder(user_id.clone(), post_id);
    let del_line = format!("DEL {uri}");
    assert!(
        !event_line_stored(&del_line).await?,
        "an ignored event must store no event line"
    );

    // Back to `v0`, then delete the post the homeserver no longer holds
    set_user_epoch(&user_id, "v0").await?;
    let event_handler = test.event_processor_runner.event_handler.clone();
    retrieve_and_handle_event_line(&del_line, event_handler).await?;
    test.cleanup_user(&user_kp).await?;
    Ok(())
}

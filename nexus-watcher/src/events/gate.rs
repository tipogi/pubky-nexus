//! The epoch gate: every user is read through one path, the one their epoch names.

use super::epoch_metrics::record_gated;
use super::event::Event;
use crate::errors::EventProcessorError;
use nexus_common::models::user::Epoch;

/// What the gate does with an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// The event goes on to the handlers.
    Handle,
    /// The event is skipped: no fetch, no stored event line, no error, no retry.
    Ignore,
}

/// The gate's decision for `event` of a user at `user_epoch`. Pure: the caller reads the
/// user's epoch.
fn decide(event: &Event, user_epoch: Epoch) -> Gate {
    match (event.target.epoch(), user_epoch) {
        (Epoch::V0, Epoch::V0) => Gate::Handle,
        // A user at `v1` is no longer read through `pubky.app`, PUT or DEL.
        (Epoch::V0, Epoch::V1) => Gate::Ignore,
        // Unreachable for now: the parse step still skips every `social/v1` event.
        (Epoch::V1, _) => Gate::Handle,
    }
}

/// Whether `event` goes on to the handlers, by its user's epoch in the graph: never the cached
/// `User:Details` copy, which a concurrent profile write can leave stale. An ignored event is
/// counted on `watcher.epoch.skipped`.
pub(super) async fn admits(event: &Event) -> Result<bool, EventProcessorError> {
    let user_epoch = Epoch::get_from_graph(event.target.user_id()).await?;
    match decide(event, user_epoch) {
        Gate::Handle => Ok(true),
        Gate::Ignore => {
            record_gated(&event.target, &event.uri, user_epoch);
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::ParseResult;

    const USER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";

    fn event(line: &str) -> Event {
        match Event::parse_event(line) {
            Ok(ParseResult::Parsed(event)) => event,
            other => panic!("{line}: expected Parsed, got {other:?}"),
        }
    }

    /// Lines of `pubky.app` events, PUT and DEL, of a profile and a post.
    fn pubky_app_lines() -> Vec<String> {
        let paths = ["profile.json", "posts/0032SSN7Q4EVG"];
        ["PUT", "DEL"]
            .iter()
            .flat_map(|kind| {
                paths
                    .iter()
                    .map(move |path| format!("{kind} pubky://{USER}/pub/pubky.app/{path}"))
            })
            .collect()
    }

    #[test]
    fn a_v0_user_has_pubky_app_events_handled() {
        for line in pubky_app_lines() {
            assert_eq!(decide(&event(&line), Epoch::V0), Gate::Handle, "{line}");
        }
    }

    #[test]
    fn a_v1_user_has_pubky_app_events_ignored() {
        for line in pubky_app_lines() {
            assert_eq!(decide(&event(&line), Epoch::V1), Gate::Ignore, "{line}");
        }
    }
}

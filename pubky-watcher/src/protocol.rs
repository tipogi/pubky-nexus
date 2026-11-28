//! The Protocol checks of an Event stream (`/events-stream`), and the [`ProtocolViolation`]
//! every Protocol check reports. The Events feed's is [`FeedBatch::closing_cursor`].
//!
//! Use them when you read an Event stream yourself, through
//! [`StreamReader`](crate::StreamReader) in your own [`Run`](crate::Run). A Stream consumer
//! ([`Watcher::stream`](crate::Watcher::stream)) calls them for you.

#![allow(
    clippy::result_large_err,
    reason = "the errors carry the keys involved (a `PublicKey` is about 200 bytes); a run holds at most one per user"
)]

use pubky::{EventCursor, PublicKey};
use thiserror::Error;

use crate::StreamEvent;
#[cfg(doc)]
use crate::{FeedBatch, StopReason, WatcherError};

/// A Protocol violation: an answer from a Homeserver that breaks the rules of its Events
/// feed or Event stream, as a Protocol check finds it. The Protocol checks are
/// [`FeedBatch::closing_cursor`], [`check_stream_cursors`], [`check_event_user_pubkey`] and
/// [`check_stream_limit`]; each only reports, and what follows is the caller's.
///
/// The Consumers handle none of the answer's Events and stop: see [`WatcherError::Protocol`]
/// and [`StopReason::Protocol`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ProtocolViolation {
    /// An Events feed's Batch had Events but no `cursor:` line.
    /// Found by [`FeedBatch::closing_cursor`].
    #[error("the homeserver sent events without a cursor line")]
    MissingCursor,
    /// The Cursor closing an Events feed's Batch is not a number.
    /// Found by [`FeedBatch::closing_cursor`].
    #[error("the homeserver sent a cursor that is no number: {raw:?}")]
    MalformedCursor {
        /// The Cursor as the Homeserver sent it.
        /// Without its [`CURSOR_PREFIX`](crate::CURSOR_PREFIX).
        raw: String,
    },
    /// Events came with a Cursor that does not move past the one they had to: the one the
    /// Batch was read from, or on an Event stream, the previous Event's.
    /// Found by [`FeedBatch::closing_cursor`] and [`check_stream_cursors`].
    #[error("{}", cursor_not_advanced(.user_pubkey.as_ref(), *.requested, *.got))]
    CursorNotAdvanced {
        /// The User pubkey whose Event stream it was, from [`check_stream_cursors`]; `None`
        /// from [`FeedBatch::closing_cursor`].
        user_pubkey: Option<PublicKey>,
        /// The Cursor the Events had to move past.
        /// On an Event stream, the previous Event's Cursor after the first Event.
        requested: u64,
        /// The Cursor the Homeserver sent.
        /// The Batch's Cursor on an Events feed, the Event's on an Event stream.
        got: u64,
    },
    /// An Events feed's Batch had no Events but moved the Cursor, which would skip whatever
    /// lies between.
    /// Found by [`FeedBatch::closing_cursor`].
    #[error("the homeserver sent no events but moved the cursor from {requested} to {got}")]
    CursorMovedWithoutEvents {
        /// The Cursor the Batch was read from.
        requested: u64,
        /// The Cursor the Homeserver sent.
        /// The Batch's cursor line ([`FeedBatch::cursor`]).
        got: u64,
    },
    /// An Event stream's Batch held more Events than were asked for.
    /// Found by [`check_stream_limit`].
    #[error(
        "the stream of {} returned {got} events, more than the {limit} requested",
        .user_pubkey.z32()
    )]
    TooManyEvents {
        /// The User pubkey whose Event stream it was.
        user_pubkey: PublicKey,
        /// The Events the Batch held.
        got: usize,
        /// The Events asked for: the `limit` given to [`check_stream_limit`].
        limit: u16,
    },
    /// A User pubkey's Event stream held an Event of another User pubkey, which would let
    /// one user's Event stream inject another's Events.
    /// Found by [`check_event_user_pubkey`].
    #[error(
        "the stream of {} returned an event of {}",
        .user_pubkey.z32(),
        .event_user_pubkey.z32()
    )]
    ForeignEvent {
        /// The User pubkey whose Event stream it was.
        user_pubkey: PublicKey,
        /// The User pubkey of the Event ([`StreamEvent::user_pubkey`]).
        event_user_pubkey: PublicKey,
    },
}

fn cursor_not_advanced(user_pubkey: Option<&PublicKey>, requested: u64, got: u64) -> String {
    match user_pubkey {
        None => format!(
            "the homeserver sent events with cursor {got}, which does not move past {requested}"
        ),
        Some(user_pubkey) => format!(
            "the stream of {} returned cursor {got}, which does not move past {requested}",
            user_pubkey.z32()
        ),
    }
}

/// A Protocol check of an Event stream's Batch: every Event's Cursor moves past the one
/// before it, starting from `requested`, the Cursor the Batch of `user_pubkey`'s Event stream
/// was read from. A Cursor that holds or goes back would have a Homeserver replay or reorder
/// the user's Events.
///
/// It reads the Cursors only, not whose Events they are: that is
/// [`check_event_user_pubkey`]. A Batch with no Events passes.
///
/// Returns only `CursorNotAdvanced` of [`ProtocolViolation`], with `user_pubkey` set, for the
/// first Event that does not move forward. It only reports: what follows is the caller's.
pub fn check_stream_cursors(
    user_pubkey: &PublicKey,
    requested: EventCursor,
    events: &[StreamEvent],
) -> Result<(), ProtocolViolation> {
    let mut floor = requested.id();
    for event in events {
        if event.cursor <= floor {
            return Err(ProtocolViolation::CursorNotAdvanced {
                user_pubkey: Some(user_pubkey.clone()),
                requested: floor,
                got: event.cursor,
            });
        }
        floor = event.cursor;
    }
    Ok(())
}

/// A Protocol check of one Stream event: `event` belongs to `user_pubkey`, the User pubkey
/// whose Event stream it was read from. An Event of another User pubkey would let one user's
/// Event stream inject another's Events.
///
/// It checks one Event, so that a Run can call it in its own handling loop and choose what
/// becomes of the Events before a foreign one.
///
/// Returns only `ForeignEvent` of [`ProtocolViolation`]. It only reports: what follows is the
/// caller's.
pub fn check_event_user_pubkey(
    user_pubkey: &PublicKey,
    event: &StreamEvent,
) -> Result<(), ProtocolViolation> {
    if event.user_pubkey() != user_pubkey {
        return Err(ProtocolViolation::ForeignEvent {
            user_pubkey: user_pubkey.clone(),
            event_user_pubkey: event.user_pubkey().clone(),
        });
    }
    Ok(())
}

/// A Protocol check of an Event stream's Batch: `events`, read from `user_pubkey`'s Event
/// stream with `limit`, holds no more than `limit` Events.
///
/// It is kept apart from [`check_stream_cursors`] and [`check_event_user_pubkey`], so that a
/// Run can take it or leave it.
///
/// Returns only `TooManyEvents` of [`ProtocolViolation`]. It only reports: what follows is
/// the caller's.
pub fn check_stream_limit(
    user_pubkey: &PublicKey,
    limit: u16,
    events: &[StreamEvent],
) -> Result<(), ProtocolViolation> {
    if events.len() > usize::from(limit) {
        return Err(ProtocolViolation::TooManyEvents {
            user_pubkey: user_pubkey.clone(),
            got: events.len(),
            limit,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pubky::{EventCursor, PublicKey};

    use super::{check_event_user_pubkey, check_stream_cursors, check_stream_limit};
    use crate::fixtures::{event, key};
    use crate::ProtocolViolation;

    fn not_advanced(user_pubkey: &PublicKey, requested: u64, got: u64) -> ProtocolViolation {
        ProtocolViolation::CursorNotAdvanced {
            user_pubkey: Some(user_pubkey.clone()),
            requested,
            got,
        }
    }

    // --- check_stream_cursors ---

    /// Cursors that move forward from the requested one pass, and so does a Batch with none.
    #[test]
    fn cursors_that_move_forward_pass() {
        let alice = key();
        let events = [event(&alice, 11), event(&alice, 12), event(&alice, 20)];

        assert_eq!(
            check_stream_cursors(&alice, EventCursor::new(10), &events),
            Ok(())
        );
        assert_eq!(
            check_stream_cursors(&alice, EventCursor::new(10), &[]),
            Ok(())
        );
    }

    /// The first Event must move past the Cursor the Batch was read from: one that holds it
    /// or goes back would be read again.
    #[test]
    fn a_first_cursor_that_does_not_move_past_the_requested_one_is_reported() {
        let alice = key();
        for got in [10, 3] {
            assert_eq!(
                check_stream_cursors(&alice, EventCursor::new(10), &[event(&alice, got)]),
                Err(not_advanced(&alice, 10, got)),
                "{got}"
            );
        }
    }

    /// Each later Event must move past the previous one's Cursor: a repeated or reordered
    /// Event is reported against the Cursor before it.
    #[test]
    fn a_cursor_that_does_not_move_past_the_previous_events_is_reported() {
        let alice = key();
        for got in [12, 11] {
            let events = [event(&alice, 11), event(&alice, 12), event(&alice, got)];
            assert_eq!(
                check_stream_cursors(&alice, EventCursor::new(10), &events),
                Err(not_advanced(&alice, 12, got)),
                "{got}"
            );
        }
    }

    /// The check reads Cursors only: the User pubkey it reports is the one given, whoever's
    /// the Event is.
    #[test]
    fn the_violation_names_the_user_pubkey_given() {
        let (alice, mallory) = (key(), key());
        assert_eq!(
            check_stream_cursors(&alice, EventCursor::new(10), &[event(&mallory, 9)]),
            Err(not_advanced(&alice, 10, 9))
        );
    }

    // --- check_event_user_pubkey ---

    /// An Event of the User pubkey whose Event stream it is passes.
    #[test]
    fn an_event_of_the_streams_own_user_pubkey_passes() {
        let alice = key();
        assert_eq!(check_event_user_pubkey(&alice, &event(&alice, 11)), Ok(()));
    }

    /// An Event of another User pubkey would let one user's Event stream inject another's.
    #[test]
    fn an_event_of_another_user_pubkey_is_foreign() {
        let (alice, mallory) = (key(), key());
        assert_eq!(
            check_event_user_pubkey(&alice, &event(&mallory, 11)),
            Err(ProtocolViolation::ForeignEvent {
                user_pubkey: alice.clone(),
                event_user_pubkey: mallory.clone(),
            })
        );
    }

    // --- check_stream_limit ---

    /// A Batch of exactly `limit` Events passes, and so does one with none.
    #[test]
    fn a_batch_of_at_most_the_limit_passes() {
        let alice = key();
        let events = [event(&alice, 11), event(&alice, 12)];

        assert_eq!(check_stream_limit(&alice, 2, &events), Ok(()));
        assert_eq!(check_stream_limit(&alice, 2, &[]), Ok(()));
    }

    /// One Event past the limit is more than was asked for.
    #[test]
    fn a_batch_of_one_event_past_the_limit_is_too_many() {
        let alice = key();
        let events = [event(&alice, 11), event(&alice, 12), event(&alice, 13)];

        assert_eq!(
            check_stream_limit(&alice, 2, &events),
            Err(ProtocolViolation::TooManyEvents {
                user_pubkey: alice.clone(),
                got: 3,
                limit: 2,
            })
        );
    }
}

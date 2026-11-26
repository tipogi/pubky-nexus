//! The line protocol of an Events feed (`/events/`): a `text/plain` body with one Event line
//! per Event (`PUT <uri>` / `DEL <uri>`), closed by a `cursor: <value>` line.
//!
//! Use [`FeedBatch::split`] when you read an Events feed yourself, through
//! [`FeedReader`](crate::FeedReader) in your own [`Run`](crate::Run), and [`FeedEvent`] when
//! the Event type and the URI are all your Handler needs. A Feed consumer
//! ([`Watcher::feed`](crate::Watcher::feed)) does both for you.

use std::convert::Infallible;

use crate::client::EventType;
use crate::hooks::{EventLineOutcome, FromEventLine};

/// Prefix of the line closing an Events feed's Batch with the Cursor to resume from.
/// [`FeedBatch::split`] reads it.
pub const CURSOR_PREFIX: &str = "cursor: ";

/// A Feed event: an Event read from a Homeserver's Events feed, `PUT <uri>` or `DEL <uri>`.
///
/// The Event a Feed consumer ([`Watcher::feed`](crate::Watcher::feed)) hands to its Handler.
/// An Indexer that needs richer parsing defines its own type and implements
/// [`FromEventLine`] for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedEvent {
    /// `PUT` or `DEL`, as an [`EventType`].
    pub event_type: EventType,
    /// The Resource's `pubky://` URI, as the Event line carries it.
    /// A `PUT`'s Resource is read with
    /// [`ResourceReader::read_resource`](crate::ResourceReader::read_resource).
    pub uri: String,
}

/// Parsing never fails: an empty line is [`EventLineOutcome::Skipped`], and a line that is
/// not `PUT <uri>` or `DEL <uri>` is [`EventLineOutcome::Unrecognized`]. Only the first space
/// separates the Event type from the URI: Homeservers write a stored file's name unencoded,
/// and a name may contain spaces.
impl FromEventLine for FeedEvent {
    type Error = Infallible;

    fn from_event_line(line: &str) -> Result<EventLineOutcome<Self>, Self::Error> {
        if line.trim().is_empty() {
            return Ok(EventLineOutcome::Skipped);
        }

        let Some((event_type, uri)) = line.split_once(' ') else {
            return Ok(EventLineOutcome::Unrecognized {
                reason: format!("expected `<METHOD> <uri>`, got: {line}"),
            });
        };

        let event_type = match event_type {
            "PUT" => EventType::Put,
            "DEL" => EventType::Del,
            other => {
                return Ok(EventLineOutcome::Unrecognized {
                    reason: format!("unknown event method: {other}"),
                });
            }
        };

        if uri.trim().is_empty() {
            return Ok(EventLineOutcome::Unrecognized {
                reason: format!("missing uri in event line: {line}"),
            });
        }

        Ok(EventLineOutcome::Parsed(Self {
            event_type,
            uri: uri.to_string(),
        }))
    }
}

/// The Batch one read of an Events feed returns, split into its Event lines and the Cursor
/// closing it.
///
/// The Homeserver sends the cursor line last, but that is positional convention
/// rather than something we can rely on from an untrusted peer: the split is
/// order-independent, and if a Batch carries several cursor lines the last one
/// wins, being the position the Homeserver ends the Batch at.
/// Built by [`FeedBatch::split`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedBatch<'a> {
    /// The Event lines, in the order the Homeserver sent them, for
    /// [`FromEventLine::from_event_line`](crate::FromEventLine::from_event_line).
    pub event_lines: Vec<&'a str>,
    /// Raw, still unparsed value of the Batch's cursor line, if it carried one.
    /// It is what follows [`CURSOR_PREFIX`].
    pub cursor: Option<&'a str>,
}

impl<'a> FeedBatch<'a> {
    /// Splits the lines of an Events feed's response.
    /// A line starting with [`CURSOR_PREFIX`] is a cursor line; any other is an Event line.
    pub fn split(lines: &'a [String]) -> Self {
        let mut event_lines = Vec::with_capacity(lines.len());
        let mut cursor = None;

        for line in lines {
            match line.strip_prefix(CURSOR_PREFIX) {
                Some(value) => cursor = Some(value),
                None => event_lines.push(line.as_str()),
            }
        }

        Self {
            event_lines,
            cursor,
        }
    }

    /// Whether the Batch carried anything other than its cursor line: whether
    /// [`Self::event_lines`] is non-empty.
    ///
    /// Deliberately counts *every* non-cursor line, including ones that later
    /// parse as skipped or unrecognized: those still legitimately advance the
    /// Cursor, so narrowing this to "lines that reached a Handler" would let a
    /// non-advancing Cursor through and re-open the replay loop.
    pub fn has_events(&self) -> bool {
        !self.event_lines.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{FeedBatch, FeedEvent};
    use crate::{EventLineOutcome, EventType, FromEventLine};

    fn parsed(line: &str) -> FeedEvent {
        match FeedEvent::from_event_line(line).unwrap() {
            EventLineOutcome::Parsed(event) => event,
            other => panic!("{line:?} did not parse: {other:?}"),
        }
    }

    fn unrecognized(line: &str) -> bool {
        matches!(
            FeedEvent::from_event_line(line).unwrap(),
            EventLineOutcome::Unrecognized { .. }
        )
    }

    // --- FeedEvent::from_event_line ---

    #[test]
    fn put_and_del_lines_parse() {
        assert_eq!(
            parsed("PUT pubky://a/pub/x"),
            FeedEvent {
                event_type: EventType::Put,
                uri: "pubky://a/pub/x".into()
            }
        );
        assert_eq!(parsed("DEL pubky://a/pub/x").event_type, EventType::Del);
    }

    /// Only the first space separates the Event type from the URI, so a file name with spaces
    /// stays in the URI.
    #[test]
    fn the_uri_runs_from_the_first_space_to_the_end() {
        assert_eq!(
            parsed("PUT pubky://a/pub/files/my holiday photo.jpg").uri,
            "pubky://a/pub/files/my holiday photo.jpg"
        );
    }

    #[test]
    fn empty_lines_are_skipped() {
        assert!(matches!(
            FeedEvent::from_event_line("").unwrap(),
            EventLineOutcome::Skipped
        ));
    }

    #[test]
    fn other_lines_are_unrecognized() {
        assert!(unrecognized("PUT"));
        assert!(unrecognized("PUT "));
        assert!(unrecognized("PATCH pubky://a/pub/x"));
        assert!(unrecognized("put pubky://a/pub/x"));
    }

    /// A cursor line that reaches the parser is no Event either.
    #[test]
    fn a_cursor_line_is_unrecognized() {
        assert!(unrecognized("cursor: 42"));
    }

    fn lines(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|l| l.to_string()).collect()
    }

    // --- FeedBatch::split ---

    /// The usual shape: Event lines followed by the Cursor that closes the Batch.
    #[test]
    fn splits_events_from_the_trailing_cursor() {
        let lines = lines(&["PUT pubky://a/pub/x", "DEL pubky://b/pub/y", "cursor: 42"]);
        let batch = FeedBatch::split(&lines);

        assert_eq!(
            batch.event_lines,
            vec!["PUT pubky://a/pub/x", "DEL pubky://b/pub/y"]
        );
        assert_eq!(batch.cursor, Some("42"));
        assert!(batch.has_events());
    }

    /// An idle Homeserver: a Cursor and nothing else, which must not read as "had events".
    #[test]
    fn cursor_only_batch_has_no_events() {
        let lines = lines(&["cursor: 42"]);
        let batch = FeedBatch::split(&lines);

        assert!(batch.event_lines.is_empty());
        assert_eq!(batch.cursor, Some("42"));
        assert!(!batch.has_events());
    }

    /// The Cursor is found wherever it sits, not only in last position.
    #[test]
    fn cursor_is_found_out_of_position() {
        let lines = lines(&["cursor: 42", "PUT pubky://a/pub/x"]);
        let batch = FeedBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["PUT pubky://a/pub/x"]);
        assert_eq!(batch.cursor, Some("42"));
    }

    /// Several cursor lines: the last one wins, being where the Batch ends.
    #[test]
    fn last_cursor_line_wins() {
        let lines = lines(&["cursor: 41", "PUT pubky://a/pub/x", "cursor: 42"]);
        let batch = FeedBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["PUT pubky://a/pub/x"]);
        assert_eq!(batch.cursor, Some("42"));
    }

    /// A Batch with no cursor line at all, which the caller rejects as stalled
    /// when it carries Event lines: there is nothing to move the Checkpoint with.
    #[test]
    fn batch_without_cursor_line() {
        let lines = lines(&["PUT pubky://a/pub/x"]);
        let batch = FeedBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["PUT pubky://a/pub/x"]);
        assert_eq!(batch.cursor, None);
        assert!(batch.has_events());
    }

    /// Lines that will later parse as skipped or unrecognized still count as
    /// events: they legitimately advance the Cursor, so a Batch made only of
    /// them must not be treated as an idle Homeserver's.
    #[test]
    fn unrecognized_lines_count_as_events() {
        let lines = lines(&["garbage", "cursor: 42"]);
        let batch = FeedBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["garbage"]);
        assert!(batch.has_events());
    }
}

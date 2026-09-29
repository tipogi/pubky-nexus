//! Line protocol for a homeserver `GET /events/` response.
//!
//! The body is `text/plain`: one event per line (`PUT <uri>` / `DEL <uri>`),
//! closed by a `cursor: <value>` line. That trailing cursor is convention, not
//! something to trust from an untrusted peer — [`EventBatch::split`] is
//! order-independent, and if several cursor lines are present the last one wins.
//!
//! [`HomeserverEvent`] is the default [`crate::ParseFromLine`] implementor for
//! that wire format. Apps that need richer parsing (Nexus resource types, etc.)
//! define their own event type.

use std::fmt;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use crate::traits::{LineParseOutcome, ParseFromLine};
use crate::watcher::WatcherError;

/// Prefix of the cursor line in a homeserver `/events/` body.
pub const CURSOR_PREFIX: &str = "cursor: ";

/// Reads chunks from `stream`, buffering at most `max + 1` bytes.
///
/// The boolean is true when the stream exceeded `max`. Stream errors are
/// returned unchanged.
pub async fn read_stream_capped<S, E>(mut stream: S, max: usize) -> Result<(Vec<u8>, bool), E>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let cap = max + 1;
    let mut buf = Vec::new();
    let mut total = 0usize;

    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        total += bytes.len();
        buf.extend_from_slice(&bytes[..bytes.len().min(cap.saturating_sub(buf.len()))]);
        if total >= cap {
            return Ok((buf, true));
        }
    }

    Ok((buf, false))
}

/// Operation on a homeserver `/events/` line (`PUT` or `DEL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventMethod {
    /// Resource created or updated.
    Put,
    /// Resource deleted.
    Del,
}

impl EventMethod {
    /// Wire token: `"PUT"` or `"DEL"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Del => "DEL",
        }
    }
}

impl fmt::Display for EventMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Default parsed form of a homeserver event line: `PUT|DEL <uri>`.
///
/// Used by [`crate::Watcher`] when you do not define your own event type.
/// Implements [`ParseFromLine`] with [`WatcherError`] (parse never fails — bad
/// lines become [`LineParseOutcome::Skipped`] or
/// [`LineParseOutcome::Unrecognized`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeserverEvent {
    /// `PUT` or `DEL`.
    pub method: EventMethod,
    /// Resource URI from the event line.
    pub uri: String,
}

impl ParseFromLine for HomeserverEvent {
    type Error = WatcherError;

    fn parse_line(line: &str) -> Result<LineParseOutcome<Self>, Self::Error> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(LineParseOutcome::Skipped);
        }

        let Some((method, uri)) = line.split_once(' ') else {
            return Ok(LineParseOutcome::Unrecognized {
                reason: format!("expected `<TYPE> <uri>`, got: {line}"),
            });
        };

        let method = match method {
            "PUT" => EventMethod::Put,
            "DEL" => EventMethod::Del,
            other => {
                return Ok(LineParseOutcome::Unrecognized {
                    reason: format!("unknown event type: {other}"),
                });
            }
        };

        let uri = uri.trim();
        if uri.is_empty() {
            return Ok(LineParseOutcome::Unrecognized {
                reason: format!("missing uri in event line: {line}"),
            });
        }

        Ok(LineParseOutcome::Parsed(HomeserverEvent {
            method,
            uri: uri.to_string(),
        }))
    }
}

/// A homeserver `/events/` response, split into event lines and the cursor
/// closing the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch<'a> {
    pub event_lines: Vec<&'a str>,
    /// Raw, still unparsed value of the batch's cursor line, if it carried one.
    pub cursor: Option<&'a str>,
}

impl<'a> EventBatch<'a> {
    /// Split a `/events/` body into event lines and cursor.
    pub fn from_body(body: &'a str) -> Self {
        Self::from_lines(body.trim().lines())
    }

    /// Split already-tokenized `/events/` lines into event lines and cursor.
    pub fn split(lines: &'a [String]) -> Self {
        Self::from_lines(lines.iter().map(String::as_str))
    }

    fn from_lines(lines: impl IntoIterator<Item = &'a str>) -> Self {
        let mut event_lines = Vec::new();
        let mut cursor = None;

        for line in lines {
            match line.strip_prefix(CURSOR_PREFIX) {
                Some(value) => cursor = Some(value),
                None => event_lines.push(line),
            }
        }

        Self {
            event_lines,
            cursor,
        }
    }

    /// Whether the batch carried anything other than its cursor line.
    ///
    /// Counts every non-cursor line, including ones that later parse as skipped
    /// or unrecognized: those still legitimately advance the cursor, so narrowing
    /// this to "lines that reached a handler" would let a non-advancing cursor
    /// through and re-open a replay loop.
    pub fn has_events(&self) -> bool {
        !self.event_lines.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{read_stream_capped, EventBatch, EventMethod, HomeserverEvent};
    use crate::traits::{LineParseOutcome, ParseFromLine};
    use bytes::Bytes;
    use futures_util::{stream, Stream};

    #[derive(Debug)]
    struct TestErr;

    fn ok_stream(data: Vec<u8>) -> impl Stream<Item = Result<Bytes, TestErr>> + Unpin {
        stream::iter(vec![Ok(Bytes::from(data))])
    }

    fn err_stream() -> impl Stream<Item = Result<Bytes, TestErr>> + Unpin {
        stream::iter(vec![Err(TestErr)])
    }

    fn partial_then_err(data: Vec<u8>) -> impl Stream<Item = Result<Bytes, TestErr>> + Unpin {
        stream::iter(vec![Ok(Bytes::from(data)), Err(TestErr)])
    }

    fn lines(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|l| l.to_string()).collect()
    }

    #[test]
    fn parses_put_and_del_lines() {
        let put = HomeserverEvent::parse_line("PUT pubky://a/pub/x").unwrap();
        assert!(matches!(
            put,
            LineParseOutcome::Parsed(HomeserverEvent {
                method: EventMethod::Put,
                ref uri
            }) if uri == "pubky://a/pub/x"
        ));

        let del = HomeserverEvent::parse_line("DEL pubky://b/pub/y").unwrap();
        assert!(matches!(
            del,
            LineParseOutcome::Parsed(HomeserverEvent {
                method: EventMethod::Del,
                ..
            })
        ));
    }

    #[test]
    fn skips_blank_and_flags_unknown() {
        assert!(matches!(
            HomeserverEvent::parse_line("  ").unwrap(),
            LineParseOutcome::Skipped
        ));
        assert!(matches!(
            HomeserverEvent::parse_line("PATCH pubky://a").unwrap(),
            LineParseOutcome::Unrecognized { .. }
        ));
        assert!(matches!(
            HomeserverEvent::parse_line("not-a-line").unwrap(),
            LineParseOutcome::Unrecognized { .. }
        ));
    }

    /// The usual shape: events followed by the cursor that closes the batch.
    #[test]
    fn splits_events_from_the_trailing_cursor() {
        let lines = lines(&["PUT pubky://a/pub/x", "DEL pubky://b/pub/y", "cursor: 42"]);
        let batch = EventBatch::split(&lines);

        assert_eq!(
            batch.event_lines,
            vec!["PUT pubky://a/pub/x", "DEL pubky://b/pub/y"]
        );
        assert_eq!(batch.cursor, Some("42"));
        assert!(batch.has_events());
    }

    /// An idle poll: a cursor and nothing else, which must not read as "had events".
    #[test]
    fn cursor_only_batch_has_no_events() {
        let lines = lines(&["cursor: 42"]);
        let batch = EventBatch::split(&lines);

        assert!(batch.event_lines.is_empty());
        assert_eq!(batch.cursor, Some("42"));
        assert!(!batch.has_events());
    }

    /// The cursor is found wherever it sits, not only in last position.
    #[test]
    fn cursor_is_found_out_of_position() {
        let lines = lines(&["cursor: 42", "PUT pubky://a/pub/x"]);
        let batch = EventBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["PUT pubky://a/pub/x"]);
        assert_eq!(batch.cursor, Some("42"));
    }

    /// Several cursor lines: the last one wins, being where the batch ends.
    #[test]
    fn last_cursor_line_wins() {
        let lines = lines(&["cursor: 41", "PUT pubky://a/pub/x", "cursor: 42"]);
        let batch = EventBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["PUT pubky://a/pub/x"]);
        assert_eq!(batch.cursor, Some("42"));
    }

    /// A batch with no cursor line at all, which the caller rejects as stalled
    /// when it carries events: there is nothing to move the checkpoint with.
    #[test]
    fn batch_without_cursor_line() {
        let lines = lines(&["PUT pubky://a/pub/x"]);
        let batch = EventBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["PUT pubky://a/pub/x"]);
        assert_eq!(batch.cursor, None);
        assert!(batch.has_events());
    }

    /// Lines that will later parse as skipped or unrecognized still count as
    /// events: they legitimately advance the cursor, so a batch made only of
    /// them must not be treated as an idle poll.
    #[test]
    fn unrecognized_lines_count_as_events() {
        let lines = lines(&["garbage", "cursor: 42"]);
        let batch = EventBatch::split(&lines);

        assert_eq!(batch.event_lines, vec!["garbage"]);
        assert!(batch.has_events());
    }

    #[test]
    fn from_body_trims_and_splits() {
        let body = "PUT pubky://a/pub/x\nDEL pubky://b/pub/y\ncursor: 42\n";
        let batch = EventBatch::from_body(body);

        assert_eq!(
            batch.event_lines,
            vec!["PUT pubky://a/pub/x", "DEL pubky://b/pub/y"]
        );
        assert_eq!(batch.cursor, Some("42"));
    }

    #[tokio::test]
    async fn capped_stream_handles_empty_and_exact_limits() {
        let (empty, exceeded) = read_stream_capped(ok_stream(vec![]), 100).await.unwrap();
        assert!(empty.is_empty());
        assert!(!exceeded);

        let (exact, exceeded) = read_stream_capped(ok_stream(vec![1; 100]), 100)
            .await
            .unwrap();
        assert_eq!(exact.len(), 100);
        assert!(!exceeded);
    }

    #[tokio::test]
    async fn capped_stream_buffers_at_most_limit_plus_one() {
        let (bytes, exceeded) = read_stream_capped(ok_stream(vec![1; 1_000_000]), 100)
            .await
            .unwrap();
        assert_eq!(bytes.len(), 101);
        assert!(exceeded);
    }

    #[tokio::test]
    async fn capped_stream_propagates_errors() {
        let result: Result<(Vec<u8>, bool), TestErr> = read_stream_capped(err_stream(), 100).await;
        assert!(result.is_err());

        let result: Result<(Vec<u8>, bool), TestErr> =
            read_stream_capped(partial_then_err(vec![1; 50]), 100).await;
        assert!(result.is_err());
    }
}

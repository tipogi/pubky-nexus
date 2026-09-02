//! Line protocol for a homeserver `GET /events/` response.
//!
//! The body is `text/plain`: one event per line (`PUT <uri>` / `DEL <uri>`),
//! closed by a `cursor: <value>` line. That trailing cursor is convention, not
//! something to trust from an untrusted peer — [`EventBatch::split`] is
//! order-independent, and if several cursor lines are present the last one wins.

/// Prefix of the cursor line in a homeserver `/events/` body.
pub const CURSOR_PREFIX: &str = "cursor: ";

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
    use super::EventBatch;

    fn lines(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|l| l.to_string()).collect()
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
}

use std::fmt;

/// The Event type: whether an Event wrote its Resource (`PUT`) or deleted it (`DEL`).
///
/// Neither carries the Resource's data: for a `PUT` the Resource is read separately.
/// Carried by [`FeedEvent`](crate::FeedEvent) and [`StreamEvent`](crate::StreamEvent).
///
/// It shares its name with the SDK's `pubky::EventType` on purpose: it is the Homeserver's
/// own word. This one drops what the SDK's carries beyond `PUT` or `DEL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    /// The Resource was written; read it with
    /// [`ResourceReader::read_resource`](crate::ResourceReader::read_resource).
    Put,
    /// The Resource was deleted when the Event was emitted. A later `PUT` can write it again,
    /// so a [`ResourceReader`](crate::ResourceReader) may still find it.
    Del,
}

impl From<pubky::EventType> for EventType {
    fn from(value: pubky::EventType) -> Self {
        match value {
            pubky::EventType::Put { .. } => Self::Put,
            pubky::EventType::Delete => Self::Del,
        }
    }
}

/// `PUT` or `DEL`, as on an Event line.
impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Put => "PUT",
            Self::Del => "DEL",
        })
    }
}

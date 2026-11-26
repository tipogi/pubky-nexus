use std::fmt;

/// What a homeserver event did to its resource: written (`PUT`) or deleted (`DEL`).
///
/// Neither carries a body: for a `PUT` the resource is fetched separately.
/// Carried by [`FeedEvent`](crate::FeedEvent) and
/// [`StreamEvent`](crate::StreamEvent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    /// The resource was written; read it with
    /// [`ResourceReader::read_resource`](crate::ResourceReader::read_resource).
    Put,
    /// The resource was deleted when the event was emitted. A later `PUT` can write it again,
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

/// `PUT` or `DEL`, as on an `/events/` line.
impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Put => "PUT",
            Self::Del => "DEL",
        })
    }
}

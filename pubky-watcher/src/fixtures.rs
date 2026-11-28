//! Test fixtures shared by the unit tests: random User pubkeys and the Stream events of one.

use pubky::{Keypair, PubkyResource, PublicKey};

use crate::{EventType, StreamEvent};

/// A random pubkey, for a User pubkey or a Homeserver pubkey.
pub(crate) fn key() -> PublicKey {
    Keypair::random().public_key()
}

/// An Event of `user_pubkey`'s at `cursor`.
pub(crate) fn event(user_pubkey: &PublicKey, cursor: u64) -> StreamEvent {
    let resource = PubkyResource::new(user_pubkey.clone(), "/pub/app/item").expect("valid path");
    StreamEvent::new(EventType::Put, &resource, cursor)
}

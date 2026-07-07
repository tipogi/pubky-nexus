//! # Pubky Watcher
//!
//! Generic library for subscribing to Pubky homeserver event streams and
//! processing PUT/DEL events. Intended for external developers building
//! indexers, sync pipelines, or other event-driven applications on top of
//! Pubky homeservers.
//!
//! This crate provides the transport and orchestration layer (bulk `/events`
//! polling, per-user event streams, parsing, cursors, retries). Domain-specific
//! indexing — such as Nexus graph and Redis rules — lives in higher-level
//! crates like `nexus-watcher`.

//! xb-core: the chat server's logic and data. Sessions and handlers
//! (server.rs), the in-memory store (store.rs), redb and the fjall offline
//! queue (db.rs, queue_db.rs), replica pairs (replica.rs), the core's side
//! of connection gateways (relay.rs), push, metrics and test hooks.
//!
//! It speaks to phones only through xb-proto's transport; the web port
//! (WebSocket, web app, media pass-through) is the server program's.

pub mod admin;
pub mod db;
pub mod keys;
pub mod metrics;
pub mod push;
pub mod queue_db;
pub mod relay;
pub mod replica;
pub mod server;
pub mod store;

// The wire modules, under their old names inside this crate.
pub use xb_proto::{codec, frame, tokens, transport, util};

#[cfg(test)]
mod fuzz;

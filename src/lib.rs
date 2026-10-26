//! xb-server as a library: the server binary (main.rs) and the load-test
//! tool (bin/loadgen.rs) share these modules.

pub mod admin;
pub mod codec;
pub mod db;
pub mod frame;
pub mod gateway;
pub mod keys;
pub mod media;
pub mod metrics;
pub mod push;
pub mod queue_db;
pub mod relay;
pub mod replica;
pub mod server;
pub mod store;
pub mod tokens;
pub mod transport;
pub mod util;

#[cfg(test)]
mod fuzz;

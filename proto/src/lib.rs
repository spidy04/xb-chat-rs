//! xb-proto: the wire, everything two programs must agree on byte for byte.
//! The server, the gateway, the media service and the load tool all build
//! on it; it knows nothing about chat logic or storage.
//!
//!   tokens, codec   binary stanzas (FunXMPP)
//!   frame           3-byte length framing
//!   transport       the client header, Noise XX, AES-GCM frames (TCP or WebSocket)
//!   link            the encrypted link between servers (replica pairs, gateways)
//!   upload_token    media upload tokens: the core issues, xb-media checks
//!   prom            Prometheus text format, for every program's /metrics

pub mod codec;
pub mod frame;
pub mod link;
pub mod prom;
pub mod tokens;
pub mod transport;
pub mod upload_token;
pub mod util;

#[cfg(test)]
mod fuzz;

//! What the server is doing, for monitoring (Prometheus text format), on a
//! separate port that only this machine reaches by default (XB_METRICS_ADDR,
//! 127.0.0.1:9091; "none" turns it off):
//!
//!   GET /metrics   counters and gauges (scraped every ~15 s by Prometheus)
//!   GET /health    200 "ok" while the process answers (liveness)
//!   GET /ready     200 when it should get traffic, 503 when it shouldn't:
//!                  nearly full, saving to disk has been failing, or draining
//!   POST /drain    drain before a restart (like Ctrl+C): stop listening, tell
//!                  every client to come back over the next XB_DRAIN_SPREAD_S
//!                  seconds, save everything, exit

use crate::server::Hub;
use crate::util::now_ms;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

pub fn router(hub: Arc<Hub>) -> Router {
    Router::new().route("/metrics", get(metrics)).route("/health", get(|| async { "ok" })).route("/ready", get(ready)).route("/drain", axum::routing::post(drain)).with_state(hub)
}

/// Starts a drain (main() does the rest).
async fn drain(State(hub): State<Arc<Hub>>) -> &'static str {
    hub.draining.cancel();
    "draining\n"
}

/// One metric in Prometheus' text format (also used by xb-gateway and xb-media).
pub fn write_metric(out: &mut String, name: &str, kind: &str, help: &str, value: f64) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}");
}

/// Ready for traffic? Not when nearly full (let a load balancer send new
/// phones elsewhere), and not when saving has failed in the last 30 s.
async fn ready(State(hub): State<Arc<Hub>>) -> Response {
    let open = hub.stats.open.load(Relaxed);
    let full = open as f64 >= hub.cfg.max_connections as f64 * 0.95;
    let failing = now_ms().saturating_sub(hub.stats.last_save_error_ms.load(Relaxed)) < 30_000;
    let why = match (full, failing) {
        _ if hub.draining.is_cancelled() => "draining for a restart",
        (true, _) => "nearly full",
        (_, true) => "saving to disk is failing",
        _ => return "ready".into_response(),
    };
    (StatusCode::SERVICE_UNAVAILABLE, why).into_response()
}

async fn metrics(State(hub): State<Arc<Hub>>) -> Response {
    let s = &hub.stats;
    let mut out = String::with_capacity(4096);
    let mut metric = |name: &str, kind: &str, help: &str, value: f64| write_metric(&mut out, name, kind, help, value);
    // Gauges that need a look at every shard (64 short locks).
    let (mut users, mut devices, mut registered, mut queued, mut in_mem, mut mem_bytes) = (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
    let mut skipped = 0u64;
    for i in 0..hub.shard_count() {
        let sh = hub.shard(i);
        let (u, d) = sh.online();
        users += u;
        devices += d;
        registered += sh.store.users.len();
        let (n, m, b) = sh.store.queue_memory();
        queued += n;
        in_mem += m;
        mem_bytes += b;
        skipped = sh.store.counters.queue_skipped.load(Relaxed);
    }
    metric("xb_connections_open", "gauge", "Sockets open now (handshaking or logged in).", s.open.load(Relaxed) as f64);
    metric("xb_connections_max", "gauge", "The connection cap (XB_MAX_CONNECTIONS).", hub.cfg.max_connections as f64);
    metric("xb_connections_total", "counter", "Connections accepted since start.", s.connections.load(Relaxed) as f64);
    metric("xb_users_online", "gauge", "Users with at least one logged-in connection.", users as f64);
    metric("xb_devices_online", "gauge", "Devices receiving their queue live.", devices as f64);
    metric("xb_users_registered", "gauge", "Accounts on this server.", registered as f64);
    metric("xb_offline_queued", "gauge", "Stanzas waiting in offline queues.", queued as f64);
    metric("xb_offline_in_memory", "gauge", "Queued stanzas held in memory (the rest are on disk only).", in_mem as f64);
    metric("xb_offline_memory_bytes", "gauge", "Bytes of queued stanzas held in memory.", mem_bytes as f64);
    metric("xb_queue_written_total", "counter", "Queued stanzas written to disk (still undelivered after the write delay).", s.queue_written.load(Relaxed) as f64);
    metric("xb_queue_skipped_total", "counter", "Queued stanzas delivered before they were written: disk work saved.", skipped as f64);
    metric("xb_queue_deleted_total", "counter", "Written stanzas deleted from disk after delivery.", s.queue_deleted.load(Relaxed) as f64);
    metric("xb_queue_evicted_bytes_total", "counter", "Bytes of cold queued stanzas dropped from memory.", s.queue_evicted_bytes.load(Relaxed) as f64);
    metric("xb_stanzas_in_total", "counter", "Stanzas received.", s.stanzas_in.load(Relaxed) as f64);
    metric("xb_messages_total", "counter", "Messages routed.", s.messages.load(Relaxed) as f64);
    metric("xb_bytes_in_total", "counter", "Bytes received on chat connections.", hub.bytes_in.load(Relaxed) as f64);
    metric("xb_bytes_out_total", "counter", "Bytes sent on chat connections.", hub.bytes_out.load(Relaxed) as f64);
    metric("xb_refused_total", "counter", "Sockets refused: over the cap, or no handshake slot.", s.refused.load(Relaxed) as f64);
    metric("xb_busy_total", "counter", "Logins told busy (login rate limit).", s.busy.load(Relaxed) as f64);
    metric("xb_slow_total", "counter", "Connections closed for reading too slowly.", s.slow.load(Relaxed) as f64);
    metric("xb_panics_total", "counter", "Handler panics (caught; the connection was closed).", s.panics.load(Relaxed) as f64);
    metric("xb_pushes_total", "counter", "Push wake-ups sent.", s.pushes.load(Relaxed) as f64);
    metric("xb_push_failures_total", "counter", "Push wake-ups that failed.", s.push_failures.load(Relaxed) as f64);
    metric("xb_saves_total", "counter", "Database transactions committed.", s.saves.load(Relaxed) as f64);
    metric("xb_save_records_total", "counter", "Records written or deleted by those transactions.", s.save_records.load(Relaxed) as f64);
    metric("xb_save_seconds_total", "counter", "Time spent committing.", s.save_ns.load(Relaxed) as f64 / 1e9);
    metric("xb_save_errors_total", "counter", "Failed commits (retried).", s.save_errors.load(Relaxed) as f64);
    metric("xb_lock_acquisitions_total", "counter", "Shard locks taken.", hub.locks.load(Relaxed) as f64);
    metric("xb_lock_wait_seconds_total", "counter", "Time spent waiting for shard locks.", hub.lock_wait_ns.load(Relaxed) as f64 / 1e9);
    metric("xb_lock_hold_seconds_total", "counter", "Time shard locks were held.", hub.lock_hold_ns.load(Relaxed) as f64 / 1e9);
    let _ = writeln!(out, "# HELP xb_build_info The server's version.\n# TYPE xb_build_info gauge\nxb_build_info{{version=\"{}\"}} 1", env!("CARGO_PKG_VERSION"));
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}

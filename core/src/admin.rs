//! Test hooks (XB_TEST_ADMIN=1 only): what the Node tests read from
//! `chat.store`, over HTTP, so the same tests can check the Rust server.

use crate::push::Pusher;
use crate::server::Hub;
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn call(hub: &Arc<Hub>, f: &str, a: String, b: String) -> Value {
    match f {
        "pendingCount" => json!(hub.user(&a).store.pending_count(&a)),
        "keyCount" => {
            let (registered, count) = hub.user(&a).store.key_count(&a, &b);
            json!({ "registered": registered, "count": count })
        }
        "keyDevices" => json!(hub.user(&a).store.key_devices(&a)),
        "pushOf" => match hub.user(&a).store.push_of(&a, &b) {
            Some((key, token)) => json!({ "key": key, "token": token }),
            None => Value::Null,
        },
        "devices" => {
            let st = hub.user(&a);
            json!(st.store.list_devices(&a).into_iter().map(|(k, d)| json!({ "key": k, "no": d.no, "e2e": d.e2e, "name": d.name })).collect::<Vec<_>>())
        }
        // Write-back counters, and where queued stanzas are now.
        "queueStats" => {
            let st = &hub.stats;
            let (mut queued, mut in_mem) = (0, 0);
            let mut skipped = 0;
            for i in 0..hub.shard_count() {
                let sh = hub.shard(i);
                let (n, m, _) = sh.store.queue_memory();
                queued += n;
                in_mem += m;
                skipped = sh.store.counters.queue_skipped.load(std::sync::atomic::Ordering::Relaxed);
            }
            let o = std::sync::atomic::Ordering::Relaxed;
            json!({ "written": st.queue_written.load(o), "deleted": st.queue_deleted.load(o), "skipped": skipped, "queued": queued, "inMemory": in_mem })
        }
        // Cold queues out of memory now (normally every minute).
        "evictCold" => {
            crate::server::evict_cold(hub);
            json!(true)
        }
        "hasUser" => json!(hub.user(&a).store.has_user(&a)),
        "pushes" => match hub.push.as_deref() {
            Some(Pusher::Fake(list)) => json!(list.lock().unwrap().iter().map(|(t, d)| json!({ "token": t, "data": { "xb": d } })).collect::<Vec<_>>()),
            _ => json!([]),
        },
        "clearPushes" => {
            if let Some(Pusher::Fake(list)) = hub.push.as_deref() {
                list.lock().unwrap().clear();
            }
            json!(true)
        }
        // Did the server receive or store these bytes (base64)? For "the server
        // sees only ciphertext" tests.
        "saw" => {
            use base64::Engine;
            let needle = base64::engine::general_purpose::STANDARD.decode(&a).unwrap_or_default();
            let has = |hay: &[u8]| !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle.as_slice());
            let seen = hub.seen.lock().unwrap().iter().any(|b| has(b));
            json!(seen || (0..hub.shard_count()).any(|i| has(hub.shard(i).store.snapshot().as_bytes())))
        }
        "flush" => {
            crate::db::flush_all(hub).await;
            json!(true)
        }
        _ => json!({ "error": format!("unknown fn {f}") }),
    }
}

//! Token dictionaries and type markers, FunXMPP layout (protocol/tokens.js).
//!
//! The index of a word IS its wire value: these lists must stay exactly the
//! same as the Node ones. Only append, never reorder.

pub const LIST_EMPTY: u8 = 0;
pub const DICTIONARY_0: u8 = 236;
pub const DICTIONARY_3: u8 = 239;
pub const AD_JID: u8 = 247;
pub const LIST_8: u8 = 248;
pub const LIST_16: u8 = 249;
pub const JID_PAIR: u8 = 250;
pub const HEX_8: u8 = 251;
pub const BINARY_8: u8 = 252;
pub const BINARY_20: u8 = 253;
pub const BINARY_32: u8 = 254;
pub const NIBBLE_8: u8 = 255;

pub const SINGLE_BYTE_MAX: u8 = 236;

pub const SINGLE_BYTE_TOKENS: &[&str] = &[
    "", // 0 = LIST_EMPTY
    "xmlstreamstart",
    "xmlstreamend",
    // servers (JID domains)
    "s.xb",
    "g.xb",
    // stanza tags
    "message",
    "receipt",
    "ack",
    "iq",
    "presence",
    "chatstate",
    "auth",
    "success",
    "failure",
    "error",
    "stream:error",
    "body",
    "media",
    "ping",
    "group",
    "participant",
    "create",
    "add",
    "remove",
    "leave",
    "list",
    "lastseen",
    "media_conn",
    "composing",
    "paused",
    "offline",
    "query",
    "subject",
    "conflict",
    // attribute names
    "id",
    "to",
    "from",
    "type",
    "class",
    "t",
    "user",
    "pass",
    "mode",
    "reason",
    "xmlns",
    "jid",
    "url",
    "mimetype",
    "size",
    "sha256",
    "key",
    "auth_token",
    "host",
    "ttl",
    "creator",
    "count",
    "code",
    "text",
    "name",
    "caption",
    "edit",
    "quoted",
    "device",
    // attribute values
    "chat",
    "delivered",
    "read",
    "get",
    "set",
    "result",
    "login",
    "register",
    "available",
    "unavailable",
    "subscribe",
    "unsubscribe",
    "not-authorized",
    "bad-request",
    "item-not-found",
    "forbidden",
    "not-member",
    "w:p",
    "g",
    "status",
    "true",
    "false",
    "admin",
    "member",
    "image/jpeg",
    "image/png",
    "video/mp4",
    "audio/ogg",
];

/// Rarer words: two bytes each (DICTIONARY_n, index).
pub const DOUBLE_BYTE_TOKENS: [&[&str]; 4] = [
    &[
        // DICTIONARY_0: error codes, less common values
        "internal-error",
        "rate-limit",
        "timeout",
        "user-exists",
        "invalid-user",
        "image",
        "video",
        "audio",
        "document",
        "sticker",
        "location",
        "contact",
        "application/pdf",
        "application/octet-stream",
        "image/webp",
        "image/gif",
        "audio/mpeg",
        "audio/mp4",
        "video/quicktime",
        "text/plain",
        "none",
        "version",
        "seconds",
        "duration",
        "width",
        "height",
        "thumbnail",
        "description",
        "announcement",
        "locked",
        "promote",
        "demote",
        "invite",
    ],
    &[],
    &[],
    &[],
];

pub const USER_SERVER: &str = "s.xb";
pub const GROUP_SERVER: &str = "g.xb";

/// "XB" + protocol version 2 + dictionary version 1. Also the Noise prologue.
pub const HEADER: [u8; 4] = [b'X', b'B', 2, 1];

/// Where a word is: (None, i) = single byte i; (Some(d), i) = dictionary d, index i.
pub fn token_index(s: &str) -> Option<(Option<u8>, u8)> {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    // Built once, the first time it's needed. OnceLock makes that thread-safe.
    static INDEX: OnceLock<HashMap<&'static str, (Option<u8>, u8)>> = OnceLock::new();
    let map = INDEX.get_or_init(|| {
        let mut m = HashMap::new();
        for (i, t) in SINGLE_BYTE_TOKENS.iter().enumerate().skip(3) {
            m.insert(*t, (None, i as u8));
        }
        for (d, words) in DOUBLE_BYTE_TOKENS.iter().enumerate() {
            for (i, t) in words.iter().enumerate() {
                m.insert(*t, (Some(d as u8), i as u8));
            }
        }
        m
    });
    map.get(s).copied()
}

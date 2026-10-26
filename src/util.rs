//! Small helpers shared by the modules.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since 1970 (JavaScript's Date.now()).
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Seconds since 1970.
pub fn now_s() -> u64 {
    now_ms() / 1000
}

/// JavaScript's new Date().toISOString(), for log lines.
pub fn iso_now() -> String {
    let ms = now_ms();
    let secs = ms / 1000;
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    // Days since 1970 -> civil date (Howard Hinnant's algorithm).
    let z = (secs / 86400) as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + (mo <= 2) as i64;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{:03}Z", ms % 1000)
}

/// One log line: `2026-10-22T10:00:00.000Z [kind] text`, like the Node server.
pub fn log(kind: &str, text: impl AsRef<str>) {
    println!("{} [{kind}] {}", iso_now(), text.as_ref());
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex(&Sha256::digest(data))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constant-time comparison (crypto.timingSafeEqual).
pub fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Writes a file only the owner can read (mode 0600 on Unix).
pub fn write_private(path: &str, data: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Number of UTF-16 code units, which is what JavaScript's `.length` counts.
pub fn js_len(s: &str) -> usize {
    s.encode_utf16().count()
}

/// The first `n` UTF-16 units of `s` (JavaScript's `.slice(0, n)`), never
/// cutting a character in half.
pub fn js_slice(s: &str, n: usize) -> String {
    let mut units = 0;
    let mut out = String::new();
    for c in s.chars() {
        units += c.len_utf16();
        if units > n {
            break;
        }
        out.push(c);
    }
    out
}

/// JavaScript object key order: keys that are array indexes (0..2^32-2)
/// first, ascending, then the rest in insertion order.
pub fn js_key_order<'a, I: IntoIterator<Item = &'a String>>(keys: I) -> Vec<&'a String> {
    let is_index = |k: &str| !k.is_empty() && k.len() <= 10 && k.bytes().all(|b| b.is_ascii_digit()) && (k == "0" || !k.starts_with('0')) && k.parse::<u64>().map(|n| n < 4294967295).unwrap_or(false);
    let all: Vec<&String> = keys.into_iter().collect();
    let mut idx: Vec<&String> = all.iter().copied().filter(|k| is_index(k)).collect();
    idx.sort_by_key(|k| k.parse::<u64>().unwrap());
    idx.extend(all.into_iter().filter(|k| !is_index(k)));
    idx
}

#[cfg(test)]
mod tests {
    #[test]
    fn iso() {
        let s = super::iso_now();
        assert_eq!(s.len(), 24);
        assert!(s.ends_with('Z'));
    }
}

//! Binary stanzas, WhatsApp's FunXMPP layout (PROTOCOL.md §3).
//!
//! A node is `tag`, attributes and optional content (bytes, text or child
//! nodes). On the wire:  LIST(n) tag k1 v1 k2 v2 … [content],
//! n = 1 + 2·attrs + (content ? 1 : 0). Strings are written as cheaply as
//! possible: a dictionary token, a JID pair, packed digits/hex, or raw UTF-8.

use crate::tokens::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Content {
    Bytes(Vec<u8>),
    Text(String),
    Nodes(Vec<Node>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub tag: String,
    /// In order; attribute names are unique.
    pub attrs: Vec<(String, String)>,
    pub content: Option<Content>,
}

impl Node {
    pub fn new(tag: &str) -> Node {
        Node { tag: tag.to_string(), attrs: Vec::new(), content: None }
    }

    /// Adds an attribute (builder style: `Node::new("ack").attr("id", id)`).
    pub fn attr(mut self, k: &str, v: impl ToString) -> Node {
        self.set(k, v.to_string());
        self
    }

    /// Adds an attribute only when there is a value (JS: `undefined` is left out).
    pub fn opt<V: ToString>(self, k: &str, v: Option<V>) -> Node {
        match v {
            Some(v) => self.attr(k, v),
            None => self,
        }
    }

    pub fn set(&mut self, k: &str, v: String) {
        match self.attrs.iter_mut().find(|(key, _)| key == k) {
            Some(slot) => slot.1 = v,
            None => self.attrs.push((k.to_string(), v)),
        }
    }

    pub fn remove(&mut self, k: &str) {
        self.attrs.retain(|(key, _)| key != k);
    }

    pub fn nodes(mut self, children: Vec<Node>) -> Node {
        self.content = Some(Content::Nodes(children));
        self
    }

    pub fn bytes(mut self, b: Vec<u8>) -> Node {
        self.content = Some(Content::Bytes(b));
        self
    }

    pub fn get(&self, k: &str) -> Option<&str> {
        self.attrs.iter().find(|(key, _)| key == k).map(|(_, v)| v.as_str())
    }

    /// The child nodes (empty when the content is not a list).
    pub fn kids(&self) -> &[Node] {
        match &self.content {
            Some(Content::Nodes(n)) => n,
            _ => &[],
        }
    }

    pub fn has_list(&self) -> bool {
        matches!(self.content, Some(Content::Nodes(_)))
    }

    pub fn child(&self, tag: &str) -> Option<&Node> {
        self.kids().iter().find(|c| c.tag == tag)
    }

    pub fn children<'a>(&'a self, tag: &'a str) -> impl Iterator<Item = &'a Node> + 'a {
        self.kids().iter().filter(move |c| c.tag == tag)
    }

    pub fn first(&self) -> Option<&Node> {
        self.kids().first()
    }

    pub fn content_bytes(&self) -> Option<&[u8]> {
        match &self.content {
            Some(Content::Bytes(b)) => Some(b),
            _ => None,
        }
    }

    /// Text content: bytes as UTF-8, or a string; anything else is "".
    pub fn text(&self) -> String {
        match &self.content {
            Some(Content::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
            Some(Content::Text(s)) => s.clone(),
            _ => String::new(),
        }
    }
}

// ---------------------------------------------------------------- encoding

const MAX_PACKED_LEN: usize = 254;
const NIBBLE_CHARS: &[u8] = b"0123456789-.";
const HEX_CHARS: &[u8] = b"0123456789ABCDEF";

pub fn encode(node: &Node) -> Vec<u8> {
    let mut w = Vec::with_capacity(64);
    write_node(&mut w, node);
    w
}

fn write_list_start(w: &mut Vec<u8>, size: usize) {
    if size == 0 {
        w.push(LIST_EMPTY);
    } else if size < 256 {
        w.push(LIST_8);
        w.push(size as u8);
    } else {
        w.push(LIST_16);
        w.extend_from_slice(&(size as u16).to_be_bytes());
    }
}

fn write_bytes(w: &mut Vec<u8>, b: &[u8]) {
    let n = b.len();
    if n < 256 {
        w.push(BINARY_8);
        w.push(n as u8);
    } else if n < 1 << 20 {
        w.push(BINARY_20);
        w.extend_from_slice(&[((n >> 16) & 0x0f) as u8, (n >> 8) as u8, n as u8]);
    } else {
        w.push(BINARY_32);
        w.extend_from_slice(&(n as u32).to_be_bytes());
    }
    w.extend_from_slice(b);
}

fn write_packed(w: &mut Vec<u8>, tag: u8, alphabet: &[u8], s: &[u8]) {
    let odd = s.len() % 2;
    let n = s.len().div_ceil(2);
    w.push(tag);
    w.push(((odd << 7) | n) as u8);
    let idx = |c: u8| alphabet.iter().position(|&a| a == c).unwrap() as u8;
    for i in 0..n {
        let hi = idx(s[2 * i]);
        let lo = if 2 * i + 1 < s.len() { idx(s[2 * i + 1]) } else { 15 };
        w.push((hi << 4) | lo);
    }
}

/// user:device@s.xb -> (user, device)
fn ad_jid(s: &str) -> Option<(&str, u8)> {
    let colon = s.find([':', '@'])?;
    if colon == 0 || s.as_bytes()[colon] != b':' {
        return None;
    }
    let rest = &s[colon + 1..];
    let at = rest.find('@')?;
    let digits = &rest[..at];
    if digits.is_empty() || digits.len() > 3 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if &rest[at + 1..] != USER_SERVER {
        return None;
    }
    let n: u32 = digits.parse().ok()?;
    if n > 255 || n.to_string() != digits {
        return None;
    }
    Some((&s[..colon], n as u8))
}

fn write_string(w: &mut Vec<u8>, s: &str, allow_jid: bool) {
    if s.is_empty() {
        w.push(LIST_EMPTY);
        return;
    }
    if let Some((dict, index)) = token_index(s) {
        if let Some(d) = dict {
            w.push(DICTIONARY_0 + d);
        }
        w.push(index);
        return;
    }
    if allow_jid {
        if let Some((user, device)) = ad_jid(s) {
            w.extend_from_slice(&[AD_JID, 0, device]);
            write_string(w, user, false);
            return;
        }
        if let Some(at) = s.find('@') {
            if at > 0 && Some(at) == s.rfind('@') && at < s.len() - 1 {
                w.push(JID_PAIR);
                write_string(w, &s[..at], false);
                write_string(w, &s[at + 1..], false);
                return;
            }
        }
    }
    let b = s.as_bytes();
    if b.len() <= MAX_PACKED_LEN {
        if b.iter().all(|c| NIBBLE_CHARS.contains(c)) {
            return write_packed(w, NIBBLE_8, NIBBLE_CHARS, b);
        }
        if b.iter().all(|c| HEX_CHARS.contains(c)) {
            return write_packed(w, HEX_8, HEX_CHARS, b);
        }
    }
    write_bytes(w, b);
}

fn write_node(w: &mut Vec<u8>, node: &Node) {
    let has_content = node.content.is_some();
    write_list_start(w, 1 + node.attrs.len() * 2 + has_content as usize);
    write_string(w, &node.tag, true);
    for (k, v) in &node.attrs {
        write_string(w, k, true);
        write_string(w, v, true);
    }
    match &node.content {
        None => {}
        Some(Content::Bytes(b)) => write_bytes(w, b),
        Some(Content::Nodes(children)) => {
            write_list_start(w, children.len());
            for c in children {
                write_node(w, c);
            }
        }
        // "" as a string would read back as an empty list
        Some(Content::Text(s)) if s.is_empty() => write_bytes(w, &[]),
        Some(Content::Text(s)) => write_string(w, s, true),
    }
}

// ---------------------------------------------------------------- decoding

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

type R<T> = Result<T, String>;

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> R<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err("Unexpected end of stanza".into());
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn byte(&mut self) -> R<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn int16(&mut self) -> R<usize> {
        let b = self.bytes(2)?;
        Ok(((b[0] as usize) << 8) | b[1] as usize)
    }

    fn int20(&mut self) -> R<usize> {
        let b = self.bytes(3)?;
        Ok((((b[0] & 0x0f) as usize) << 16) | ((b[1] as usize) << 8) | b[2] as usize)
    }

    fn int32(&mut self) -> R<usize> {
        let b = self.bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
    }

    fn list_size(&mut self, tag: u8) -> R<usize> {
        match tag {
            LIST_EMPTY => Ok(0),
            LIST_8 => Ok(self.byte()? as usize),
            LIST_16 => self.int16(),
            _ => Err(format!("Expected list, got 0x{tag:x}")),
        }
    }

    fn packed(&mut self, alphabet: &[u8]) -> R<String> {
        let head = self.byte()?;
        let odd = (head >> 7) as usize;
        let n = (head & 0x7f) as usize;
        let bytes = self.bytes(n)?;
        let len = (n * 2).saturating_sub(odd);
        let mut s = String::with_capacity(len);
        for i in 0..len {
            let v = if i % 2 == 0 { bytes[i >> 1] >> 4 } else { bytes[i >> 1] & 0x0f };
            let ch = *alphabet.get(v as usize).ok_or("Bad packed string")?;
            s.push(ch as char);
        }
        Ok(s)
    }

    fn binary(&mut self, tag: u8) -> R<Option<&'a [u8]>> {
        let n = match tag {
            BINARY_8 => self.byte()? as usize,
            BINARY_20 => self.int20()?,
            BINARY_32 => self.int32()?,
            _ => return Ok(None),
        };
        Ok(Some(self.bytes(n)?))
    }

    fn string(&mut self, tag: u8) -> R<String> {
        if tag == LIST_EMPTY {
            return Ok(String::new());
        }
        if tag < SINGLE_BYTE_MAX {
            return SINGLE_BYTE_TOKENS.get(tag as usize).map(|t| t.to_string()).ok_or(format!("Unknown token {tag}"));
        }
        if (DICTIONARY_0..=DICTIONARY_3).contains(&tag) {
            let index = self.byte()?;
            return DOUBLE_BYTE_TOKENS[(tag - DICTIONARY_0) as usize].get(index as usize).map(|t| t.to_string()).ok_or(format!("Unknown token {tag}/{index}"));
        }
        match tag {
            AD_JID => {
                let agent = self.byte()?;
                let device = self.byte()?;
                if agent != 0 {
                    return Err(format!("Unsupported JID agent {agent}"));
                }
                let t = self.byte()?;
                let user = self.string(t)?;
                Ok(format!("{user}:{device}@{USER_SERVER}"))
            }
            JID_PAIR => {
                let t = self.byte()?;
                let user = self.string(t)?;
                let t = self.byte()?;
                let server = self.string(t)?;
                Ok(format!("{user}@{server}"))
            }
            NIBBLE_8 => self.packed(NIBBLE_CHARS),
            HEX_8 => self.packed(HEX_CHARS),
            _ => match self.binary(tag)? {
                Some(b) => Ok(String::from_utf8_lossy(b).into_owned()),
                None => Err(format!("Expected string, got 0x{tag:x}")),
            },
        }
    }

    fn node(&mut self, depth: usize) -> R<Node> {
        if depth > 32 {
            return Err("Stanza nested too deeply".into());
        }
        let t = self.byte()?;
        let size = self.list_size(t)?;
        if size == 0 {
            return Err("Empty node".into());
        }
        let t = self.byte()?;
        let tag = self.string(t)?;
        if tag.is_empty() {
            return Err("Empty tag".into());
        }
        let mut node = Node::new(&tag);
        for _ in 0..(size - 1) >> 1 {
            let t = self.byte()?;
            let k = self.string(t)?;
            let t = self.byte()?;
            let v = self.string(t)?;
            node.set(&k, v);
        }
        if size % 2 == 0 {
            let t = self.byte()?;
            if t == LIST_EMPTY || t == LIST_8 || t == LIST_16 {
                let n = self.list_size(t)?;
                let mut kids = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    kids.push(self.node(depth + 1)?);
                }
                node.content = Some(Content::Nodes(kids));
            } else if let Some(b) = self.binary(t)? {
                node.content = Some(Content::Bytes(b.to_vec()));
            } else {
                node.content = Some(Content::Text(self.string(t)?));
            }
        }
        Ok(node)
    }
}

pub fn decode(buf: &[u8]) -> Result<Node, String> {
    let mut r = Reader { buf, pos: 0 };
    let node = r.node(0)?;
    if r.pos != buf.len() {
        return Err("Trailing bytes after stanza".into());
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let n = Node::new("message")
            .attr("id", "3EB0C431C26A1916")
            .attr("to", "bob@s.xb")
            .attr("type", "chat")
            .attr("t", 1700000000)
            .attr("participant", "919812345678:3@s.xb")
            .nodes(vec![Node::new("body").bytes(b"hi".to_vec()), Node::new("x").nodes(vec![])]);
        let b = encode(&n);
        assert_eq!(decode(&b).unwrap(), n);
    }

    #[test]
    fn same_bytes_as_node() {
        // <message id="3EB0C431C26A1916" to="bob@s.xb" type="chat"><body>hi</body></message>:
        // 33 bytes from the reference codec.
        let n = Node::new("message").attr("id", "3EB0C431C26A1916").attr("to", "bob@s.xb").attr("type", "chat").nodes(vec![Node::new("body").bytes(b"hi".to_vec())]);
        assert_eq!(crate::util::hex(&encode(&n)), "f8080522fb083eb0c431c26a191623fafc03626f6203253ff801f80210fc026869");
    }

    /// Stanzas encoded by the reference codec (tests/fixtures/codec.hex): we must
    /// read them, and write them back byte for byte.
    #[test]
    fn node_fixtures_round_trip() {
        let fixtures = include_str!("../tests/fixtures/codec.hex");
        for line in fixtures.lines().filter(|l| !l.is_empty()) {
            let bytes: Vec<u8> = (0..line.len()).step_by(2).map(|i| u8::from_str_radix(&line[i..i + 2], 16).unwrap()).collect();
            let node = decode(&bytes).unwrap_or_else(|e| panic!("{e}: {}", &line[..line.len().min(60)]));
            assert_eq!(encode(&node), bytes, "re-encoding <{}>", node.tag);
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[LIST_8, 1, 5, 0]).is_err()); // trailing byte
        assert!(decode(&[LIST_8, 2, 5, BINARY_8, 10]).is_err()); // short
    }
}

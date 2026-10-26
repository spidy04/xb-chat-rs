//! Frames: a 3-byte big-endian length, then the payload (PROTOCOL.md §2).
//!
//! The network delivers bytes in arbitrary pieces: half a frame, or several
//! frames glued together. `FrameDecoder` collects the pieces and hands out
//! whole frames. It never touches the network, so it's easy to test.

/// The biggest frame we accept: 1 MB, the same limit as the Node server.
pub const MAX_FRAME: usize = 1024 * 1024;

/// What can go wrong with frames.
/// `#[derive(Debug, PartialEq)]` asks the compiler to write the code that
/// prints it ({:?}) and compares it (==), so tests can check errors.
#[derive(Debug, PartialEq)]
pub enum FrameError {
    /// The other side announced a frame bigger than we accept.
    TooLarge(usize),
}

/// Puts the 3-byte length in front of a payload, ready to send.
///
/// `&[u8]` is a *slice*: a borrowed view of some bytes (from a Vec, an
/// array, a string…). We only need to read the payload, so we borrow it.
pub fn encode(payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    let n = payload.len();
    if n > MAX_FRAME {
        return Err(FrameError::TooLarge(n));
    }
    // with_capacity: reserve room for 3 + n bytes up front.
    let mut out = Vec::with_capacity(3 + n);
    out.push((n >> 16) as u8); // `as u8` keeps the lowest 8 bits
    out.push((n >> 8) as u8);
    out.push(n as u8);
    out.extend_from_slice(payload);
    Ok(out)
}

/// Collects bytes as they arrive and hands out complete frames.
pub struct FrameDecoder {
    /// Bytes received but not yet handed out as frames.
    buf: Vec<u8>,
    /// The biggest frame this decoder accepts.
    max: usize,
}

// The methods of FrameDecoder.
impl FrameDecoder {
    /// A new, empty decoder. `Self` means "the type we're implementing", FrameDecoder.
    pub fn new(max: usize) -> Self {
        FrameDecoder { buf: Vec::new(), max }
    }

    /// Adds bytes that just arrived from the network.
    /// `&mut self`: this method changes the decoder (its buffer grows).
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// How many bytes are waiting (only used by tests).
    #[cfg(test)]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// YOUR TURN: hand out the next complete frame, if there is one.
    ///
    /// Return:
    ///   Ok(None)                    fewer than 3 bytes, or the frame isn't complete yet
    ///   Err(FrameError::TooLarge(n)) the length says n bytes and n > self.max
    ///   Ok(Some(payload))           a whole frame; also remove its 3 + n bytes from self.buf
    ///
    /// Steps (it's lesson 1's read_frame again, but on self.buf instead of a socket):
    ///   1. if self.buf.len() < 3 → return Ok(None)
    ///   2. n = the 3 length bytes, exactly like lesson 1 (self.buf[0], self.buf[1], self.buf[2])
    ///   3. if n > self.max → return Err(FrameError::TooLarge(n))
    ///   4. if self.buf.len() < 3 + n → return Ok(None)
    ///   5. let payload = self.buf[3..3 + n].to_vec();   (copy bytes 3 up to 3+n)
    ///   6. self.buf.drain(..3 + n);                       (remove the first 3+n bytes)
    ///   7. Ok(Some(payload))
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        if self.buf.len() < 3 {
            return Ok(None);
        }

        // read the length from the first 3 len
        let n = ((self.buf[0] as usize) << 16) | ((self.buf[1] as usize) << 8) | (self.buf[2] as usize);

        // refuse frames that are too big
        if n > self.max {
            return Err(FrameError::TooLarge(n));
        }

        // the length is known but is the whole frame here?
        if self.buf.len() < 3 + n {
            return Ok(None);
        }

        // copy the payload out (the bytes after the 3 length bytes.)
        let payload = self.buf[3..3 + n].to_vec();

        // remove this frame from the buffer, so the next call starts at the next frame.
        self.buf.drain(..3 + n);

        // Empty again: give the memory back (an idle connection keeps none).
        if self.buf.is_empty() && self.buf.capacity() > 4096 {
            self.buf = Vec::new();
        }

        Ok(Some(payload))
    }
}

// Tests: compiled only for `cargo test`. They check the same things as the
// Node test "frame decoder reassembles split and merged chunks".
#[cfg(test)]
mod tests {
    use super::*; // everything from the module above

    #[test]
    fn encode_puts_the_length_first() {
        // "hello" is 5 bytes: 00 00 05 h e l l o
        assert_eq!(encode(b"hello").unwrap(), b"\x00\x00\x05hello".to_vec());
        // 1000 bytes: 0x0003e8
        assert_eq!(&encode(&[1u8; 1000]).unwrap()[..3], &[0x00, 0x03, 0xe8]);
    }

    #[test]
    fn one_frame_in_one_piece() {
        let mut d = FrameDecoder::new(MAX_FRAME);
        d.push(&encode(b"hello").unwrap());
        assert_eq!(d.next_frame(), Ok(Some(b"hello".to_vec())));
        assert_eq!(d.next_frame(), Ok(None)); // nothing left
        assert_eq!(d.buffered(), 0);
    }

    #[test]
    fn not_enough_bytes_yet() {
        let mut d = FrameDecoder::new(MAX_FRAME);
        d.push(&[0x00, 0x00]); // only 2 of the 3 length bytes
        assert_eq!(d.next_frame(), Ok(None));
        d.push(&[0x05, b'h', b'e']); // length complete, payload not yet
        assert_eq!(d.next_frame(), Ok(None));
        d.push(b"llo");
        assert_eq!(d.next_frame(), Ok(Some(b"hello".to_vec())));
    }

    #[test]
    fn split_and_merged_pieces() {
        // Two frames glued together, then cut into 7-byte pieces, like the Node test.
        let mut all = encode(b"hello").unwrap();
        all.extend(encode(&[1u8; 1000]).unwrap());
        let mut d = FrameDecoder::new(MAX_FRAME);
        let mut frames = Vec::new();
        for piece in all.chunks(7) {
            d.push(piece);
            // `while let`: keep taking frames as long as there are complete ones.
            while let Ok(Some(f)) = d.next_frame() {
                frames.push(f);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], b"hello");
        assert_eq!(frames[1].len(), 1000);
    }

    #[test]
    fn too_large_is_refused() {
        let mut d = FrameDecoder::new(10);
        d.push(&encode(&[0u8; 11]).unwrap());
        assert_eq!(d.next_frame(), Err(FrameError::TooLarge(11)));
    }

    #[test]
    fn empty_frame_is_allowed() {
        let mut d = FrameDecoder::new(MAX_FRAME);
        d.push(&[0, 0, 0]);
        assert_eq!(d.next_frame(), Ok(Some(Vec::new())));
    }
}

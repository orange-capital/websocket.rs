#![doc(hidden)]

pub struct Frame<'a> {
    pub fin: bool,
    pub rsv: u8,
    pub opcode: u8,
    pub data: &'a [u8],
}

impl<'a> Frame<'a> {
    #[inline]
    pub fn encode_without_mask(self) -> Vec<u8> {
        let mut buf = Vec::<u8>::with_capacity(10 + self.data.len());
        unsafe {
            let dist = buf.as_mut_ptr();
            let head_len = self.encode_header_unchecked(dist, 0);
            std::ptr::copy_nonoverlapping(self.data.as_ptr(), dist.add(head_len), self.data.len());
            buf.set_len(head_len + self.data.len());
        }
        buf
    }

    #[inline]
    pub fn encode_with(self, mask: [u8; 4]) -> Vec<u8> {
        let data_len = self.data.len();
        let mut buf = Vec::<u8>::with_capacity(14 + data_len);
        unsafe {
            let dist = buf.as_mut_ptr();
            let head_len = self.encode_header_unchecked(dist, 0x80);

            let [a, b, c, d] = mask;
            dist.add(head_len).write(a);
            dist.add(head_len + 1).write(b);
            dist.add(head_len + 2).write(c);
            dist.add(head_len + 3).write(d);

            std::ptr::copy_nonoverlapping(self.data.as_ptr(), dist.add(head_len + 4), data_len);
            buf.set_len(head_len + 4 + data_len);
        }
        // Mask the payload in place, word-at-a-time (see `apply_mask`).
        let payload_start = buf.len() - data_len;
        apply_mask(&mut buf[payload_start..], mask);
        buf
    }

    /// # SEAFTY
    ///
    /// - `dist` must be valid for writes of 10 bytes.
    pub(crate) unsafe fn encode_header_unchecked(&self, dist: *mut u8, mask_bit: u8) -> usize {
        dist.write(((self.fin as u8) << 7) | (self.rsv << 4) | self.opcode);
        if self.data.len() < 126 {
            dist.add(1).write(mask_bit | self.data.len() as u8);
            2
        } else if self.data.len() < 65536 {
            let [b2, b3] = (self.data.len() as u16).to_be_bytes();
            dist.add(1).write(mask_bit | 126);
            dist.add(2).write(b2);
            dist.add(3).write(b3);
            4
        } else {
            let [b2, b3, b4, b5, b6, b7, b8, b9] = (self.data.len() as u64).to_be_bytes();
            dist.add(1).write(mask_bit | 127);
            dist.add(2).write(b2);
            dist.add(3).write(b3);
            dist.add(4).write(b4);
            dist.add(5).write(b5);
            dist.add(6).write(b6);
            dist.add(7).write(b7);
            dist.add(8).write(b8);
            dist.add(9).write(b9);
            10
        }
    }
}

impl<'a> From<&'a str> for Frame<'a> {
    #[inline]
    fn from(string: &'a str) -> Self {
        Self {
            fin: true,
            rsv: 0,
            opcode: 1,
            data: string.as_bytes(),
        }
    }
}

impl<'a> From<&'a [u8]> for Frame<'a> {
    #[inline]
    fn from(data: &'a [u8]) -> Self {
        Self {
            fin: true,
            rsv: 0,
            opcode: 2,
            data,
        }
    }
}

/// XOR `data` in place with the repeating 4-byte WebSocket `mask`, applied from
/// `data[0]` (i.e. `data[i] ^= mask[i % 4]`).
///
/// Used both to mask outgoing client payloads and to unmask incoming server
/// payloads. It processes 8 bytes per iteration against a repeated mask word so
/// the loop auto-vectorizes, rather than the scalar `i & 3` indexing that
/// prevents it. Because each 8-byte step is a multiple of the 4-byte mask
/// period, the mask stays aligned and the remainder resumes at `mask[i & 3]`.
#[inline]
pub(crate) fn apply_mask(data: &mut [u8], mask: [u8; 4]) {
    let [a, b, c, d] = mask;
    let mask_word = u64::from_ne_bytes([a, b, c, d, a, b, c, d]);
    let mut chunks = data.chunks_exact_mut(8);
    for chunk in &mut chunks {
        let word = u64::from_ne_bytes(<[u8; 8]>::try_from(&chunk[..]).unwrap()) ^ mask_word;
        chunk.copy_from_slice(&word.to_ne_bytes());
    }
    for (i, byte) in chunks.into_remainder().iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}

#[cfg(test)]
mod tests {
    use super::apply_mask;

    /// The straightforward byte-at-a-time reference implementation.
    fn naive(data: &mut [u8], mask: [u8; 4]) {
        for (i, byte) in data.iter_mut().enumerate() {
            *byte ^= mask[i & 3];
        }
    }

    #[test]
    fn apply_mask_matches_naive_and_is_involutive() {
        let mask = [0xAB, 0x12, 0xCD, 0x34];
        // Cover every remainder (0..=7) across several full 8-byte chunks.
        for len in 0..40usize {
            let original: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let mut fast = original.clone();
            let mut reference = original.clone();

            apply_mask(&mut fast, mask);
            naive(&mut reference, mask);
            assert_eq!(fast, reference, "masking mismatch at len {len}");

            // Masking twice with the same key restores the original.
            apply_mask(&mut fast, mask);
            assert_eq!(fast, original, "masking not involutive at len {len}");
        }
    }
}

#![allow(clippy::unusual_byte_groupings)]
use crate::*;
use std::io::{IoSlice, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// WebSocket implementation for both client and server
#[derive(Debug)]
pub struct WebSocket<Stream> {
    /// it is a low-level abstraction that represents the underlying byte stream over which WebSocket messages are exchanged.
    pub stream: Stream,

    /// Maximum allowed payload length in bytes.
    ///
    /// Default: 16 MB
    pub max_payload_len: usize,

    /// Bitmask of `rsv` bits permitted by a negotiated extension, as the 3-bit
    /// value `0..=7` (RSV1 = `0b100`, RSV2 = `0b010`, RSV3 = `0b001`).
    ///
    /// Per [RFC 6455 §5.2](https://datatracker.ietf.org/doc/html/rfc6455#section-5.2),
    /// a frame whose `rsv` sets any bit outside this mask fails the connection
    /// with [`Event::Error`]. Since this crate does not negotiate extensions,
    /// the default is `0` (all reserved bits must be zero). Set it to `0b100`
    /// (RSV1) when [`permessage-deflate`](https://datatracker.ietf.org/doc/html/rfc7692)
    /// has been negotiated.
    ///
    /// Default: 0
    pub allowed_rsv: u8,

    role: Role,
    is_closed: bool,
    fragment: Option<MessageType>,

    /// Accumulates the payload of a fragmented message across `recv_message` calls.
    msg_buf: Vec<u8>,
    /// `rsv` bits captured from the first (`Stream::Start`) fragment.
    msg_rsv: u8,
}

impl<IO> WebSocket<IO> {
    /// Create a new websocket client instance.
    #[inline]
    pub fn client(stream: IO) -> Self {
        Self::from((stream, Role::Client))
    }
    /// Create a websocket server instance.
    #[inline]
    pub fn server(stream: IO) -> Self {
        Self::from((stream, Role::Server))
    }
}

impl<W> WebSocket<W>
where
    W: Unpin + AsyncWrite,
{
    #[doc(hidden)]
    pub async fn send_raw(&mut self, frame: Frame<'_>) -> Result<()> {
        let buf = match self.role {
            Role::Server => {
                if self.stream.is_write_vectored() {
                    let mut head = [0; 10];
                    let head_len = unsafe { frame.encode_header_unchecked(head.as_mut_ptr(), 0) };
                    let total_len = head_len + frame.data.len();

                    let mut bufs = [IoSlice::new(&head[..head_len]), IoSlice::new(frame.data)];
                    let mut amt = self.stream.write_vectored(&bufs).await?;
                    if amt == total_len {
                        return Ok(());
                    }
                    while amt < head_len {
                        bufs[0] = IoSlice::new(&head[amt..head_len]);
                        amt += self.stream.write_vectored(&bufs).await?;
                    }
                    if amt < total_len {
                        self.stream.write_all(&frame.data[amt - head_len..]).await?;
                    }
                    return Ok(());
                }
                frame.encode_without_mask()
            }
            Role::Client => frame.encode_with(rand::random::<u32>().to_ne_bytes()),
        };
        self.stream.write_all(&buf).await
    }

    /// Send message to a endpoint.
    pub async fn send(&mut self, data: impl Into<Frame<'_>>) -> Result<()> {
        self.send_raw(data.into()).await
    }

    /// - The Close frame MAY contain a body that indicates a reason for closing.
    pub async fn close<T>(mut self, reason: T) -> Result<()>
    where
        T: CloseReason,
        T::Bytes: AsRef<[u8]>,
    {
        self.send_raw(Frame {
            fin: true,
            rsv: 0,
            opcode: 8,
            data: reason.to_bytes().as_ref(),
        })
        .await?;
        self.stream.flush().await
    }

    /// A Ping frame may serve either as a keepalive or as a means to verify that the remote endpoint is still responsive.
    ///
    /// It is used to send ping frame.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use web_socket::*;
    /// # async {
    /// let writer = Vec::new();
    /// let mut ws = WebSocket::client(writer);
    /// ws.send_ping("Hello!").await;
    /// # };
    /// ```
    pub async fn send_ping(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_raw(Frame {
            fin: true,
            rsv: 0,
            opcode: 9,
            data: data.as_ref(),
        })
        .await
    }

    /// A Pong frame sent in response to a Ping frame must have identical
    /// "Application data" as found in the message body of the Ping frame being replied to.
    ///
    /// A Pong frame MAY be sent unsolicited.  This serves as a unidirectional heartbeat.  A response to an unsolicited Pong frame is not expected.
    pub async fn send_pong(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_raw(Frame {
            fin: true,
            rsv: 0,
            opcode: 10,
            data: data.as_ref(),
        })
        .await
    }

    /// Flushes this output stream, ensuring that all intermediately buffered contents reach their destination.
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await
    }
}

// ------------------------------------------------------------------------

macro_rules! err { [$msg: expr] => { return Ok(Event::Error($msg)) }; }

#[inline]
pub async fn read_buf<const N: usize, R>(stream: &mut R) -> Result<[u8; N]>
where
    R: Unpin + AsyncRead,
{
    let mut buf = [0; N];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

impl<R> WebSocket<R>
where
    R: Unpin + AsyncRead,
{
    /// reads [Event] from websocket stream.
    pub async fn recv(&mut self) -> Result<Event> {
        if self.is_closed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "read after close",
            ));
        }
        let event = self.recv_event().await;
        if let Ok(Event::Close { .. } | Event::Error(..)) | Err(..) = event {
            self.is_closed = true;
        }
        event
    }

    /// Like [`recv`](Self::recv), but reassembles a fragmented message into a
    /// single complete one before returning it.
    ///
    /// Each call to [`recv`](Self::recv) yields one frame at a time, so a
    /// fragmented message is observed as a sequence of
    /// [`DataType::Stream`] events ([`Start`](Stream::Start),
    /// [`Next`](Stream::Next), [`End`](Stream::End)). This method drives that
    /// loop for you: the [`Event::Data`] it returns is always a
    /// [`DataType::Complete`] holding the concatenated payload, with the `rsv`
    /// bits taken from the first fragment.
    ///
    /// Per [RFC 6455 §5.4](https://datatracker.ietf.org/doc/html/rfc6455#section-5.4),
    /// control frames (`Ping`/`Pong`/`Close`) may be interleaved between the
    /// fragments of a data message. Those — and [`Event::Error`] — are returned
    /// immediately; the partial payload is buffered and a later call resumes the
    /// reassembly. The caller must still handle those events (e.g. answer a
    /// `Ping` with [`send_pong`](Self::send_pong)).
    ///
    /// A reassembled payload exceeding [`max_payload_len`](Self::max_payload_len)
    /// yields [`Event::Error`].
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use web_socket::*;
    /// # use tokio::io::{AsyncRead, AsyncWrite};
    /// # async fn example<IO: Unpin + AsyncRead + AsyncWrite>(mut ws: WebSocket<IO>) -> std::io::Result<()> {
    /// match ws.recv_message().await? {
    ///     Event::Data { ty: DataType::Complete(_), data, .. } => { let _ = data; }
    ///     Event::Ping(data) => ws.send_pong(data).await?,
    ///     _ => {}
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn recv_message(&mut self) -> Result<Event> {
        loop {
            let stream = match self.recv().await? {
                Event::Data {
                    ty: DataType::Complete(ty),
                    data,
                    rsv,
                } if self.msg_buf.is_empty() => {
                    // Fast path: an unfragmented message with nothing buffered.
                    return Ok(Event::Data {
                        ty: DataType::Complete(ty),
                        data,
                        rsv,
                    });
                }
                Event::Data {
                    ty: DataType::Stream(stream),
                    data,
                    rsv,
                } => {
                    if let Stream::Start(..) = stream {
                        self.msg_rsv = rsv;
                    }
                    self.msg_buf.extend_from_slice(&data);
                    stream
                }
                // Interleaved control frames (and errors) pass straight through;
                // any buffered fragments are kept for the next call.
                other => return Ok(other),
            };

            if self.msg_buf.len() > self.max_payload_len {
                self.msg_buf = Vec::new();
                self.is_closed = true;
                return Ok(Event::Error("fragmented message too large"));
            }
            if let Stream::End(ty) = stream {
                let data = std::mem::take(&mut self.msg_buf).into_boxed_slice();
                return Ok(Event::Data {
                    ty: DataType::Complete(ty),
                    data,
                    rsv: self.msg_rsv,
                });
            }
        }
    }

    // ### WebSocket Frame Header
    //
    // ```txt
    //  0                   1                   2                   3
    //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-------+-+-------------+-------------------------------+
    // |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
    // |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
    // |N|V|V|V|       |S|             |   (if payload len==126/127)   |
    // | |1|2|3|       |K|             |                               |
    // +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
    // |     Extended payload length continued, if payload len == 127  |
    // + - - - - - - - - - - - - - - - +-------------------------------+
    // |                               |Masking-key, if MASK set to 1  |
    // +-------------------------------+-------------------------------+
    // | Masking-key (continued)       |          Payload Data         |
    // +-------------------------------- - - - - - - - - - - - - - - - +
    // :                     Payload Data continued ...                :
    // + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
    // |                     Payload Data continued ...                |
    // +---------------------------------------------------------------+
    // ```
    /// reads [Event] from websocket stream.
    pub async fn recv_event(&mut self) -> Result<Event> {
        let [b1, b2] = read_buf(&mut self.stream).await?;

        let fin = b1 & 0b_1000_0000 != 0;
        let rsv = (b1 & 0b_111_0000) >> 4;
        let opcode = b1 & 0b_1111;
        let len = (b2 & 0b_111_1111) as usize;

        // Reserved bits MUST be `0` unless an extension is negotiated that
        // defines meanings for non-zero values.  If a nonzero value is received
        // and none of the negotiated extensions (see `allowed_rsv`) defines the
        // meaning of such a nonzero value, the receiving endpoint MUST _Fail the
        // WebSocket Connection_.
        if rsv & !self.allowed_rsv != 0 {
            err!("reserve bit must be `0`");
        }

        // Defines whether the "Payload data" is masked.  If set to 1, a
        // masking key is present in masking-key, and this is used to unmask
        // the "Payload data" as per [Section 5.3](https://datatracker.ietf.org/doc/html/rfc6455#section-5.3).  All frames sent from
        // client to server have this bit set to 1.
        let is_masked = b2 & 0b_1000_0000 != 0;

        // A client MUST mask all frames that it sends to the server. (Note
        // that masking is done whether or not the WebSocket Protocol is running
        // over TLS.)  The server MUST close the connection upon receiving a
        // frame that is not masked.
        //
        // A server MUST NOT mask any frames that it sends to the client.
        if let Role::Server = self.role {
            if !is_masked {
                err!("expected masked frame");
            }
        } else if is_masked {
            err!("expected unmasked frame");
        }

        // 3-7 are reserved for further non-control frames.
        if opcode >= 8 {
            if !fin {
                err!("control frame must not be fragmented");
            }
            if len > 125 {
                err!("control frame must have a payload length of 125 bytes or less");
            }
            let msg = self.read_payload(len).await?;
            match opcode {
                8 => Ok(on_close(&msg)),
                9 => Ok(Event::Ping(msg)),
                10 => Ok(Event::Pong(msg)),
                // 11-15 are reserved for further control frames
                _ => err!("unknown opcode"),
            }
        } else {
            let ty = match (opcode, fin, self.fragment) {
                (2, true, None) => DataType::Complete(MessageType::Binary),
                (1, true, None) => DataType::Complete(MessageType::Text),
                (2, false, None) => {
                    self.fragment = Some(MessageType::Binary);
                    DataType::Stream(Stream::Start(MessageType::Binary))
                }
                (1, false, None) => {
                    self.fragment = Some(MessageType::Text);
                    DataType::Stream(Stream::Start(MessageType::Text))
                }
                (0, false, Some(ty)) => DataType::Stream(Stream::Next(ty)),
                (0, true, Some(ty)) => {
                    self.fragment = None;
                    DataType::Stream(Stream::End(ty))
                }
                _ => err!("invalid data frame"),
            };
            let len = match len {
                126 => u16::from_be_bytes(read_buf(&mut self.stream).await?) as usize,
                127 => u64::from_be_bytes(read_buf(&mut self.stream).await?) as usize,
                len => len,
            };
            if len > self.max_payload_len {
                err!("payload too large");
            }
            let data = self.read_payload(len).await?;
            Ok(Event::Data { ty, data , rsv})
        }
    }

    async fn read_payload(&mut self, len: usize) -> Result<Box<[u8]>> {
        let mut data = vec![0; len].into_boxed_slice();
        match self.role {
            Role::Server => {
                let mask: [u8; 4] = read_buf(&mut self.stream).await?;
                self.stream.read_exact(&mut data).await?;
                // TODO: Use SIMD wherever possible for best performance
                for i in 0..data.len() {
                    data[i] ^= mask[i & 3];
                }
            }
            Role::Client => {
                self.stream.read_exact(&mut data).await?;
            }
        }
        Ok(data)
    }
}

/// - If there is a body, the first two bytes of the body MUST be a 2-byte unsigned integer (in network byte order: Big Endian)
///   representing a status code with value /code/ defined in [Section 7.4](https:///datatracker.ietf.org/doc/html/rfc6455#section-7.4).
///   Following the 2-byte integer,
///
/// - The application MUST NOT send any more data frames after sending a `Close` frame.
///
/// - If an endpoint receives a Close frame and did not previously send a
///   Close frame, the endpoint MUST send a Close frame in response.  (When
///   sending a Close frame in response, the endpoint typically echos the
///   status code it received.)  It SHOULD do so as soon as practical.  An
///   endpoint MAY delay sending a Close frame until its current message is
///   sent
///
/// - After both sending and receiving a Close message, an endpoint
///   considers the WebSocket connection closed and MUST close the
///   underlying TCP connection.
fn on_close(msg: &[u8]) -> Event {
    let code = msg
        .get(..2)
        .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
        .unwrap_or(1000);

    match code {
        1000..=1003 | 1007..=1011 | 1015 | 3000..=3999 | 4000..=4999 => {
            match msg.get(2..).map(|data| String::from_utf8(data.to_vec())) {
                Some(Ok(msg)) => Event::Close {
                    code,
                    reason: msg.into_boxed_str(),
                },
                None => Event::Close {
                    code,
                    reason: "".into(),
                },
                Some(Err(_)) => Event::Error("invalid utf-8 payload"),
            }
        }
        _ => Event::Error("invalid close code"),
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    /// Build a single raw, unmasked frame (server -> client direction) so it
    /// can be read by a `WebSocket::client`. Only small payloads (< 126 bytes)
    /// are supported, which keeps the length encoding a single byte.
    fn frame(fin: bool, rsv: u8, opcode: u8, payload: &[u8]) -> Vec<u8> {
        assert!(payload.len() < 126, "test helper only supports small payloads");
        let b1 = ((fin as u8) << 7) | (rsv << 4) | opcode;
        let b2 = payload.len() as u8; // top (mask) bit clear => unmasked
        let mut buf = vec![b1, b2];
        buf.extend_from_slice(payload);
        buf
    }

    #[tokio::test]
    async fn reassembles_fragmented_text() {
        let mut bytes = Vec::new();
        bytes.extend(frame(false, 0, 1, b"Hel")); // Start(Text)
        bytes.extend(frame(false, 0, 0, b"lo")); // Next
        bytes.extend(frame(true, 0, 0, b"!")); // End
        let mut ws = WebSocket::client(&bytes[..]);

        match ws.recv_message().await.unwrap() {
            Event::Data { ty, data, rsv } => {
                assert!(matches!(ty, DataType::Complete(MessageType::Text)));
                assert_eq!(&*data, b"Hello!");
                assert_eq!(rsv, 0);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn passes_complete_message_through() {
        let bytes = frame(true, 0, 1, b"hi");
        let mut ws = WebSocket::client(&bytes[..]);

        match ws.recv_message().await.unwrap() {
            Event::Data { ty, data, .. } => {
                assert!(matches!(ty, DataType::Complete(MessageType::Text)));
                assert_eq!(&*data, b"hi");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn handles_control_frame_interleaved_between_fragments() {
        let mut bytes = Vec::new();
        bytes.extend(frame(false, 0, 1, b"Hel")); // Start(Text)
        bytes.extend(frame(true, 0, 9, b"pi")); // Ping (control) in the middle
        bytes.extend(frame(false, 0, 0, b"lo")); // Next
        bytes.extend(frame(true, 0, 0, b"!")); // End
        let mut ws = WebSocket::client(&bytes[..]);

        // The interleaved ping surfaces immediately; partial payload stays buffered.
        match ws.recv_message().await.unwrap() {
            Event::Ping(data) => assert_eq!(&*data, b"pi"),
            other => panic!("expected ping, got: {other:?}"),
        }
        // The next call resumes reassembly and returns the full message.
        match ws.recv_message().await.unwrap() {
            Event::Data { ty, data, .. } => {
                assert!(matches!(ty, DataType::Complete(MessageType::Text)));
                assert_eq!(&*data, b"Hello!");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn reassembles_binary_and_keeps_rsv_of_first_fragment() {
        let mut bytes = Vec::new();
        bytes.extend(frame(false, 0b100, 2, &[1, 2])); // Start(Binary), rsv1 set
        bytes.extend(frame(true, 0, 0, &[3])); // End (rsv here is ignored)
        let mut ws = WebSocket::client(&bytes[..]);
        ws.allowed_rsv = 0b100; // RSV1 negotiated (e.g. permessage-deflate)

        match ws.recv_message().await.unwrap() {
            Event::Data { ty, data, rsv } => {
                assert!(matches!(ty, DataType::Complete(MessageType::Binary)));
                assert_eq!(&*data, &[1, 2, 3]);
                assert_eq!(rsv, 0b100);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn errors_when_reassembled_message_exceeds_limit() {
        let mut bytes = Vec::new();
        bytes.extend(frame(false, 0, 2, &[0; 100])); // Start(Binary), within per-frame limit
        bytes.extend(frame(true, 0, 0, &[0; 100])); // End, pushes the total over the limit
        let mut ws = WebSocket::client(&bytes[..]);
        ws.max_payload_len = 150;

        match ws.recv_message().await.unwrap() {
            Event::Error(msg) => assert_eq!(msg, "fragmented message too large"),
            other => panic!("expected error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_reserved_bit_by_default() {
        // RSV1 set, but no extension negotiated (allowed_rsv defaults to 0).
        let bytes = frame(true, 0b100, 1, b"hi");
        let mut ws = WebSocket::client(&bytes[..]);
        assert_eq!(ws.allowed_rsv, 0);

        match ws.recv_event().await.unwrap() {
            Event::Error(msg) => assert_eq!(msg, "reserve bit must be `0`"),
            other => panic!("expected error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn accepts_negotiated_rsv_but_still_rejects_others() {
        let mut bytes = Vec::new();
        bytes.extend(frame(true, 0b100, 1, b"ok")); // RSV1: allowed once negotiated
        bytes.extend(frame(true, 0b010, 1, b"no")); // RSV2: still outside the mask
        let mut ws = WebSocket::client(&bytes[..]);
        ws.allowed_rsv = 0b100; // permessage-deflate negotiated

        match ws.recv_event().await.unwrap() {
            Event::Data { data, rsv, .. } => {
                assert_eq!(&*data, b"ok");
                assert_eq!(rsv, 0b100);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        match ws.recv_event().await.unwrap() {
            Event::Error(msg) => assert_eq!(msg, "reserve bit must be `0`"),
            other => panic!("expected error, got: {other:?}"),
        }
    }
}

impl<IO> From<(IO, Role)> for WebSocket<IO> {
    #[inline]
    fn from((stream, role): (IO, Role)) -> Self {
        Self {
            stream,
            max_payload_len: 16 * 1024 * 1024,
            allowed_rsv: 0,
            role,
            is_closed: false,
            fragment: None,
            msg_buf: Vec::new(),
            msg_rsv: 0,
        }
    }
}

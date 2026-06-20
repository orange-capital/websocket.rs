# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`web-socket` is a minimal, high-performance implementation of the [RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455) WebSocket protocol for both client and server roles. It operates purely on the framing layer over any `AsyncRead`/`AsyncWrite` byte stream — the HTTP upgrade handshake and TLS are explicit **non-goals** (callers must perform the handshake themselves; see `examples/utils/handshake.rs` and `examples/axum-example`).

The crate has only two runtime dependencies: `rand` (for client masking keys) and `tokio` with just the `io-util` feature.

## Commands

```bash
cargo build                         # build the library
cargo test                          # run doctests (the only tests in-crate)
cargo run --example minimal         # client demo connecting to ws.ifelse.io
cargo run --example chatroom        # server demo (serves examples/assets/chatroom.html)
cargo doc --open                    # render docs; #![warn(missing_docs)] is enabled

# Autobahn conformance suite (requires Docker)
bash ./autobahn/autobahn-server.sh  # tests this crate as a server (fuzzingclient)
bash ./autobahn/autobahn-client.sh  # tests this crate as a client
npx serve ./autobahn/client         # view generated client report
```

The `examples/axum-example` is a separate workspace member with its own dependencies (axum, hyper); build it from its own directory or via the workspace.

## Architecture

Three source files under `src/`:

- **`lib.rs`** — public protocol types, no I/O. Defines `Role` (Server/Client), `MessageType` (Text=1/Binary=2), `Event` (what `recv` returns), `DataType`/`Stream` (complete vs. fragmented messages), `CloseCode`, and the `CloseReason` trait. `CloseReason` is implemented for `()`, `u16`, `CloseCode`, `&str`, and `(Code, Msg)` tuples — this is what lets `ws.close("bye")`, `ws.close(CloseCode::Normal)`, and `ws.close(())` all work.

- **`ws.rs`** — `WebSocket<Stream>`, the main type, generic over any `AsyncRead`/`AsyncWrite`. Construct with `WebSocket::client(io)` or `WebSocket::server(io)`. Key methods: `send`, `send_ping`, `send_pong`, `close`, `flush`, and `recv`/`recv_event`. `recv` wraps `recv_event` to enforce the closed-connection invariant (returns `NotConnected` error on read-after-close); `recv_event` does the actual frame parsing. Fragmented messages are tracked via the `fragment: Option<MessageType>` field across calls.

- **`frame.rs`** — `Frame<'a>` (`#[doc(hidden)]`, low-level), borrows its payload as `&[u8]`. Handles wire encoding of the frame header (`encode_header_unchecked` uses raw pointer writes for the 2/4/10-byte header depending on payload length) and masking. This is the unsafe/perf-critical layer.

### Role-driven masking (the central protocol asymmetry)

Per RFC 6455, **clients MUST mask all outgoing frames; servers MUST NOT**, and each side rejects frames masked the wrong way. The `Role` field drives this throughout:

- **Sending** (`send_raw` in `ws.rs`): Client frames are XOR-masked with a random `u32` key. Server frames are unmasked, and when the underlying stream supports vectored writes, the header and payload are sent via `write_vectored` to avoid copying the payload into a combined buffer.
- **Receiving** (`recv_event`/`read_payload`): A server unmasks incoming payloads and errors on unmasked frames; a client errors on masked frames.

### Receive flow

`recv_event` reads the 2-byte header, then branches on opcode: control frames (opcode ≥ 8: close=8, ping=9, pong=10) must be unfragmented and ≤125 bytes; data frames (opcode 0=continuation, 1=text, 2=binary) drive the fragmentation state machine producing `DataType::Complete` or `DataType::Stream(Start/Next/End)`. Payloads larger than `max_payload_len` (default 16 MB, a public field) yield `Event::Error`. The `rsv` bits are parsed and surfaced on `Event::Data` (the crate does not interpret extensions itself). `on_close` validates the close code against the RFC-permitted ranges and decodes the UTF-8 reason.

## Conventions

- Protocol errors are returned in-band as `Event::Error(&'static str)` (via the `err!` macro), **not** as `io::Error`. Only actual stream failures produce `Err`. Callers typically respond to an `Event::Error` by closing with an appropriate `CloseCode`.
- `frame.rs` uses `unsafe` pointer writes for header encoding; preserve the documented safety invariants (`dist` valid for 10 bytes) when modifying.
- `#![warn(missing_docs)]` is on for the crate root — public items need doc comments.
- Masking/unmasking loops are marked `TODO: Use SIMD` — current implementation is a scalar XOR loop.

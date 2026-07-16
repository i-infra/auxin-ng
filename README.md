# auxin-ng

A scriptable, protocol-transparent Signal CLI in Rust — the spiritual
successor to [auxin](https://github.com/mobilecoinofficial/auxin), rebuilt on
the maintained [presage](https://github.com/whisperfish/presage) /
[libsignal-service-rs](https://github.com/whisperfish/libsignal-service-rs) /
official [libsignal](https://github.com/signalapp/libsignal) stack.

Like auxin, and unlike most Signal clients, auxin-ng is built for pipes and
programs rather than humans:

- **JSONL everywhere.** Every received event is one JSON object per line on
  stdout: the full envelope metadata (sender, device, timestamps, sealed
  sender flag, server GUID) plus the message body as **canonical
  protobuf-JSON** — nothing is summarized away.
- **Raw protobuf passthrough on send.** `send --content '<proto-JSON>'`
  accepts a raw `DataMessage` (or `TypingMessage`, `ReceiptMessage`, …) so
  you can exercise any protocol feature without waiting for a CLI flag.
  `--simulate` builds and prints the message without sending it.
- **JSON-RPC 2.0 daemon.** `auxin-ng jsonrpc` serves requests on stdin and
  streams incoming messages as notifications on stdout — drive it from any
  language.

The protobuf↔JSON mapping is done with `prost-reflect` over a descriptor set
compiled at build time (via `protox`, no protoc needed) from the `.proto`
files vendored from libsignal-service-rs — so the JSON you see is the wire
format, mechanically derived from Signal's own schema.

## Quick start

```sh
cargo build --release

# Link as a secondary device (like Signal Desktop):
auxin-ng link-device --device-name my-bot

# Who am I?
auxin-ng whoami

# Stream messages as JSONL:
auxin-ng receive | jq -c '.content.body?'

# Send:
auxin-ng send --to <uuid> -m "hello"
auxin-ng send --to <uuid> --content '{"body":"hi","preview":[{"url":"https://example.com"}]}'

# JSON-RPC daemon:
auxin-ng jsonrpc <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"send","params":{"recipient":"<uuid>","message":"hi"}}
EOF
```

State lives in a sqlite database (`--db-path`, optionally encrypted with
`--passphrase`), compatible with nothing else on purpose — presage owns the
schema.

## JSONL event shapes

```json
{"type":"message","envelope":{"sender":"...","senderDevice":2,"destination":"...","timestamp":1752650000000,"serverTimestamp":1752650000123,"unidentifiedSender":true,"needsReceipt":true,"wasPlaintext":false,"serverGuid":"..."},"contentType":"dataMessage","content":{"body":"hello","timestamp":"1752650000000"}}
{"type":"queueEmpty"}
{"type":"contactsSynced"}
{"type":"attachmentDownloaded","sender":"...","timestamp":1752650000000,"path":"...","contentType":"image/jpeg","fileName":"cat.jpg"}
{"type":"error","message":"..."}
```

## JSON-RPC methods

`send` (params: `recipient` | `group`, `message` | `content` [+
`contentType`], `attachments`), `whoami`, `listContacts`, `listGroups`,
`version`. Incoming events arrive as notifications whose `method` is the
event type and whose `params` is the JSONL event object.

## Provenance

- presage / presage-store-sqlite are git-pinned; `Cargo.lock` is committed
  (auxin's unpinned git deps are the main reason it no longer builds).
- `proto/` is vendored from libsignal-service-rs at the same revision presage
  pins, and must be re-vendored when bumping presage.
- License: AGPL-3.0-only (inherited from the stack).

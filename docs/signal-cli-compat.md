# signal-cli compatibility shim — spec

Goal: make `auxin-ng jsonRpc` a drop-in backend for forestfire
(`~/Code/Projects/forestfire`), which spawns
`{SIGNAL_PATH} --trust-new-identities always --config <dir> --user <account> jsonRpc`
and speaks newline-delimited JSON-RPC 2.0 over stdio.

Scope is forestfire's actual wire contract (7 outbound methods + the `receive`
envelope read by `forest/message.py::StdioMessage`), not the full signal-cli
schema. Unimplemented methods keep returning `-32601`.

## Conventions

- Requests carry client-chosen `id` (e.g. `send-<uid>`); echo it verbatim in
  the response (already done). Errors: `{"code": N, "message": "..."}`.
- Param keys must be accepted in **both** camelCase and dash/snake form —
  forestfire sends `group-id`, `target-author`, `target-timestamp`,
  `end_session` (signal-cli's `JsonRpcNamespace` tolerates both).
- `groupId` in the signal-cli schema is the **base64 group identifier**;
  auxin-ng internally uses the **hex group master key**. The shim maps
  base64 id → master key by iterating presage's stored groups and comparing
  `GroupSecretParams::derive_from_master_key(mk).get_group_identifier()`.
  Reverse mapping produces `groupInfo.groupId` on receive.

## Method translation table

### `send`
| signal-cli / forest param | auxin-ng mapping |
|---|---|
| `recipient` (ACI uuid) | `Recipient::Contact` via existing `parse_service_id` |
| `group-id` / `groupId` (b64) | b64 id → master key → `Recipient::Group` |
| `message` | `DataMessage.body` (existing `build_body`) |
| `attachments` (paths) | existing `upload_attachments` |
| `end_session` / `endSession` | `DataMessage.flags |= END_SESSION` |
| `sticker` (`"<packIdHex>:<stickerId>"`) | `DataMessage.sticker {packId, stickerId, packKey}` — pack key looked up from installed packs (`presage sticker_metadata`) |
| `content` (raw proto-JSON) | **auxin-ng native superset** — merge into body exactly like the old auxin backend; forestfire still has plumbing for this |

Response: `{"timestamp": N, ...}` — forest reads only `result.timestamp`
(and occasionally `result.groupId`); `app::send` already returns `timestamp`,
extra fields are harmless.

### `sendReaction`
Params: `emoji`, `target-author` (uuid), `target-timestamp`,
`recipient` **or** `group` (b64 id; note key is `group`, not `group-id`, here),
optional `remove`.
→ `DataMessage { reaction: { emoji, remove, targetAuthorAci, targetSentTimestamp }, timestamp }`
to contact or group. (Shape already proven by `examples/jsonrpc-requests.jsonl`.)

### `sendTyping`
Params: `recipient: [uuid]` **or** `group: [b64 id]` (single-element lists), `stop`.
→ `TypingMessage { timestamp, action: STARTED|STOPPED, groupId: <raw group id bytes, b64> }`
sent to the contact or fanned out to the group.

### `sendPaymentNotification`
Params: `recipient`, `receipt` (b64), `note`.
→ `DataMessage { payment: { notification: { mobileCoin: { receipt }, note } } }`.
Pure proto-JSON wrapper; types already in the descriptor registry.

### `updateProfile`
Params: `givenName`, `familyName`, `about?`, `aboutEmoji?`,
`mobileCoinAddress`, `avatar`.
→ `presage Manager::update_profile(ProfileName{given, family}, about, emoji)`.
**Supported today: name/about/emoji only.** `avatar` and `mobileCoinAddress`
require upstream libsignal-service-rs work (see gaps). Shim should apply what
it can and error clearly if unsupported params are present.

### `updateAccount`
forest uses only `username` (reads back `result.usernameLink`) for `/signalme`.
**Not implementable today** — libsignal-service-rs has no username
reserve/confirm endpoints. Return a clear error; deprioritize.

### `listContacts`
Params: `recipient?`, `allRecipients?`.
→ presage store contacts, filtered by recipient; emit signal-cli's shape:
`[{"uuid", "number", "username", "name", "profile": {"givenName", "familyName",
"about", "mobileCoinAddress", ...}}]`, fetching live profiles via
`retrieve_profile_by_uuid` when requested.
`profile.mobileCoinAddress` is **unavailable** until the upstream Profile gap
is fixed. Note: forest's `get_signalpay_address` does
`result.get("result", {}).get("profile", {})` — treating the result as a
single object, which doesn't match signal-cli's array shape either; that call
site likely needs a fix in forest regardless.

### Inbound: `receive` notifications
In compat mode, wrap each incoming message as signal-cli does:
`{"jsonrpc":"2.0","method":"receive","params":{"envelope":{...},"account":"<account>"}}`
(no `id`). Mapping from auxin-ng's `JsonEnvelope` + proto-JSON content:

| signal-cli envelope field (read by StdioMessage) | source |
|---|---|
| `sourceUuid` | `sender` service id (ACI string; PNI senders keep `PNI:` handling TBD) |
| `sourceName` | contact/profile name from presage store (optional — forest falls back to uuid) |
| `sourceDevice` | `senderDevice` |
| `timestamp` | `timestamp` |
| `dataMessage.message` | `DataMessage.body` |
| `dataMessage.timestamp` | `DataMessage.timestamp` |
| `dataMessage.expiresInSeconds` | `DataMessage.expireTimer` |
| `dataMessage.attachments[]` | `AttachmentPointer` → `{contentType, filename, id, size, file: <local path if downloaded>}` (join with `attachmentDownloaded` info) |
| `dataMessage.mentions[]` | `bodyRanges` with `mentionAci` → `{uuid, start, length}` |
| `dataMessage.groupInfo.groupId` | `groupV2.masterKey` → b64 group identifier; `type: "DELIVER"` |
| `dataMessage.quote` | `{id: quote.id, authorUuid: quote.authorAci, text}` |
| `dataMessage.reaction` | `{emoji, targetAuthorUuid: targetAuthorAci, targetSentTimestamp, isRemove}` |
| `dataMessage.payment` | `payment.notification` → `{receipt: b64, note}` |
| `typingMessage.action` | `TypingMessage.action` enum name (`STARTED`/`STOPPED`) |

Response blobs forest also correlates: top-level `id`, `result`, `error` —
already compatible.

Native event notifications (`message`, `queueEmpty`, `reconnecting`, …) stay
available outside compat mode. Compat is gated on the global
`--signal-cli-compat` flag (no auto-detection); in compat mode, native
notifications are suppressed (logged to stderr at debug level) and message
events become `receive` notifications instead. Attachments are downloaded
*before* the notification is emitted so `dataMessage.attachments[].file`
can carry the local path (requires `--attachments-dir`).

### CLI invocation compat
- `jsonRpc` as a clap alias for the `jsonrpc` subcommand.
- Accept and ignore `--trust-new-identities <mode>` (presage trusts new
  identities implicitly; document the semantic difference).
- `--config <dir>` → treat as data dir root; `--user <account>` → account
  selector (map onto `--db-path` / `-a` convention).
  Alternative: one-line change in `forest/utils.py` launch args instead.

**Implemented:** all of the above are global flags, so forestfire's
flags-before-subcommand order parses. `--config <dir> --user <acct>` maps to
`<dir>/accounts/<acct>.db3` (no `--user` → `<dir>/auxin.db3`); account names
additionally allow `+` for E.164 numbers. forestfire's launcher needs one
extra token: `--signal-cli-compat` (e.g.
`SIGNAL_PATH="auxin-ng --signal-cli-compat --attachments-dir <dir>"` or the
equivalent edit in `forest/utils.py`), since compat is opt-in.

## Feature gaps in auxin-ng (and where the work actually lives)

Shim-level (local, no upstream changes):
1. b64 groupId ↔ master key mapping (zkgroup derive over stored groups).
2. `sourceName` lookup from store at envelope-build time.
3. signal-cli envelope formatter (proto-JSON → flattened shape above).
4. `sendReaction` / `sendTyping` / `sendPaymentNotification` / `send`
   param wrappers over existing proto-JSON send.
5. Sticker pack-key lookup for `sticker` param.
6. `listContacts` recipient filter + profile embedding.
7. CLI flag/subcommand aliases.

Upstream in presage / libsignal-service-rs:
8. ~~**`updateProfile` avatar**~~ — **implemented** in our local
   libsignal-service-rs checkout (`../libsignal-service-rs`, wired via
   `[patch]`): `ProfileCipher::encrypt_avatar` (unpadded AES-256-GCM,
   `nonce || ct || tag`, matching Java's ProfileCipherOutputStream) and the
   CDN0 multipart upload using the `ProfileAvatarUploadAttributes` form the
   server returns from `PUT /v1/profile`. Live-verified 2026-07-18.
9. ~~**MobileCoin payment address, both directions**~~ — **implemented** in
   the local checkouts: `ProfileCipher::{encrypt,decrypt}_payment_address`
   (4-byte LE length prefix, padded to 554 bytes, the fixed 776-char base64
   field), `paymentAddress` on profile read/write, signature verification
   against the owner's identity key on read (Java:
   `PaymentUtils.verifyPaymentsAddress`), and
   `presage Manager::{update_profile_extended, sign_payment_address,
   retrieve_profile_by_uuid_uncached}`. compat `updateProfile` merges over
   the current profile (fetch-first), re-signs and re-sends the stored
   address when the param is absent (a profile write replaces everything
   server-side), and treats `mobileCoinAddress: ""` as clear.
   `listContacts` emits `profile.mobileCoinAddress` as base64 of the raw
   verified MobileCoin public address, like signal-cli. Live-verified
   2026-07-18 (set → read-back-verified → clear).
10. ~~**Username set + usernameLink**~~ — **implemented** across the local
    forks (research + design: [username-support.md](username-support.md)):
    reserve/confirm/delete endpoints in libsignal-service-rs (`/v1/`
    prefix confirmed against production), `presage
    Manager::{set_username, delete_username, username}` with kv-store
    persistence and link-entropy reclaim, and compat
    `updateAccount {username}` → `{username, usernameLink}` (plus
    `{deleteUsername: true}` and a native `lookupUsername` method).
    Live-verified 2026-07-19: set `auxin.27`, resolved it back to our
    own ACI via public lookup, and re-confirmed with a byte-identical
    link URL (reclaim path).

Not needed by forest, known absent (unchanged from before): group management,
profile/contact/device/identity management commands, polls, sendReceipt
(forest relies on auto delivery receipts — covered by `--auto-accept`).

# Username / `updateAccount` support — research & plan

Research notes for closing gap #10 in [signal-cli-compat.md](signal-cli-compat.md)
(forest's `/signalme` flow: `updateAccount {username}` → reads back
`result.usernameLink`). Sources: signal-cli + signal-service-java (javap of
the resolved jars), libsignal v0.94.4 (the rev our stack pins), our
libsignal-service-rs fork, and Signal-Server upstream.

**Headline: this is much closer than the old gap note assumed.** All of the
client-side crypto already exists in the `usernames` crate that
libsignal-service-rs *already depends on*, and half the endpoints are
already implemented in `src/websocket/usernames.rs`. What's missing is three
endpoint wrappers, a presage Manager method, and the auxin-ng RPC surface.

## What already exists

### libsignal `usernames` crate (pinned tag v0.94.4, already a dependency)

- `Username::candidates_from(rng, nickname, NicknameLimits)` →
  `Vec<String>` of `nickname.NN` candidates (20, spread over discriminator
  ranges 1..100 … 1e8..1e9); `Username::new(s)` parses; `.hash()` → 32-byte
  compressed Ristretto; `.proof(&randomness)` → zk proof (poksho);
  `Username::verify_proof(proof, hash)`.
- Link crypto: `create_for_username(rng, username, Option<&entropy>)` →
  `(entropy[32], encrypted_blob)` (IV(16) ‖ AES-256-CBC ‖ HMAC-SHA256(32),
  keys HKDF'd from entropy; plaintext is a padded `UsernameData` proto);
  `decrypt_username(&entropy, &blob)` → `String`. Passing the old entropy
  reclaims an existing link URL; `None` mints fresh entropy.

### libsignal-service-rs (`src/websocket/usernames.rs`) — already there

- `look_up_username[_hash]` — `GET /v1/accounts/username_hash/{b64url}` →
  `Option<Aci>` (404 → None).
- `look_up_username_link` — `GET /v1/accounts/username_link/{uuid}` +
  `decrypt_username`.
- `set_username_link(username, keep_link_handle)` —
  `PUT /v1/accounts/username_link`
  `{usernameLinkEncryptedValue, keepLinkHandle}` → `{usernameLinkHandle}`.
- `parse_username_link` / `generate_username_link(handle, entropy)` — the
  `https://signal.me/#eu/<b64url(entropy(32) ‖ handle_uuid(16))>` format.
- `WhoAmIResponse` already carries `username_hash: Option<Vec<u8>>` and
  `username_link_handle: Option<Uuid>` — the reconciliation primitive.

### presage

- `Manager::lookup_username(&str)` (unidentified socket) — the only
  username API today. Websocket/push-service accessor patterns to extend:
  `self.identified_websocket(false).await?` (cf. `whoami`).

## What's missing

### 1. Three endpoint wrappers in libsignal-service-rs (`SignalWebSocket<Identified>`)

| Op | Wire | Body → Response |
|---|---|---|
| reserve | `PUT /v1/accounts/username_hash/reserve` | `{usernameHashes: [b64url; ≤20]}` → `{usernameHash}` |
| confirm | `PUT /v1/accounts/username_hash/confirm` | `{usernameHash, zkProof, encryptedUsername}` (all b64url) → `{usernameHash, usernameLinkHandle: uuid}` |
| delete username | `DELETE /v1/accounts/username_hash` | — |
| delete link | `DELETE /v1/accounts/username_link` | — |

Follow the `set_username_link` template (`http_request(...).send_json(...)
.service_error_for_status()`); byte fields use
`serde_base64_url_safe_no_pad` (matches Signal-Server's
`ByteArrayBase64UrlAdapter`).

Path-prefix caveat: Signal-Server's current `AccountController` and the
signal-service-java `_101` jar both say `/v1/accounts/username_hash/…`;
one report suggests newer signal-network builds may use `/v2/…`. Everything
else in libsignal-service-rs uses `/v1/` and works, so start with `/v1/`
and confirm against the live server (a 404 on reserve = wrong prefix).

Error semantics to surface: reserve **409** = all candidates taken;
confirm **409** = reservation mismatch/missing, **410** = hash gone/
unavailable; link PUT **409** = no username set; **422** malformed;
**429** rate-limited.

Confirm's `encryptedUsername` is the link blob — confirm establishes the
username *and* its link in one call (that's how signal-cli does it).

### 2. presage `Manager` orchestration (mirror signal-cli's `AccountHelper`)

```rust
pub struct UsernameLink { pub entropy: [u8; 32], pub handle: Uuid }  // → url via generate_username_link

pub async fn set_username(&mut self, nickname_or_full: &str)
    -> Result<(String, Url), Error<S::Error>>
```

Flow (from `AccountHelper.reserveUsername*`):
1. Input with `.` → exact `Username::new`; bare nickname →
   `Username::candidates_from(rng, nick, NicknameLimits::default())`.
2. If it equals the current username → *refresh* instead: compare
   `whoami().username_hash` with the local hash; if equal, just re-confirm
   /re-link (reclaim with stored entropy) as needed.
3. reserve(candidate hashes) → find returned hash among candidates → pick
   that `Username`.
4. `create_for_username(rng, username.to_string(), None)` → (entropy, blob);
   `username.proof(&random_32)`; confirm(hash, proof, blob) →
   `usernameLinkHandle`.
5. Persist `username` + link components; return
   `(username, generate_username_link(handle, &entropy))`.

Also: `delete_username()` (DELETE both, clear local state) and a
`username()` accessor. Local persistence needs a small store addition
(username string + entropy + handle; signal-cli stores exactly these
three). A kv-style entry in the sqlite store is enough — no new table
needed if the store has a kv table (it does: registration data lives in
`kv`).

Retry note: signal-cli retries `set_username_link` up to 3× when healing a
username that lost its link; fine to skip initially.

### 3. auxin-ng surface

- compat `updateAccount {username}` → `set_username`, respond
  `{"username": "...", "usernameLink": "https://signal.me/#eu/..."}` —
  forest reads `result.usernameLink` (signal-cli emits the same shape).
  `{deleteUsername: true}` → `delete_username`, respond `{}`.
- Optionally native `lookupUsername` (presage already has it) — trivial.

## Effort estimate

Small. No new crypto, no new dependencies, no protobuf work:
- libsignal-service-rs: ~4 endpoint wrappers + DTOs (~100 lines, template
  exists in the same file).
- presage: one orchestration method + store persistence (~120 lines).
- auxin-ng: compat handler + response shape (~40 lines).

The only genuinely fiddly parts are (a) the reserve→confirm state machine
edge cases (reuse-current-nickname, healing a missing link — can be cut to
"always reserve fresh" for forest's needs, which never reclaims), and
(b) verifying the `/v1` vs `/v2` reserve/confirm prefix against production.

## Wire-format cheat sheet (verified against 3 sources)

- Username hash: 32 bytes, base64url-no-pad on the wire and in
  `signal.me` fragments.
- Link fragment: `b64url_nopad(entropy(32) ‖ handle_uuid_be(16))` after
  `#eu/`.
- Proof: variable-length poksho serialization, b64url in `zkProof`.
- Link blob ≤ 128 bytes (server limit), stored server-side under the
  handle; rotating the blob with `keepLinkHandle: true` keeps the URL.

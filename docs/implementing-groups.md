# Implementing full group support

Status: **phases 1–3 implemented and verified against Signal's production
servers** (2026-08-09), except group avatars and the phase 4 invite-link work.

Every RPC method has been exercised against production, single-account and
two-account: `createGroup` (solo and with invitees), `leaveGroup` (incl.
last-admin refusal and `force`), `setGroupTitle`, `setGroupDescription`,
`setGroupTimer`, `addGroupMembers`, `removeGroupMembers`,
`setGroupMemberRole` (promotion to admin, applied to a real member), and the
no-op / guard paths for each.

The two-account round trip additionally confirmed the **read** side end to
end: a second account accepting its invitation, and later renaming the group
as an admin, both arrived as signed `groupChange` blobs that decrypted and
persisted automatically — changes we did not author. The recipient also
confirmed each of our changes (rename, timer set to 1h, timer cleared, admin
promotion) applied on their device without a restart, with correct values.

`acceptGroupInvite` is the one method whose success path is unverified *as an
RPC*: the second account accepted through the Signal app, so our promote-
pending-member action builder ran only as the thing being read, not the thing
being sent. Its "no invitation" refusal is verified.

This doc is the roadmap for full group lifecycle support: creation, membership
changes, attribute changes, invites, and leaving.

## What live testing found

Four defects that unit tests could not have caught, all fixed:

1. **Creation failed with HTTP 400** until the group carried an explicit
   `AccessControl`. Signal's clients always set one; a group omitting it is
   malformed. Defaults now used: attributes/members `Member`,
   `add_from_invite_link` `Unsatisfiable`.
2. **`leave_group` reported failure after succeeding.** The PATCH returned
   200, but the unconditional post-change refetch then failed — leaving the
   group means losing the membership proof needed to read it. An `Unauthorized`
   on that refetch is now the expected self-removal path, and the store is
   updated from pre-change state instead.
3. **The last-admin guard ignored pending members.** Leaving a group whose only
   other participant was an invitee was wrongly allowed, stranding them in a
   group nobody can administer once they accept.
4. **Removing a pending member silently did nothing** while still bumping the
   revision: revoking an invite needs
   `DeleteMemberPendingProfileKeyAction`, not `DeleteMemberAction`. Empty
   action sets are now short-circuited before the PATCH, since the server
   accepts them and announces a no-op change to everyone.

Work spans all three layers of the stack (see `README.md` / Cargo patch notes):

- `auxin-ng` — RPC surface
- `presage` fork (`i-infra/presage`, branch `profile-extended`) — orchestration
- `libsignal-service-rs` fork (`i-infra/libsignal-service-rs`, branch
  `profile-avatar-payment-address`) — endpoints + group crypto

## Background: how Groups v2 works

Group state lives on Signal's **groups server** (the storage host,
`Endpoint::storage`), not the chat server. The canonical state is a
`proto::Group`: every sensitive field (title, description, member service
IDs, profile keys, timer) is a ciphertext, encrypted client-side with keys
derived from the 32-byte **group master key** via
`zkgroup::GroupSecretParams::derive_from_master_key`. The server can
validate structure and membership *proofs* but reads no content.

Three credential types are in play:

| Credential | Proves | Obtained from | Status in fork |
|---|---|---|---|
| `AuthCredentialWithPni` | "I am ACI/PNI X" (blinded), used to auth all groups-server calls as a member | chat server `GET /v1/certificate/auth/group` | **done** — `groups_v2/manager.rs::get_authorization_for_today`, incl. 7-day cache |
| `ExpiringProfileKeyCredential` | "I know user Y's profile key" — required to add Y as a full member, and for your *own* membership entry at creation | chat server `GET /v1/profile/{aci}/{version}/{credential_request}` | **missing** — no endpoint; `groups_v2/model.rs` already has the `credential:` slot |
| Group auth presentation | membership in a specific group, per-request | derived locally from the two above + `GroupSecretParams` | **done** — `get_authorization_string` |

Mutations are submitted as `GroupChange.Actions` at `revision = current + 1`.
The server validates, **signs** the change, bumps the revision, and returns
the signed `GroupChange`. The author then announces the change to all members
(including removed ones) as a `DataMessage` with
`GroupContextV2 { master_key, revision, group_change }`; recipient clients
either apply the signed change directly or refetch state.

## What already works (read path)

- Incoming `GroupContextV2` → presage `upsert_group` fetches + decrypts group
  state when the revision is newer (`groups_v2/manager.rs::fetch_encrypted_group`,
  `operations.rs::decrypt_group`), persists to store.
- `auxin-ng`: `listGroups` (store), send-to-group by master-key hex
  (`app.rs::parse_group_master_key`, `rpc.rs` `group` param), auto-attaches
  `GroupContextV2` to outgoing `DataMessage`s (`app.rs:203`).
- presage `send_message_to_group`: per-member sealed-sender fan-out
  (`manager/registered.rs:1430`).
- Group avatar download (`retrieve_avatar`), group-change *decryption*
  (`operations.rs::decrypt_group_change`).
- Fork-only write-side crypto already present in `groups_v2/operations.rs`
  (upstream whisperfish lacks these): `encrypt_title`, `encrypt_description`,
  `encrypt_disappearing_messages_timer`, `encrypt_group_with_credentials`,
  `create_member_presentation`, `build_add_member_action(_with_credential)`,
  `build_remove_member_action`, `build_add_pending_member_action`,
  `build_remove_pending_member_action`.

## Phase 1 — libsignal-service-rs

### 1a. Groups-server write endpoints (`push_service/`)

Only `GET /v1/groups/` exists today (`push_service/mod.rs:197`). Add, all
auth'd with the group auth presentation:

- [x] `PUT /v1/groups/` — create; body is the encrypted `proto::Group`
      (`push_service/mod.rs::create_group`)
- [x] `PATCH /v1/groups/` — modify; body is `GroupChange.Actions`; response is
      the server-signed `GroupChange` (`push_service/mod.rs::patch_group`).
      409 surfaces as the typed `ServiceError::GroupPatchConflict`
- [x] `GET /v1/groups/logs/{fromRevision}` — paged change log (`GroupChanges`)
      (`push_service/mod.rs::get_group_logs`). Endpoint only; no caller yet —
      the mutators refetch full state after a change
- [ ] `GET /v1/groups/avatar/form` — upload form for the avatar CDN; avatar
      blob is encrypted client-side (same blob cipher as title) then POSTed
      to the CDN per the form
- [ ] (phase 4) `GET /v1/groups/token`, join-via-invite-link PATCH variant

### 1b. Profile key credential flow (`push_service/` + new module)

All of 1b landed in the new `groups_v2/credentials.rs`:

- [x] Build `ProfileKeyCredentialRequestContext` from a target's ACI +
      profile key — `create_credential_request_context`
- [x] `GET /v1/profile/{aci}/{profile_version}/{credential_request}` —
      `retrieve_profile_with_credential`
- [x] `receive_expiring_profile_key_credential` → `ExpiringProfileKeyCredential`
      — `receive_credential`. Note zkgroup *enforces* day-alignment and a
      ≤7-day window at receive time, so expiry handling is mandatory
- [x] Wired as `GroupsManager::fetch_profile_key_credential`, which returns
      `Ok(None)` (not an error) when the server declines to issue
- [x] Fallback when target's profile key is unknown: pending-member invite —
      `create_group` does this automatically per member

### 1c. Remaining action builders (`groups_v2/operations.rs`)

Field ciphers exist; each needs an `Actions`-level builder. All builders
take/produce encrypted values and set nothing else on the envelope:

- [ ] modify title / description / avatar-key / disappearing-timer actions
- [ ] modify member role (promote/demote admin)
- [ ] promote pending member (invite acceptance — presentation of own
      profile-key credential)
- [ ] modify access control (attributes / membership / invite-link)
- [ ] announcement-only flag
- [ ] banned-members add/remove (needed for clean re-add semantics after kick)
- [x] the `Actions` envelope itself — `build_actions(revision)`. Two
      corrections to the original plan, both from `groups.proto`: the client
      must **not** set `sourceUserId` (the server stamps it from the
      authenticated presentation) and must **not** set `group_id` at all —
      doing so is a documented 400. Covered by unit tests.
- [x] `build_leave_group_actions` — deletes own membership at `revision + 1`

## Phase 2 — presage fork

All mutators follow one canonical shape:

```
fetch current Group (server, not store — store may be stale)
  → build Actions at revision + 1
  → PATCH → signed GroupChange
  → update store
  → announce: DataMessage + GroupContextV2 { master_key, revision, group_change }
      fan-out to all members incl. removed ones
```

### 2a. Manager methods (`manager/registered.rs`)

- [x] `create_group(title, description, members, timer) -> GroupMasterKeyBytes`
      — as described. Requires a profile key for **self** (hard error if
      absent); per-member credential failure downgrades to a pending invite
      rather than failing the whole creation
- [x] `leave_group(master_key, force)` — removes self; refuses with
      `Error::LastGroupAdmin` when we are the only admin and others remain
- [x] **`modify_group`** — the shared mutation harness the rest of 2a plugs
      into: fetch-from-server → build actions at `revision + 1` → PATCH →
      refetch/save → announce, with the 409 loop built in. Each remaining
      mutator below is now just a closure that builds its actions.
- [x] `add_group_members(master_key, members)` — resolves per-member
      credential vs. pending path automatically; already-present members and
      outstanding invitees are filtered out first, so a repeat add is a no-op
- [x] `remove_group_members(master_key, members)` — removes full members *and*
      revokes outstanding invitations in one change
- [x] `set_group_title` / `set_group_description` / `set_group_timer`
      (`None`/0 clears description/timer)
- [ ] `set_group_avatar` — needs `GET /v1/groups/avatar/form` (1a) plus CDN
      upload; the only phase 1–3 item not built
- [x] `set_group_member_role(master_key, aci, role)` — rejects a target who is
      not a full member. Promotion verified live against a real member, and
      confirmed applied on the target's device
- [x] `accept_group_invite(master_key)` — promotes own pending entry via a
      profile-key presentation. The "no invitation" refusal is verified live;
      the success path is not yet exercised *as an RPC* (see status note)
- [ ] (phase 4) invite-link create/rotate/join, join-request approval

### 2b. Conflict handling

- [x] PATCH returns HTTP 409 on revision race → refetch, rebuild actions on
      the new revision, bounded retry (3 attempts) — in `modify_group`
- [ ] Surface a typed error if the change became moot (e.g. member already
      removed). Currently a moot change is submitted as a no-op action set;
      needs per-mutator predicates, so it lands with 2a

### 2c. Announcement details

- [x] Reuse the existing fan-out sender; recipient set = post-change members
      ∪ removed members — `announce_group_change` unions pre- and post-change
      membership, so a removed member still receives the change
- [x] Empty-body `DataMessage` (group context only); expire-timer autofill is
      skipped — `announce_group_change` deliberately does *not* call
      `restore_thread_timer`, unlike `send_message_to_group`
- [x] Announcement failure is logged, not fatal: the change is already
      committed server-side and stored locally, so returning an error would
      wrongly imply it did not happen

### 2d. Store additions (`store.rs` + `presage-store-sqlite`)

- [x] Profile-key-credential cache with expiry-aware lookup —
      `ProfileKeyCredentialCache` in `groups_v2/credentials.rs`, with a 1h skew
      so a credential can't lapse between fetch and use. **In-memory and not
      yet wired into the mutators** (they fetch per call); persisting it in
      `presage-store-sqlite` is the remaining piece
- [x] No schema change needed for groups themselves — confirmed; `save_group`
      is keyed by master key and the mutators reuse it

## Phase 3 — auxin-ng

- [x] `createGroup` (params: `title`, `description`, `members[]`, `timer`)
      and `leaveGroup` (params: `group`, `force`) in `rpc.rs`
- [x] `addGroupMembers` / `removeGroupMembers` (`group`, `members[]`),
      `setGroupTitle` (`group`, `title`), `setGroupDescription`
      (`group`, `description?`), `setGroupTimer` (`group`, `timer?` seconds),
      `setGroupMemberRole` (`group`, `member`, `role`:
      `ADMINISTRATOR`|`DEFAULT`), `acceptGroupInvite` (`group`)
- [x] Mutators return the post-change group view (`revision`, `title`,
      `memberCount`) read back from the store, so callers can confirm the
      change landed rather than just that the call returned
- [ ] `setGroupAvatar` — blocked on `set_group_avatar` (2a)
- [x] Group handle stays master-key hex; `createGroup` returns it as
      `{"masterKey": "<hex>", "title": …}`
- [x] `listGroups` gains members/roles/pending detail
- [ ] Emit decrypted group-change events on receive (`emit.rs`)
- [ ] `docs/signal-cli-compat.md`: map new methods to signal-cli's
      `updateGroup`/`quitGroup` JSON-RPC where shapes align

Note: `schema.rs` turned out to be protobuf-descriptor introspection, not an
RPC method registry — there is no method schema to update for new RPCs.

## Phase 4 — invite links & join requests (optional, after 1–3)

Invite-link password on the group, link-access control, `GET /v1/groups/token`,
join-via-link (auto or admin-approval), `requesting_members` approval/denial
actions. Self-contained; nothing in phases 1–3 depends on it.

## Explicitly out of scope

- **Sender key / sealed-sender v2 group sends** — the per-member fan-out is
  O(members × devices) but correct; efficiency work, separate effort.
- **Storage-service group records** — multi-device sync concern; bot is
  single-device.
- **GV1** — dead protocol; ignore migration paths beyond the existing
  `derive_v2_migration_master_key`.

## Edge cases & gotchas

- **409 revision races** are normal under concurrent admins — always rebuild
  from refetched state, never blind-retry the same Actions.
- **Removed members** must receive the announcement or their clients keep the
  group alive as a "zombie".
- **Last admin leaving**: server permits it (confirmed — it accepted the
  change without complaint); clients treat an admin-less group as degraded.
  Refused in `leave_group` unless `force`. The check must count **pending
  members** as well as full members, or the sole admin can walk away from a
  group an invitee is about to join.
- **Credential expiry mid-flight**: auth credentials are day-scoped, profile
  key credentials expire on the order of days — both caches need
  expiry-aware fallback to refetch, not hard failure.
- **Pending members** count against group size and can be re-invited; adding
  an ACI already pending must be a no-op, not an error. Removing one needs
  `DeleteMemberPendingProfileKeyAction` — a `DeleteMemberAction` naming them
  is silently accepted and does nothing.
- **Empty changes are accepted by the server**, bump the revision and announce
  a no-op to every member. Any mutator that filters its targets against
  current state can produce one, so `modify_group` short-circuits before the
  PATCH (`is_empty_change`).
- **Banned list**: removing a member without banning lets them rejoin via
  link; kick-and-ban is one atomic Actions set.

## Live verification

Only the groups server can confirm it accepts what we build — a wrong `Actions`
field or a malformed presentation fails at the server, not at compile time.
The sequence below was run against production on 2026-08-09 and found the four
defects listed at the top of this doc. Re-run it after any change to the write
path.

Build note: auxin-ng builds against the *published* forks, so the presage and
libsignal-service-rs changes must be committed and pushed before auxin-ng
compiles standalone. For local iteration use the path patches already
commented in `Cargo.toml`.

Use a **throwaway group**, not a real one. Group mutations are announced to
every member and are not reversible client-side: a bad revision bump or an
errant `leaveGroup` is visible to everyone in the group. Adding a member sends
them a real invite, so only add an account you control.

1. `createGroup` with **only yourself** — exercises the credential fetch, the
   encrypted `proto::Group`, and `PUT`, with no announcement fan-out. If the
   credential flow is wrong this fails here, in isolation.
2. `createGroup` with one member whose profile key you hold → expect them as a
   full member; and one you don't → expect a pending invite. Confirm via
   `listGroups` and on the recipient's device.
3. Attribute mutators (`setGroupTitle`, `setGroupDescription`,
   `setGroupTimer`) — each must bump the revision by exactly one, and
   `listGroups` must show the new value decrypting correctly.
4. `addGroupMembers` then repeat the same call → the second must **not** change
   the revision. Same for `removeGroupMembers`. A revision bump on a repeat is
   the empty-change bug.
5. `leaveGroup` on a group where you are *not* the last admin — exercises the
   full `modify_group` path: PATCH, signed change, announcement.
6. `leaveGroup` as last admin, with another member *or a pending invitee*
   present → expect `LastGroupAdmin` refusal; retry with `force: true`.

Watch for: a 400 on PATCH (usually a field the client must leave unset, or a
missing one like `AccessControl`), a 409 loop that never converges, a revision
bump from a call that should have been a no-op, and removed members whose
clients still show the group (announcement fan-out missed them).

### Two-account round trip

Steps 1–6 above only prove we can *write*. Confirming that other clients
actually apply our changes — and that we apply theirs — needs a second account
you control. Run on a throwaway group; each step sends real notifications.

1. `createGroup` including the second account. If no profile key is held for
   them they arrive as a **pending invite**, which is correct.
2. Accept the invitation in the Signal app on the second device, then send a
   message from it. Ours should receive a `groupChange` promoting them, apply
   it automatically (full member, empty pending list), and learn their profile
   key from the message — after which they can be added as a full member
   directly rather than invited.
3. Run `setGroupTitle`, `setGroupTimer` (set then clear) and
   `setGroupMemberRole` (promote to admin) from our side. On the second device
   each must appear **without restarting the app**, with correct values — a
   timer showing 0 or a garbled title means broken attribute encryption.
4. Rename the group *from the second device*. Ours must decrypt and persist a
   change it did not author — this is the only step exercising the read path
   for a `GroupChange` from another admin.
5. `removeGroupMembers` on the second account while it is an **admin**. The
   removed client must stop treating the group as live; if it still lets them
   post, the announcement fan-out is missing removed members.

Note that a mutator failing *after* a 200 PATCH does not mean the change was
rejected — the change is committed server-side from that point on. Check the
server's view via a fresh `listGroups` before assuming a reported error means
nothing happened.

## Effort ranking

Original estimate: presage orchestration (2) > credential flow (1b) > action
builders (1c) ≈ endpoints (1a) > auxin-ng RPC (3).

Held up in practice. Orchestration was the bulk of the work, and most of it was
the shared `modify_group` harness rather than the mutators themselves — which
is why the remaining 2a methods should now be comparatively cheap: each is a
closure that builds one action list. The credential flow was smaller than
expected because the fork's write-side crypto was already in place.

## Reference implementations

- Signal-Android `GroupManagerV2` / libsignal-service-java `GroupsV2Operations`
  — canonical Actions semantics and announcement behavior
- signal-cli `updateGroup` — JSON-RPC surface precedent (see
  `docs/signal-cli-compat.md`)
- whisperfish upstream PRs for groups-v2 write support, if any have landed
  since the fork point (`f74b96e` / `bb43e81`) — check before building 1a/1b
  from scratch

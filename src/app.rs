//! Shared application logic used by both the CLI commands and the JSON-RPC
//! daemon: store setup, sending, and the receive loop.

use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};
use futures::{pin_mut, StreamExt};
use mime_guess::mime::APPLICATION_OCTET_STREAM;
use presage::libsignal_service::content::{Content, ContentBody};
use presage::libsignal_service::protocol::ServiceId;
use presage::libsignal_service::sender::AttachmentSpec;
use presage::libsignal_service::zkgroup::GroupMasterKeyBytes;
use presage::model::identity::OnNewIdentity;
use presage::model::messages::Received;
use presage::proto::{AttachmentPointer, DataMessage, GroupContextV2};
use presage::store::ContentsStore;
use presage::{manager::Registered, Manager};
use presage_store_sqlite::SqliteStore;
use serde_json::Value;

use crate::emit::Event;
use crate::proto_json;

pub type App = Manager<SqliteStore, Registered>;

fn data_dir() -> PathBuf {
    directories::ProjectDirs::from("io", "auxin", "auxin-ng")
        .expect("home directory exists")
        .data_dir()
        .to_path_buf()
}

pub fn default_db_path() -> PathBuf {
    data_dir().join("auxin.db3")
}

pub fn accounts_dir() -> PathBuf {
    data_dir().join("accounts")
}

/// Database path for a named account (`--account`), kept under
/// `<data-dir>/accounts/<name>.db3`.
pub fn account_db_path(name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        || name.starts_with('.')
    {
        bail!("account names may only contain [A-Za-z0-9._-] and must not start with a dot");
    }
    Ok(accounts_dir().join(format!("{name}.db3")))
}

/// signal-cli compat: database path under `--config <dir>`, selected by
/// `--user <account>`. signal-cli accounts are phone numbers, so `+` is
/// allowed in addition to the usual account-name characters.
pub fn compat_db_path(config: &Path, user: Option<&str>) -> Result<PathBuf> {
    let Some(user) = user else {
        return Ok(config.join("auxin.db3"));
    };
    if user.is_empty()
        || !user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '+'))
        || user.starts_with('.')
    {
        bail!("--user may only contain [A-Za-z0-9+._-] and must not start with a dot");
    }
    Ok(config.join("accounts").join(format!("{user}.db3")))
}

/// Inventory of local account databases: the default db plus everything
/// under the accounts directory. Reads registration data only — no network.
pub async fn list_accounts() -> Value {
    let mut candidates: Vec<(String, PathBuf)> = Vec::new();
    let default = default_db_path();
    if default.exists() {
        candidates.push(("default".into(), default));
    }
    if let Ok(entries) = std::fs::read_dir(accounts_dir()) {
        let mut named: Vec<_> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "db3"))
            .collect();
        named.sort();
        for path in named {
            let name = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            candidates.push((name, path));
        }
    }

    let mut accounts = Vec::new();
    for (name, path) in candidates {
        let mut entry = serde_json::json!({
            "account": name,
            "path": path.display().to_string(),
        });
        match describe_account(&path).await {
            Ok(details) => {
                entry["status"] = "registered".into();
                for (k, v) in details {
                    entry[k] = v;
                }
            }
            Err(status) => entry["status"] = status.into(),
        }
        accounts.push(entry);
    }
    Value::Array(accounts)
}

async fn describe_account(path: &Path) -> Result<Vec<(&'static str, Value)>, String> {
    use presage::store::StateStore;
    let store = open_store(path, None)
        .await
        .map_err(|_| "unreadable (encrypted or corrupt)".to_string())?;
    let data = match store.load_registration_data().await {
        Ok(Some(data)) => data,
        Ok(None) => return Err("unregistered".to_string()),
        Err(_) => return Err("unreadable (encrypted or corrupt)".to_string()),
    };
    Ok(vec![
        ("aci", data.service_ids.aci.to_string().into()),
        ("pni", data.service_ids.pni.to_string().into()),
        ("phoneNumber", data.phone_number.to_string().into()),
        ("deviceName", data.device_name().map(str::to_string).into()),
        (
            "deviceId",
            data.device_id.map(Value::from).unwrap_or(Value::Null),
        ),
        ("servers", format!("{:?}", data.signal_servers).into()),
    ])
}

pub async fn open_store(db_path: &Path, passphrase: Option<&str>) -> Result<SqliteStore> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    SqliteStore::open_with_passphrase(
        &db_path.display().to_string(),
        passphrase,
        OnNewIdentity::Trust,
    )
    .await
    .context("failed to open local store")
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}

pub fn parse_service_id(input: &str) -> Result<ServiceId> {
    ServiceId::parse_from_service_id_string(input)
        .with_context(|| format!("invalid service id: {input}"))
}

pub fn parse_group_master_key(input: &str) -> Result<GroupMasterKeyBytes> {
    let bytes = hex::decode(input).context("group master key is not valid hex")?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("group master key must be 32 bytes"))
}

pub enum Recipient {
    Contact(ServiceId),
    Group(GroupMasterKeyBytes),
}

/// Build the content body for a send request from either plain text or raw
/// proto-JSON (`content`), attaching uploaded attachment pointers.
pub async fn build_body(
    manager: &App,
    recipient: &Recipient,
    message: Option<String>,
    content: Option<&Value>,
    content_type: &str,
    attachments: Vec<PathBuf>,
) -> Result<ContentBody> {
    let mut body = match (message, content) {
        (Some(_), Some(_)) => bail!("pass either a text message or raw content, not both"),
        (None, Some(json)) => proto_json::json_to_body(content_type, json)?,
        (Some(text), None) => ContentBody::DataMessage(DataMessage {
            body: Some(text),
            ..Default::default()
        }),
        (None, None) if !attachments.is_empty() => ContentBody::DataMessage(DataMessage::default()),
        (None, None) => bail!("nothing to send: pass a message, raw content, or attachments"),
    };

    let pointers = upload_attachments(manager, attachments).await?;

    if let ContentBody::DataMessage(data_message) = &mut body {
        data_message.timestamp = Some(now_millis());
        data_message.attachments.extend(pointers);
        if let Recipient::Group(master_key) = recipient {
            if data_message.group_v2.is_none() {
                let revision = manager
                    .store()
                    .group(*master_key)
                    .await
                    .ok()
                    .flatten()
                    .map(|g| g.revision)
                    .unwrap_or(0);
                data_message.group_v2 = Some(GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(revision),
                    ..Default::default()
                });
            }
        }
    } else if !pointers.is_empty() {
        bail!("attachments require a data message body");
    }

    Ok(body)
}

pub async fn send(manager: &mut App, recipient: Recipient, body: ContentBody) -> Result<Value> {
    let timestamp = match &body {
        ContentBody::DataMessage(m) => m.timestamp.unwrap_or_else(now_millis),
        _ => now_millis(),
    };
    let (content_type, json) = proto_json::body_to_json(&body)?;
    match recipient {
        Recipient::Contact(service_id) => {
            manager.send_message(service_id, body, timestamp).await?;
        }
        Recipient::Group(master_key) => {
            manager
                .send_message_to_group(&master_key, body, timestamp)
                .await?;
        }
    }
    Ok(serde_json::json!({
        "timestamp": timestamp,
        "contentType": content_type,
        "content": json,
    }))
}

pub async fn upload_attachments(
    manager: &App,
    paths: Vec<PathBuf>,
) -> Result<Vec<AttachmentPointer>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let mut specs = Vec::with_capacity(paths.len());
    for path in paths {
        let data = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        specs.push((
            AttachmentSpec {
                content_type: mime_guess::from_path(&path)
                    .first()
                    .unwrap_or(APPLICATION_OCTET_STREAM)
                    .to_string(),
                length: data.len(),
                file_name: path.file_name().map(|s| s.to_string_lossy().to_string()),
                preview: None,
                voice_note: None,
                borderless: None,
                width: None,
                height: None,
                caption: None,
                blur_hash: None,
            },
            data,
        ));
    }
    manager
        .upload_attachments(specs)
        .await?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .context("attachment upload failed")
}

pub struct ReceiveOptions {
    pub stop_after_empty_queue: bool,
    /// Download attachments into this directory, emitting an
    /// `attachmentDownloaded` event per file. `None` disables downloads.
    pub attachments_dir: Option<PathBuf>,
    /// On the first data message from each sender, send a delivery receipt
    /// and share our profile key, so new conversations get our profile
    /// details without a manual accept.
    pub auto_accept: bool,
    /// Download attachments before emitting the message event, recording the
    /// local paths on the event itself (signal-cli compat needs the paths in
    /// the envelope). Off, downloads happen after the message is emitted.
    pub download_before_emit: bool,
    /// Send a delivery receipt for every incoming message, including group
    /// messages. Without it senders never see the second checkmark.
    pub send_receipts: bool,
}

/// Stream incoming messages, invoking `on_event` for each JSONL event.
///
/// Presage's message stream terminates when the websocket dies (failed
/// keepalives, server-side idle drop) and never reconnects on its own, so
/// this reconnects with capped exponential backoff until told to stop.
pub async fn receive(
    manager: &mut App,
    options: ReceiveOptions,
    mut on_event: impl FnMut(Event),
) -> Result<()> {
    if let Some(dir) = &options.attachments_dir {
        std::fs::create_dir_all(dir)?;
    }

    let mut accepted: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut sender_manager = manager.clone();
    let own_aci = manager.registration_data().service_ids.aci.to_string();

    let mut attempt: u32 = 0;
    loop {
        let messages = match manager.receive_messages().await {
            Ok(messages) => messages,
            // The very first connection failing is a config/credential
            // problem worth dying loudly over; later failures get retried.
            Err(error) if attempt == 0 => {
                return Err(error).context("failed to initialize message stream")
            }
            Err(error) => {
                on_event(Event::Error {
                    message: format!("failed to reconnect message stream: {error:#}"),
                });
                attempt = backoff(attempt, &mut on_event).await;
                continue;
            }
        };
        pin_mut!(messages);

        while let Some(received) = messages.next().await {
            attempt = 0;
            match received {
                Received::QueueEmpty => {
                    on_event(Event::QueueEmpty);
                    if options.stop_after_empty_queue {
                        return Ok(());
                    }
                }
                Received::Contacts => on_event(Event::ContactsSynced),
                Received::DecryptionError(sender) => on_event(Event::DecryptionError {
                    sender: sender.service_id_string(),
                }),
                Received::Content(content) => {
                    let mut downloads = Vec::new();
                    if options.download_before_emit {
                        if let Some(dir) = &options.attachments_dir {
                            downloads =
                                download_attachments(manager, dir, &content, &mut on_event).await;
                        }
                    }
                    on_event(Event::message(&content, downloads));
                    // Acknowledge before any attachment work, so a slow or
                    // failing download never delays the sender's checkmark.
                    if options.send_receipts && should_send_receipt(&content, &own_aci) {
                        send_delivery_receipt(&mut sender_manager, &content, &mut on_event).await;
                    }
                    if options.auto_accept {
                        auto_accept(
                            &mut sender_manager,
                            &own_aci,
                            &mut accepted,
                            &content,
                            &mut on_event,
                        )
                        .await;
                    }
                    if !options.download_before_emit {
                        if let Some(dir) = &options.attachments_dir {
                            download_attachments(manager, dir, &content, &mut on_event).await;
                        }
                    }
                }
            }
        }

        // Stream ended: the websocket is gone. Reconnect.
        attempt = backoff(attempt, &mut on_event).await;
    }
}

async fn backoff(attempt: u32, on_event: &mut impl FnMut(Event)) -> u32 {
    let delay_ms = 1000u64.saturating_mul(1 << attempt.min(6)); // 1s .. 64s
    on_event(Event::Reconnecting {
        attempt: attempt + 1,
        delay_ms,
    });
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    attempt + 1
}

/// Send a delivery receipt for a received message.
///
/// Without this the sender's client shows one checkmark (accepted by the
/// server) and never advances to two (delivered to the recipient). Signal
/// expects the receipt to reference the *original message's* timestamp, which
/// is how the sender matches it to the message it acknowledges — the outer
/// envelope timestamp is a fresh one for the receipt itself.
///
/// Receipts go to the message's author directly, never to the group: a group
/// message still yields one receipt per recipient, addressed to the sender.
async fn send_delivery_receipt(
    manager: &mut App,
    content: &Content,
    on_event: &mut impl FnMut(Event),
) {
    let sender = content.metadata.sender;
    let message_timestamp = content.metadata.timestamp.timestamp_millis() as u64;

    let receipt = ContentBody::ReceiptMessage(presage::proto::ReceiptMessage {
        r#type: Some(presage::proto::receipt_message::Type::Delivery as i32),
        timestamp: vec![message_timestamp],
    });

    if let Err(error) = manager.send_message(sender, receipt, now_millis()).await {
        on_event(Event::Error {
            message: format!(
                "delivery receipt to {} failed: {error:#}",
                sender.service_id_string()
            ),
        });
    }
}

/// Whether a received message warrants a delivery receipt.
///
/// Only actual messages from other people do. Receipts, typing indicators,
/// sync messages and our own echoes must not be acknowledged — receipting a
/// receipt would bounce back and forth forever.
///
/// Note this deliberately ignores the envelope's `needs_receipt` flag: it is
/// set on incoming receipts too, so keying off it would create exactly the
/// loop above.
fn should_send_receipt(content: &Content, own_aci: &str) -> bool {
    if content.metadata.sender.service_id_string() == own_aci {
        return false;
    }
    matches!(
        content.body,
        ContentBody::DataMessage(_) | ContentBody::EditMessage(_)
    )
}

/// "Accept" a first contact from a new sender: acknowledge with a delivery
/// receipt and share our profile key (a `PROFILE_KEY_UPDATE` data message),
/// which is what lets the peer decrypt our profile name/details.
async fn auto_accept(
    manager: &mut App,
    own_aci: &str,
    accepted: &mut std::collections::HashSet<String>,
    content: &Content,
    on_event: &mut impl FnMut(Event),
) {
    // Only direct data messages from someone else start a conversation.
    let ContentBody::DataMessage(data_message) = &content.body else {
        return;
    };
    if data_message.group_v2.is_some() {
        return;
    }
    let sender = content.metadata.sender.service_id_string();
    if sender == own_aci || accepted.contains(&sender) {
        return;
    }
    accepted.insert(sender.clone());

    let service_id = content.metadata.sender;

    let profile_share = ContentBody::DataMessage(DataMessage {
        profile_key: Some(
            manager
                .registration_data()
                .profile_key()
                .get_bytes()
                .to_vec(),
        ),
        flags: Some(presage::proto::data_message::Flags::ProfileKeyUpdate as u32),
        timestamp: Some(now_millis()),
        ..Default::default()
    });

    // The delivery receipt is sent by the receive loop for every message, so
    // auto-accept only has to share our profile key here.
    let timestamp = now_millis();
    if let Err(error) = manager
        .send_message(service_id, profile_share, timestamp)
        .await
    {
        on_event(Event::Error {
            message: format!("auto-accept send to {sender} failed: {error:#}"),
        });
        return;
    }
    on_event(Event::Accepted { sender });
}

/// Download every attachment on a data message into `dir`, emitting an
/// `attachmentDownloaded` event per file. Returns the local paths keyed by
/// the attachment's index in `DataMessage.attachments` (failed downloads
/// are absent).
async fn download_attachments(
    manager: &App,
    dir: &Path,
    content: &Content,
    on_event: &mut impl FnMut(Event),
) -> Vec<crate::emit::DownloadedAttachment> {
    let mut downloads = Vec::new();
    let ContentBody::DataMessage(DataMessage { attachments, .. }) = &content.body else {
        return downloads;
    };
    for (index, pointer) in attachments.iter().enumerate() {
        let data = match manager.get_attachment(pointer).await {
            Ok(data) => data,
            Err(error) => {
                on_event(Event::Error {
                    message: format!("failed to fetch attachment: {error:#}"),
                });
                continue;
            }
        };
        let extension = mime_guess::get_mime_extensions_str(
            pointer
                .content_type
                .as_deref()
                .unwrap_or("application/octet-stream"),
        )
        .and_then(|e| e.first())
        .unwrap_or(&"bin");
        let timestamp = content.metadata.timestamp.timestamp_millis();
        let base = pointer
            .file_name
            .clone()
            .unwrap_or_else(|| format!("attachment-{timestamp}"));
        let path = dir.join(format!("{timestamp}-{base}.{extension}"));
        match tokio::fs::write(&path, &data).await {
            Ok(()) => {
                downloads.push(crate::emit::DownloadedAttachment {
                    index,
                    path: path.display().to_string(),
                });
                on_event(Event::AttachmentDownloaded {
                    sender: content.metadata.sender.service_id_string(),
                    timestamp,
                    path: path.display().to_string(),
                    content_type: pointer.content_type.clone(),
                    file_name: pointer.file_name.clone(),
                });
            }
            Err(error) => on_event(Event::Error {
                message: format!("failed to write attachment {}: {error:#}", path.display()),
            }),
        }
    }
    downloads
}

/// JSON summaries of local store data.
pub async fn list_contacts(manager: &App) -> Result<Value> {
    let contacts: Vec<Value> = manager
        .store()
        .contacts()
        .await?
        .filter_map(|c| c.ok())
        .map(|c| {
            serde_json::json!({
                "uuid": c.uuid.to_string(),
                "phoneNumber": c.phone_number.as_ref().map(|p| p.to_string()),
                "name": c.name,
            })
        })
        .collect();
    Ok(Value::Array(contacts))
}

pub async fn list_groups(manager: &App) -> Result<Value> {
    let groups: Vec<Value> = manager
        .store()
        .groups()
        .await?
        .filter_map(|g| g.ok())
        .map(|(master_key, group)| {
            let members: Vec<Value> = group
                .members
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "uuid": m.aci.service_id_string(),
                        "role": role_name(m.role),
                    })
                })
                .collect();
            let pending_members: Vec<Value> = group
                .pending_members
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "uuid": m.uuid.to_string(),
                        "role": role_name(m.role),
                        "addedBy": m.added_by_aci.service_id_string(),
                    })
                })
                .collect();
            let requesting_members: Vec<Value> = group
                .requesting_members
                .iter()
                .map(|m| serde_json::json!({ "uuid": m.aci.service_id_string() }))
                .collect();
            let invite_link = stored_group_invite_link(&master_key, &group);
            serde_json::json!({
                "masterKey": hex::encode(master_key),
                "title": group.title,
                "description": group.description,
                "revision": group.revision,
                "memberCount": group.members.len(),
                "members": members,
                "pendingMembers": pending_members,
                "requestingMembers": requesting_members,
                "inviteLink": invite_link,
                "hasAvatar": !group.avatar.is_empty(),
            })
        })
        .collect();
    Ok(Value::Array(groups))
}

/// The invite link for a stored group, when links are enabled — the same
/// rule presage applies after a change.
fn stored_group_invite_link(
    master_key: &GroupMasterKeyBytes,
    group: &presage::model::groups::Group,
) -> Option<String> {
    use presage::libsignal_service::groups_v2::{AccessRequired, GroupInviteLink};
    let enabled = matches!(
        group
            .access_control
            .as_ref()
            .map(|ac| ac.add_from_invite_link),
        Some(AccessRequired::Any | AccessRequired::Administrator)
    ) && !group.invite_link_password.is_empty();
    enabled.then(|| {
        GroupInviteLink {
            master_key: *master_key,
            password: group.invite_link_password.clone(),
        }
        .to_url()
    })
}

fn role_name(role: presage::libsignal_service::groups_v2::Role) -> &'static str {
    use presage::libsignal_service::groups_v2::Role;
    match role {
        Role::Administrator => "ADMINISTRATOR",
        Role::Default => "DEFAULT",
        Role::Unknown => "UNKNOWN",
    }
}

/// Creates a group and returns its master key hex — the handle every other
/// group method takes.
///
/// Members we hold no profile key for are invited as pending members rather
/// than added outright; they show up under `pendingMembers` in `listGroups`
/// until they accept.
pub async fn create_group(
    manager: &mut App,
    title: &str,
    description: Option<&str>,
    members: Vec<String>,
    timer_seconds: Option<u32>,
) -> Result<Value> {
    let members = members
        .iter()
        .map(|m| parse_service_id(m.as_str()))
        .collect::<Result<Vec<_>>>()?;

    let timer =
        timer_seconds.map(|duration| presage::libsignal_service::prelude::Timer { duration });

    let master_key = manager
        .create_group(title, description, members, timer)
        .await?;

    Ok(serde_json::json!({
        "masterKey": hex::encode(master_key),
        "title": title,
    }))
}

/// Leaves a group. Refuses if we are its last admin unless `force` is set,
/// since an admin-less group can never be modified again.
pub async fn leave_group(manager: &mut App, group: &str, force: bool) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    manager.leave_group(&master_key, force).await?;
    Ok(serde_json::json!({
        "masterKey": group,
        "left": true,
    }))
}

/// Adds members to a group. Anyone whose profile key we don't hold is invited
/// as a pending member instead; already-present members are skipped.
pub async fn add_group_members(
    manager: &mut App,
    group: &str,
    members: Vec<String>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let members = members
        .iter()
        .map(|m| parse_service_id(m.as_str()))
        .collect::<Result<Vec<_>>>()?;
    manager.add_group_members(&master_key, members).await?;
    group_result(manager, group, &master_key).await
}

/// Removes members from a group. Members not in the group are skipped.
pub async fn remove_group_members(
    manager: &mut App,
    group: &str,
    members: Vec<String>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let acis = members
        .iter()
        .map(|m| parse_aci(m.as_str()))
        .collect::<Result<Vec<_>>>()?;
    manager.remove_group_members(&master_key, acis).await?;
    group_result(manager, group, &master_key).await
}

pub async fn set_group_title(manager: &mut App, group: &str, title: &str) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    manager.set_group_title(&master_key, title).await?;
    group_result(manager, group, &master_key).await
}

pub async fn set_group_description(
    manager: &mut App,
    group: &str,
    description: Option<&str>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    manager
        .set_group_description(&master_key, description)
        .await?;
    group_result(manager, group, &master_key).await
}

/// Sets the disappearing-message timer in seconds; `None` (or 0) disables it.
pub async fn set_group_timer(
    manager: &mut App,
    group: &str,
    seconds: Option<u32>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let timer = match seconds {
        None | Some(0) => None,
        Some(duration) => Some(presage::libsignal_service::prelude::Timer { duration }),
    };
    manager.set_group_timer(&master_key, timer).await?;
    group_result(manager, group, &master_key).await
}

/// Promotes a member to `ADMINISTRATOR` or demotes them to `DEFAULT`.
pub async fn set_group_member_role(
    manager: &mut App,
    group: &str,
    member: &str,
    role: &str,
) -> Result<Value> {
    use presage::libsignal_service::groups_v2::Role;

    let master_key = parse_group_master_key(group)?;
    let aci = parse_aci(member)?;
    let role = match role.to_ascii_uppercase().as_str() {
        "ADMINISTRATOR" | "ADMIN" => Role::Administrator,
        "DEFAULT" | "MEMBER" => Role::Default,
        other => bail!("unknown role {other}; expected ADMINISTRATOR or DEFAULT"),
    };
    manager
        .set_group_member_role(&master_key, aci, role)
        .await?;
    group_result(manager, group, &master_key).await
}

/// Accepts an outstanding invitation, promoting our pending entry to a full
/// membership.
pub async fn accept_group_invite(manager: &mut App, group: &str) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    manager.accept_group_invite(&master_key).await?;
    group_result(manager, group, &master_key).await
}

/// Sets the group avatar from an image file, or clears it when `path` is
/// `None`. The bytes are uploaded as-is (encrypted); Signal clients render
/// JPEG/PNG/WebP.
pub async fn set_group_avatar(manager: &mut App, group: &str, path: Option<&str>) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let bytes = match path {
        Some(path) => Some(
            tokio::fs::read(path)
                .await
                .with_context(|| format!("reading avatar {path}"))?,
        ),
        None => None,
    };
    manager
        .set_group_avatar(&master_key, bytes.as_deref())
        .await?;
    let mut result = group_result(manager, group, &master_key).await?;
    result["avatarSet"] = Value::Bool(path.is_some());
    Ok(result)
}

/// The server-signed change log of a group, oldest first. Only revisions
/// from our own join onward are visible; `from_revision` narrows further.
pub async fn get_group_history(
    manager: &mut App,
    group: &str,
    from_revision: Option<u32>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let entries = manager.group_history(&master_key, from_revision).await?;
    let history: Vec<Value> = entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "revision": entry.change.version,
                "editor": entry.change.editor.service_id_string(),
                "changeEpoch": entry.change.change_epoch,
                "changes": entry.change.changes.iter().map(group_change_json).collect::<Vec<_>>(),
                "state": entry.state.as_ref().map(|g| serde_json::json!({
                    "title": g.title,
                    "description": g.description_text,
                    "memberCount": g.members.len(),
                })),
            })
        })
        .collect();
    Ok(serde_json::json!({
        "masterKey": group,
        "history": history,
    }))
}

/// One decrypted group change as a tagged JSON object.
fn group_change_json(change: &presage::libsignal_service::groups_v2::GroupChange) -> Value {
    use presage::libsignal_service::groups_v2::GroupChange::*;
    use serde_json::json;

    match change {
        NewMember(m) => {
            json!({"type": "memberAdded", "uuid": m.aci.service_id_string(), "role": role_name(m.role)})
        }
        DeleteMember(aci) => json!({"type": "memberRemoved", "uuid": aci.service_id_string()}),
        ModifyMemberRole { aci, role } => {
            json!({"type": "memberRoleChanged", "uuid": aci.service_id_string(), "role": role_name(*role)})
        }
        ModifyMemberProfileKey { aci, .. } => {
            json!({"type": "memberProfileKeyChanged", "uuid": aci.service_id_string()})
        }
        NewPendingMember(p) => {
            json!({"type": "memberInvited", "serviceId": p.address.service_id_string(), "addedBy": p.added_by_aci.service_id_string()})
        }
        DeletePendingMember(id) => {
            json!({"type": "inviteRevoked", "serviceId": id.service_id_string()})
        }
        PromotePendingMember { address, .. } => {
            json!({"type": "inviteAccepted", "serviceId": address.service_id_string()})
        }
        Title(title) => json!({"type": "titleChanged", "title": title}),
        Avatar(key) => json!({"type": "avatarChanged", "cleared": key.is_empty()}),
        Timer(timer) => {
            json!({"type": "timerChanged", "seconds": timer.as_ref().map(|t| t.duration).unwrap_or(0)})
        }
        AttributeAccess(a) => json!({"type": "attributeAccessChanged", "access": access_name(*a)}),
        MemberAccess(a) => json!({"type": "memberAccessChanged", "access": access_name(*a)}),
        InviteLinkAccess(a) => {
            json!({"type": "inviteLinkAccessChanged", "access": access_name(*a)})
        }
        NewRequestingMember(r) => {
            json!({"type": "joinRequested", "uuid": r.aci.service_id_string()})
        }
        DeleteRequestingMember(aci) => {
            json!({"type": "joinRequestDenied", "uuid": aci.service_id_string()})
        }
        PromoteRequestingMember { aci, role } => {
            json!({"type": "joinRequestApproved", "uuid": aci.service_id_string(), "role": role_name(*role)})
        }
        InviteLinkPassword(_) => json!({"type": "inviteLinkPasswordChanged"}),
        Description(d) => json!({"type": "descriptionChanged", "description": d}),
        AnnouncementOnly(on) => json!({"type": "announcementsOnlyChanged", "enabled": on}),
        AddBannedMember(b) => {
            json!({"type": "memberBanned", "serviceId": b.user_id.service_id_string()})
        }
        DeleteBannedMember(id) => {
            json!({"type": "memberUnbanned", "serviceId": id.service_id_string()})
        }
        PromotePendingPniAciMemberProfileKey(p) => {
            json!({"type": "inviteAcceptedByPni", "uuid": p.aci.service_id_string()})
        }
        MemberLabel {
            user_id,
            label_emoji,
            label_string,
        } => {
            json!({"type": "memberLabelChanged", "serviceId": user_id.service_id_string(), "emoji": label_emoji, "label": label_string})
        }
        MemberLabelAccess(a) => {
            json!({"type": "memberLabelAccessChanged", "access": access_name(*a)})
        }
    }
}

fn access_name(access: presage::libsignal_service::groups_v2::AccessRequired) -> &'static str {
    use presage::libsignal_service::groups_v2::AccessRequired;
    match access {
        AccessRequired::Any => "ANY",
        AccessRequired::Member => "MEMBER",
        AccessRequired::Administrator => "ADMINISTRATOR",
        AccessRequired::Unsatisfiable => "UNSATISFIABLE",
        AccessRequired::Unknown => "UNKNOWN",
    }
}

/// Enables, reconfigures or disables the group's invite link.
///
/// `access`: `disabled`, `anyone` (link holders join immediately) or
/// `adminApproval` (link holders file a request). `reset_password`
/// invalidates every previously shared link.
pub async fn set_group_invite_link(
    manager: &mut App,
    group: &str,
    access: &str,
    reset_password: bool,
) -> Result<Value> {
    use presage::manager::GroupInviteLinkAccess;

    let master_key = parse_group_master_key(group)?;
    let access = match access.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
        "disabled" | "off" | "none" => GroupInviteLinkAccess::Disabled,
        "anyone" | "any" | "open" => GroupInviteLinkAccess::Anyone,
        "adminapproval" | "approval" | "administrator" | "admin" => {
            GroupInviteLinkAccess::AdminApproval
        }
        other => {
            bail!("unknown invite link access {other}; expected disabled, anyone or adminApproval")
        }
    };
    let link = manager
        .set_group_invite_link(&master_key, access, reset_password)
        .await?;
    let mut result = group_result(manager, group, &master_key).await?;
    result["inviteLink"] = link.map(Value::String).unwrap_or(Value::Null);
    Ok(result)
}

/// The group's current invite link, or null when links are disabled.
pub async fn get_group_invite_link(manager: &App, group: &str) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let link = manager.group_invite_link(&master_key).await?;
    Ok(serde_json::json!({
        "masterKey": group,
        "inviteLink": link,
    }))
}

/// Previews what an invite link leads to, without joining.
pub async fn get_group_join_info(manager: &mut App, link: &str) -> Result<Value> {
    let info = manager.group_join_info(link).await?;
    let master_key =
        presage::libsignal_service::groups_v2::GroupInviteLink::parse(link)?.master_key;
    Ok(serde_json::json!({
        "masterKey": hex::encode(master_key),
        "title": info.title,
        "description": info.description,
        "memberCount": info.member_count,
        "revision": info.version,
        // What joining through this link would do.
        "requiresAdminApproval":
            info.add_from_invite_link == presage::libsignal_service::groups_v2::AccessRequired::Administrator,
        "linkAccess": access_name(info.add_from_invite_link),
        // Whether *we* already have a request awaiting approval.
        "requestPending": info.pending_admin_approval,
    }))
}

/// Joins a group through its invite link. `status` is `joined` when we are
/// now a member, or `requested` when an admin still has to approve.
pub async fn join_group(manager: &mut App, link: &str) -> Result<Value> {
    use presage::manager::JoinGroupOutcome;

    let (status, master_key) = match manager.join_group_via_invite_link(link).await? {
        JoinGroupOutcome::Joined(key) => ("joined", key),
        JoinGroupOutcome::Requested(key) => ("requested", key),
    };
    let group = hex::encode(master_key);
    let mut result = group_result(manager, &group, &master_key).await?;
    result["status"] = Value::String(status.into());
    Ok(result)
}

/// Approves join requests from the given ACIs; non-requesters are skipped.
pub async fn approve_group_join_requests(
    manager: &mut App,
    group: &str,
    members: Vec<String>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let acis = members
        .iter()
        .map(|m| parse_aci(m.as_str()))
        .collect::<Result<Vec<_>>>()?;
    manager
        .approve_group_join_requests(&master_key, acis)
        .await?;
    group_result(manager, group, &master_key).await
}

/// Denies join requests from the given ACIs; non-requesters are skipped.
pub async fn deny_group_join_requests(
    manager: &mut App,
    group: &str,
    members: Vec<String>,
) -> Result<Value> {
    let master_key = parse_group_master_key(group)?;
    let acis = members
        .iter()
        .map(|m| parse_aci(m.as_str()))
        .collect::<Result<Vec<_>>>()?;
    manager.deny_group_join_requests(&master_key, acis).await?;
    group_result(manager, group, &master_key).await
}

/// A service ID that must be an ACI — group membership is keyed by ACI, so a
/// PNI here is a caller mistake worth naming.
fn parse_aci(input: &str) -> Result<presage::libsignal_service::protocol::Aci> {
    match parse_service_id(input)? {
        ServiceId::Aci(aci) => Ok(aci),
        ServiceId::Pni(_) => bail!("{input} is a PNI; group members are identified by ACI"),
    }
}

/// Post-mutation view of a group, read back from the store so callers see the
/// applied result rather than just an acknowledgement.
async fn group_result(
    manager: &App,
    group: &str,
    master_key: &GroupMasterKeyBytes,
) -> Result<Value> {
    let stored = manager.store().group(*master_key).await.ok().flatten();
    Ok(serde_json::json!({
        "masterKey": group,
        "title": stored.as_ref().map(|g| g.title.clone()),
        "revision": stored.as_ref().map(|g| g.revision),
        "memberCount": stored.as_ref().map(|g| g.members.len()),
    }))
}

/// Fetch a profile (own when `recipient` is `None`), bypassing the local
/// cache, as JSON. `mobileCoinAddress` is only present when its signature
/// verified against the profile owner's identity key.
pub async fn get_profile(manager: &mut App, recipient: Option<&str>) -> Result<Value> {
    let (uuid, profile_key) = match recipient {
        None => {
            let data = manager.registration_data();
            (data.service_ids.aci, data.profile_key())
        }
        Some(id) => {
            let service_id = parse_service_id(id)?;
            let uuid = uuid::Uuid::from_bytes(service_id.raw_uuid().into_bytes());
            let contact = manager
                .store()
                .contact_by_id(&service_id)
                .await?
                .with_context(|| format!("no known contact {id}"))?;
            let key_bytes: [u8; 32] = contact
                .profile_key
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("no profile key stored for {id}"))?;
            (
                uuid,
                presage::libsignal_service::zkgroup::profiles::ProfileKey::create(key_bytes),
            )
        }
    };
    let profile = manager
        .retrieve_profile_by_uuid_uncached(uuid, profile_key)
        .await?;
    Ok(serde_json::json!({
        "uuid": uuid.to_string(),
        "givenName": profile.name.as_ref().map(|n| n.given_name.clone()),
        "familyName": profile.name.as_ref().and_then(|n| n.family_name.clone()),
        "about": profile.about,
        "aboutEmoji": profile.about_emoji,
        "avatar": profile.avatar,
        "mobileCoinAddress":
            crate::compat::mobile_coin_address_b64(profile.payment_address.as_deref()),
    }))
}

pub async fn whoami(manager: &App) -> Result<Value> {
    let whoami = manager.whoami().await?;
    Ok(serde_json::json!({
        "aci": whoami.aci.to_string(),
        "pni": whoami.pni.to_string(),
        "number": whoami.number.to_string(),
        "deviceId": u32::from(manager.device_id()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::content::Metadata;
    use presage::libsignal_service::protocol::{Aci, DeviceId};

    const OWN: &str = "38b0b933-925a-43ac-849d-e19528ec8d08";
    const OTHER: &str = "471c2cac-72e2-4157-9878-759ac84b397f";

    fn content(sender: &str, body: ContentBody) -> Content {
        let aci = Aci::parse_from_service_id_string(sender).expect("valid aci");
        Content {
            metadata: Metadata {
                sender: aci.into(),
                destination: aci.into(),
                sender_device: DeviceId::new(1).unwrap(),
                timestamp: chrono::Utc::now(),
                server_timestamp: chrono::Utc::now(),
                // Set on incoming receipts too, which is exactly why
                // should_send_receipt must not key off it.
                needs_receipt: true,
                unidentified_sender: true,
                was_plaintext: false,
                server_guid: None,
            },
            body,
        }
    }

    fn data_message(body: &str) -> ContentBody {
        ContentBody::DataMessage(DataMessage {
            body: Some(body.into()),
            timestamp: Some(1),
            ..Default::default()
        })
    }

    #[test]
    fn receipts_are_sent_for_messages_from_others() {
        assert!(should_send_receipt(
            &content(OTHER, data_message("hello")),
            OWN
        ));
    }

    #[test]
    fn group_messages_still_get_receipts() {
        // Regression: group messages were previously skipped entirely, so
        // senders never saw a second checkmark in any group.
        let body = ContentBody::DataMessage(DataMessage {
            body: Some("in a group".into()),
            group_v2: Some(presage::proto::GroupContextV2 {
                master_key: Some(vec![0u8; 32]),
                revision: Some(3),
                ..Default::default()
            }),
            timestamp: Some(1),
            ..Default::default()
        });
        assert!(should_send_receipt(&content(OTHER, body), OWN));
    }

    #[test]
    fn receipts_are_not_sent_for_receipts() {
        // The loop-breaker: acknowledging an acknowledgement never terminates.
        let receipt = ContentBody::ReceiptMessage(presage::proto::ReceiptMessage {
            r#type: Some(presage::proto::receipt_message::Type::Delivery as i32),
            timestamp: vec![1],
        });
        assert!(!should_send_receipt(&content(OTHER, receipt), OWN));
    }

    #[test]
    fn receipts_are_not_sent_for_typing_or_our_own_messages() {
        let typing = ContentBody::TypingMessage(presage::proto::TypingMessage {
            timestamp: Some(1),
            ..Default::default()
        });
        assert!(!should_send_receipt(&content(OTHER, typing), OWN));

        // Our own message echoed back to us needs no acknowledgement.
        assert!(!should_send_receipt(
            &content(OWN, data_message("mine")),
            OWN
        ));
    }
}

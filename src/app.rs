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
            let name = path.file_stem().unwrap_or_default().to_string_lossy().into_owned();
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
            data.device_id.map(|id| Value::from(id)).unwrap_or(Value::Null),
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
        (None, None) if !attachments.is_empty() => {
            ContentBody::DataMessage(DataMessage::default())
        }
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
        let data =
            std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
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
                Received::Content(content) => {
                    let mut downloads = Vec::new();
                    if options.download_before_emit {
                        if let Some(dir) = &options.attachments_dir {
                            downloads =
                                download_attachments(manager, dir, &content, &mut on_event)
                                    .await;
                        }
                    }
                    on_event(Event::message(&content, downloads));
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
    let message_timestamp = content.metadata.timestamp.timestamp_millis() as u64;

    let receipt = ContentBody::ReceiptMessage(presage::proto::ReceiptMessage {
        r#type: Some(presage::proto::receipt_message::Type::Delivery as i32),
        timestamp: vec![message_timestamp],
    });
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

    for body in [receipt, profile_share] {
        let timestamp = now_millis();
        if let Err(error) = manager.send_message(service_id, body, timestamp).await {
            on_event(Event::Error {
                message: format!("auto-accept send to {sender} failed: {error:#}"),
            });
            return;
        }
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
            pointer.content_type.as_deref().unwrap_or("application/octet-stream"),
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
            serde_json::json!({
                "masterKey": hex::encode(master_key),
                "title": group.title,
                "description": group.description,
                "revision": group.revision,
                "memberCount": group.members.len(),
            })
        })
        .collect();
    Ok(Value::Array(groups))
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

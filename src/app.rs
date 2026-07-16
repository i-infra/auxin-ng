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
use presage::store::Store;
use presage::{manager::Registered, Manager};
use presage_store_sqlite::SqliteStore;
use serde_json::Value;

use crate::emit::Event;
use crate::proto_json;

pub type App = Manager<SqliteStore, Registered>;

pub fn default_db_path() -> PathBuf {
    directories::ProjectDirs::from("io", "auxin", "auxin-ng")
        .expect("home directory exists")
        .data_dir()
        .join("auxin.db3")
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
}

/// Stream incoming messages, invoking `on_event` for each JSONL event.
pub async fn receive(
    manager: &mut App,
    options: ReceiveOptions,
    mut on_event: impl FnMut(Event),
) -> Result<()> {
    if let Some(dir) = &options.attachments_dir {
        std::fs::create_dir_all(dir)?;
    }
    let messages = manager
        .receive_messages()
        .await
        .context("failed to initialize message stream")?;
    pin_mut!(messages);

    while let Some(received) = messages.next().await {
        match received {
            Received::QueueEmpty => {
                on_event(Event::QueueEmpty);
                if options.stop_after_empty_queue {
                    break;
                }
            }
            Received::Contacts => on_event(Event::ContactsSynced),
            Received::Content(content) => {
                on_event(Event::message(&content));
                if let Some(dir) = &options.attachments_dir {
                    download_attachments(manager, dir, &content, &mut on_event).await;
                }
            }
        }
    }
    Ok(())
}

async fn download_attachments(
    manager: &App,
    dir: &Path,
    content: &Content,
    on_event: &mut impl FnMut(Event),
) {
    let ContentBody::DataMessage(DataMessage { attachments, .. }) = &content.body else {
        return;
    };
    for pointer in attachments {
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
            Ok(()) => on_event(Event::AttachmentDownloaded {
                sender: content.metadata.sender.service_id_string(),
                timestamp,
                path: path.display().to_string(),
                content_type: pointer.content_type.clone(),
                file_name: pointer.file_name.clone(),
            }),
            Err(error) => on_event(Event::Error {
                message: format!("failed to write attachment {}: {error:#}", path.display()),
            }),
        }
    }
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

pub async fn whoami(manager: &App) -> Result<Value> {
    let whoami = manager.whoami().await?;
    Ok(serde_json::json!({
        "aci": whoami.aci.to_string(),
        "pni": whoami.pni.to_string(),
        "number": whoami.number.to_string(),
        "deviceId": u32::from(manager.device_id()),
    }))
}

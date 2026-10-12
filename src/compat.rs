//! signal-cli compatibility layer (`--signal-cli-compat`).
//!
//! Translates between signal-cli's JSON-RPC dialect and auxin-ng's native
//! one, covering the wire contract forestfire actually uses (see
//! docs/signal-cli-compat.md): the `send`, `sendReaction`, `sendTyping`,
//! `sendPaymentNotification`, `updateProfile`, `updateAccount`, and
//! `listContacts` methods, plus signal-cli-shaped `receive` notifications
//! for incoming messages.
//!
//! signal-cli identifies groups by the base64 **group identifier** (derived
//! from the master key via zkgroup); auxin-ng natively uses the hex master
//! key. The mapping here iterates the store's groups and derives each id.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use base64::prelude::{Engine, BASE64_STANDARD};
use presage::libsignal_service::zkgroup::groups::{GroupMasterKey, GroupSecretParams};
use presage::libsignal_service::zkgroup::GroupMasterKeyBytes;
use presage::store::ContentsStore;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::app::{self, App, Recipient};

/// Base64 group identifier for a master key, as signal-cli's `groupId`.
pub fn group_id_b64(master_key: &GroupMasterKeyBytes) -> String {
    let params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(*master_key));
    BASE64_STANDARD.encode(params.get_group_identifier())
}

/// Resolve a signal-cli base64 `groupId` to the stored group master key.
pub async fn master_key_for_group_id(manager: &App, group_id: &str) -> Result<GroupMasterKeyBytes> {
    let wanted = BASE64_STANDARD
        .decode(group_id)
        .context("groupId is not valid base64")?;
    let mut groups = manager.store().groups().await?;
    while let Some(Ok((master_key, _))) = groups.next() {
        let params =
            GroupSecretParams::derive_from_master_key(GroupMasterKey::new(master_key));
        if params.get_group_identifier().as_slice() == wanted.as_slice() {
            return Ok(master_key);
        }
    }
    bail!("no known group with id {group_id}")
}

/// A param that signal-cli accepts as either a scalar or a list; forest
/// sends `sendTyping` recipients as single-element lists.
#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrList {
    One(String),
    Many(Vec<String>),
}

impl StringOrList {
    fn first(self) -> Option<String> {
        match self {
            StringOrList::One(s) => Some(s),
            StringOrList::Many(v) => v.into_iter().next(),
        }
    }
}

/// `send` params, accepting signal-cli's dashed/camel/snake key spellings.
#[derive(Deserialize)]
struct SendParams {
    recipient: Option<StringOrList>,
    #[serde(rename = "group-id", alias = "groupId", alias = "group_id")]
    group_id: Option<String>,
    message: Option<String>,
    #[serde(default)]
    attachments: Vec<String>,
    #[serde(rename = "end_session", alias = "endSession", alias = "end-session", default)]
    end_session: bool,
    /// `"<packIdHex>:<stickerId>"`.
    sticker: Option<String>,
    /// Raw proto-JSON DataMessage fields, merged over the assembled message
    /// (auxin-ng superset kept from the old auxin backend).
    content: Option<Value>,
}

#[derive(Deserialize)]
struct SendReactionParams {
    emoji: String,
    #[serde(rename = "target-author", alias = "targetAuthor", alias = "target_author")]
    target_author: String,
    #[serde(
        rename = "target-timestamp",
        alias = "targetTimestamp",
        alias = "target_timestamp"
    )]
    target_timestamp: u64,
    #[serde(default)]
    remove: bool,
    recipient: Option<StringOrList>,
    /// Note: key is `group` (base64 group id) for this method, not `group-id`.
    #[serde(alias = "group-id", alias = "groupId", alias = "group_id")]
    group: Option<StringOrList>,
}

#[derive(Deserialize)]
struct SendTypingParams {
    recipient: Option<StringOrList>,
    #[serde(alias = "group-id", alias = "groupId", alias = "group_id")]
    group: Option<StringOrList>,
    #[serde(default)]
    stop: bool,
}

#[derive(Deserialize)]
struct SendPaymentNotificationParams {
    recipient: StringOrList,
    /// Base64 MobileCoin receipt blob.
    receipt: String,
    #[serde(default)]
    note: String,
}

#[derive(Deserialize)]
struct UpdateProfileParams {
    #[serde(rename = "givenName", alias = "given-name", alias = "name")]
    given_name: Option<String>,
    #[serde(rename = "familyName", alias = "family-name")]
    family_name: Option<String>,
    about: Option<String>,
    #[serde(rename = "aboutEmoji", alias = "about-emoji")]
    about_emoji: Option<String>,
    #[serde(rename = "mobileCoinAddress", alias = "mobile-coin-address")]
    mobile_coin_address: Option<String>,
    avatar: Option<String>,
}

#[derive(Deserialize)]
struct ListContactsParams {
    recipient: Option<StringOrList>,
    #[serde(rename = "allRecipients", alias = "all-recipients", default)]
    #[allow(dead_code)]
    all_recipients: bool,
}

/// Resolve signal-cli-style addressing (`recipient` uuid vs base64 group id)
/// to an auxin-ng `Recipient`.
async fn resolve_recipient(
    manager: &App,
    recipient: Option<String>,
    group_id: Option<String>,
) -> Result<Recipient> {
    match (recipient, group_id) {
        (Some(id), None) => Ok(Recipient::Contact(app::parse_service_id(&id)?)),
        (None, Some(group_id)) => Ok(Recipient::Group(
            master_key_for_group_id(manager, &group_id).await?,
        )),
        _ => bail!("pass exactly one of `recipient` or a group id"),
    }
}

/// Shallow-merge `overlay`'s keys over `base` (both JSON objects).
fn merge_content(base: &mut Value, overlay: &Value) -> Result<()> {
    let (Value::Object(base), Value::Object(overlay)) = (base, overlay) else {
        bail!("`content` must be a JSON object");
    };
    for (key, value) in overlay {
        base.insert(key.clone(), value.clone());
    }
    Ok(())
}

pub async fn handle_send(manager: &mut App, params: Value) -> Result<Value> {
    let params: SendParams = serde_json::from_value(params)?;
    let recipient = resolve_recipient(
        manager,
        params.recipient.and_then(StringOrList::first),
        params.group_id,
    )
    .await?;

    // Assemble the DataMessage as proto-JSON, then let the native path
    // handle attachments, timestamps, and group context.
    let mut content = json!({});
    if let Some(message) = params.message {
        if !message.is_empty() {
            content["body"] = message.into();
        }
    }
    if params.end_session {
        // END_SESSION (1) is reserved in current protos; kept for signal-cli compat.
        content["flags"] = json!(1u32);
    }
    if let Some(sticker) = &params.sticker {
        content["sticker"] = sticker_json(manager, sticker).await?;
    }
    if let Some(overlay) = &params.content {
        merge_content(&mut content, overlay)?;
    }
    if content.as_object().is_some_and(Map::is_empty) && params.attachments.is_empty() {
        bail!("nothing to send: pass a message, sticker, content, or attachments");
    }

    let body = app::build_body(
        manager,
        &recipient,
        None,
        Some(&content),
        "DataMessage",
        params.attachments.iter().map(Into::into).collect(),
    )
    .await?;
    let group_id = match &recipient {
        Recipient::Group(master_key) => Some(group_id_b64(master_key)),
        Recipient::Contact(_) => None,
    };
    let mut result = app::send(manager, recipient, body).await?;
    if let Some(group_id) = group_id {
        result["groupId"] = group_id.into();
    }
    Ok(result)
}

/// Look up an installed sticker pack by hex pack id and build the
/// proto-JSON `DataMessage.sticker` for `"<packIdHex>:<stickerId>"`.
async fn sticker_json(manager: &App, sticker: &str) -> Result<Value> {
    let (pack_id_hex, sticker_id) = sticker
        .split_once(':')
        .context("sticker must be <packIdHex>:<stickerId>")?;
    let sticker_id: u32 = sticker_id.parse().context("sticker id is not a number")?;
    let pack_id = hex::decode(pack_id_hex).context("sticker pack id is not valid hex")?;
    let pack = manager
        .store()
        .sticker_pack(&pack_id)
        .await?
        .with_context(|| format!("sticker pack {pack_id_hex} is not installed"))?;
    let emoji = pack
        .manifest
        .stickers
        .iter()
        .find(|s| s.id == sticker_id)
        .and_then(|s| s.emoji.clone());
    let mut value = json!({
        "packId": BASE64_STANDARD.encode(&pack_id),
        "packKey": BASE64_STANDARD.encode(&pack.key),
        "stickerId": sticker_id,
    });
    if let Some(emoji) = emoji {
        value["emoji"] = emoji.into();
    }
    Ok(value)
}

pub async fn handle_send_reaction(manager: &mut App, params: Value) -> Result<Value> {
    let params: SendReactionParams = serde_json::from_value(params)?;
    let recipient = resolve_recipient(
        manager,
        params.recipient.and_then(StringOrList::first),
        params.group.and_then(StringOrList::first),
    )
    .await?;
    let content = json!({
        "reaction": {
            "emoji": params.emoji,
            "remove": params.remove,
            "targetAuthorAci": params.target_author,
            "targetSentTimestamp": params.target_timestamp.to_string(),
        }
    });
    let body = app::build_body(manager, &recipient, None, Some(&content), "DataMessage", vec![])
        .await?;
    app::send(manager, recipient, body).await
}

pub async fn handle_send_typing(manager: &mut App, params: Value) -> Result<Value> {
    let params: SendTypingParams = serde_json::from_value(params)?;
    let recipient = resolve_recipient(
        manager,
        params.recipient.and_then(StringOrList::first),
        params.group.and_then(StringOrList::first),
    )
    .await?;
    let timestamp = app::now_millis();
    let action = if params.stop {
        presage::proto::typing_message::Action::Stopped
    } else {
        presage::proto::typing_message::Action::Started
    };
    let body = presage::libsignal_service::content::ContentBody::TypingMessage(
        presage::proto::TypingMessage {
            timestamp: Some(timestamp),
            action: Some(action as i32),
            group_id: match &recipient {
                Recipient::Group(master_key) => {
                    let params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(
                        *master_key,
                    ));
                    Some(params.get_group_identifier().to_vec())
                }
                Recipient::Contact(_) => None,
            },
        },
    );
    app::send(manager, recipient, body).await
}

pub async fn handle_send_payment_notification(
    manager: &mut App,
    params: Value,
) -> Result<Value> {
    let params: SendPaymentNotificationParams = serde_json::from_value(params)?;
    let recipient = resolve_recipient(manager, params.recipient.first(), None).await?;
    // Validate early so a bad blob errors before hitting the proto layer.
    BASE64_STANDARD
        .decode(&params.receipt)
        .context("receipt is not valid base64")?;
    let content = json!({
        "payment": {
            "notification": {
                "mobileCoin": {"receipt": params.receipt},
                "note": params.note,
            }
        }
    });
    let body = app::build_body(manager, &recipient, None, Some(&content), "DataMessage", vec![])
        .await?;
    app::send(manager, recipient, body).await
}

pub async fn handle_update_profile(manager: &mut App, params: Value) -> Result<Value> {
    use presage::manager::AvatarUpdate;

    let params: UpdateProfileParams = serde_json::from_value(params)?;

    // A profile write replaces the whole profile server-side, so merge the
    // request over the current profile (signal-cli semantics: only provided
    // fields change).
    let own_aci = manager.registration_data().service_ids.aci;
    let own_key = manager.registration_data().profile_key();
    let current = manager
        .retrieve_profile_by_uuid_uncached(own_aci, own_key)
        .await
        .unwrap_or_default();

    let (current_given, current_family) = current
        .name
        .map(|n| (n.given_name, n.family_name))
        .unwrap_or_default();
    let name = presage::libsignal_service::profile_name::ProfileName {
        given_name: params.given_name.unwrap_or(current_given),
        family_name: params.family_name.or(current_family),
    };
    let about = params.about.or(current.about);
    let about_emoji = params.about_emoji.or(current.about_emoji);

    let avatar = match &params.avatar {
        Some(path) => AvatarUpdate::Set(
            std::fs::read(path)
                .with_context(|| format!("reading avatar {path}"))?,
        ),
        None => AvatarUpdate::Retain,
    };

    let payment_address = match &params.mobile_coin_address {
        // Empty string clears the stored address (a profile write without
        // it drops the field server-side).
        Some(b64) if b64.is_empty() => None,
        // A fresh address gets signed with our identity key.
        Some(b64) => {
            let address_bytes = BASE64_STANDARD
                .decode(b64)
                .context("mobileCoinAddress is not valid base64")?;
            Some(manager.sign_payment_address(&address_bytes).await?)
        }
        // No new address: pass the current one back through, else the
        // write clears it.
        None => current.payment_address.as_deref().and_then(|bytes| {
            <presage::proto::PaymentAddress as prost::Message>::decode(bytes).ok()
        }),
    };

    let avatar_key = manager
        .update_profile_extended(name, about, about_emoji, avatar, payment_address)
        .await?;
    let mut result = json!({});
    if let Some(avatar_key) = avatar_key {
        result["avatarUrlPath"] = avatar_key.into();
    }
    Ok(result)
}

#[derive(Deserialize)]
struct UpdateAccountParams {
    username: Option<String>,
    #[serde(rename = "deleteUsername", alias = "delete-username", default)]
    delete_username: bool,
}

/// signal-cli's `updateAccount`: forest uses only the `username` param and
/// reads back `result.usernameLink` for `/signalme`.
pub async fn handle_update_account(manager: &mut App, params: Value) -> Result<Value> {
    let params: UpdateAccountParams = serde_json::from_value(params)?;
    match (params.username, params.delete_username) {
        (Some(_), true) => bail!("pass either `username` or `deleteUsername`, not both"),
        (Some(username), false) => {
            let data = manager.set_username(&username).await?;
            Ok(json!({
                "username": data.username,
                "usernameLink": data.link().map(|url| url.to_string()),
            }))
        }
        (None, true) => {
            manager.delete_username().await?;
            Ok(json!({}))
        }
        (None, false) => Ok(json!({})),
    }
}

pub async fn handle_list_contacts(manager: &mut App, params: Value) -> Result<Value> {
    let params: ListContactsParams = serde_json::from_value(params)?;
    let filter = params.recipient.and_then(StringOrList::first);
    let stored: Vec<_> = manager
        .store()
        .contacts()
        .await?
        .filter_map(|c| c.ok())
        .collect();
    let mut contacts = Vec::new();
    for contact in stored {
        let uuid = contact.uuid.to_string();
        let number = contact.phone_number.as_ref().map(|p| p.to_string());
        if let Some(filter) = &filter {
            if *filter != uuid && Some(filter.as_str()) != number.as_deref() {
                continue;
            }
        }
        // An explicitly requested recipient gets a fresh profile fetch
        // (forest reads the payment address right before sending a payment);
        // bulk listings settle for the cache.
        let profile = fetch_profile_json(manager, &contact, filter.is_some()).await;
        contacts.push(json!({
            "uuid": uuid,
            "number": number,
            "username": Value::Null,
            "name": contact.name,
            "profile": profile,
        }));
    }
    Ok(Value::Array(contacts))
}

/// signal-cli's `JsonContact.profile` shape, from the cached/live profile.
/// `mobileCoinAddress` is the base64 raw MobileCoin public address, present
/// only when its signature verified against the owner's identity key.
async fn fetch_profile_json(
    manager: &mut App,
    contact: &presage::model::contacts::Contact,
    fresh: bool,
) -> Value {
    let Ok(key_bytes) = <[u8; 32]>::try_from(contact.profile_key.as_slice()) else {
        return Value::Null;
    };
    let profile_key =
        presage::libsignal_service::zkgroup::profiles::ProfileKey::create(key_bytes);
    let profile = if fresh {
        manager
            .retrieve_profile_by_uuid_uncached(contact.uuid, profile_key)
            .await
    } else {
        manager
            .retrieve_profile_by_uuid(contact.uuid, profile_key)
            .await
    };
    match profile {
        Ok(profile) => json!({
            "givenName": profile.name.as_ref().map(|n| n.given_name.clone()),
            "familyName": profile.name.as_ref().and_then(|n| n.family_name.clone()),
            "about": profile.about,
            "aboutEmoji": profile.about_emoji,
            "hasAvatar": profile.avatar.is_some(),
            "mobileCoinAddress": mobile_coin_address_b64(profile.payment_address.as_deref()),
        }),
        Err(error) => {
            tracing::debug!(uuid = %contact.uuid, "profile fetch failed: {error:#}");
            Value::Null
        }
    }
}

/// Base64 of the raw MobileCoin public address inside decrypted
/// `PaymentAddress` protobuf bytes (signal-cli's `profile.mobileCoinAddress`).
pub fn mobile_coin_address_b64(payment_address: Option<&[u8]>) -> Value {
    use presage::proto::payment_address::Address;

    let address = payment_address
        .and_then(|bytes| {
            <presage::proto::PaymentAddress as prost::Message>::decode(bytes).ok()
        })
        .and_then(|address| match address.address {
            Some(Address::MobileCoin(mobile_coin)) => mobile_coin.public_address,
            None => None,
        });
    match address {
        Some(bytes) => BASE64_STANDARD.encode(bytes).into(),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Incoming: signal-cli `receive` envelope
// ---------------------------------------------------------------------------

/// Build the signal-cli `receive` notification for a native message event
/// (the JSON of `Event::Message`), or `None` for event types signal-cli
/// doesn't announce. `source_name` and `source_number` come from the store;
/// `group_ids` maps hex master keys to base64 group ids.
pub fn receive_notification(
    account: &str,
    event: &Value,
    source_name: Option<String>,
    source_number: Option<String>,
    group_id_b64: Option<String>,
) -> Option<Value> {
    if event.get("type").and_then(Value::as_str) != Some("message") {
        return None;
    }
    let native = event.get("envelope")?;
    let content = event.get("content")?;
    let content_type = event.get("contentType").and_then(Value::as_str)?;

    let source_uuid = native.get("sender").cloned().unwrap_or(Value::Null);
    let mut envelope = json!({
        "source": source_uuid,
        "sourceNumber": source_number,
        "sourceUuid": source_uuid,
        "sourceName": source_name,
        "sourceDevice": native.get("senderDevice"),
        "timestamp": native.get("timestamp"),
        "serverReceivedTimestamp": native.get("serverTimestamp"),
        "serverDeliveredTimestamp": native.get("serverTimestamp"),
    });

    match content_type {
        "dataMessage" => {
            let downloads = event.get("downloads");
            envelope["dataMessage"] = data_message_json(content, downloads, group_id_b64);
        }
        "typingMessage" => {
            let mut typing = json!({
                "action": content.get("action").cloned().unwrap_or(Value::Null),
                "timestamp": as_number(content.get("timestamp")),
            });
            if let Some(group_id) = content.get("groupId") {
                typing["groupId"] = group_id.clone();
            }
            envelope["typingMessage"] = typing;
        }
        "receiptMessage" => {
            let kind = content.get("type").and_then(Value::as_str).unwrap_or("");
            let timestamps: Vec<Value> = content
                .get("timestamp")
                .and_then(Value::as_array)
                .map(|t| t.iter().map(|v| as_number(Some(v))).collect())
                .unwrap_or_default();
            envelope["receiptMessage"] = json!({
                "when": timestamps.first().cloned().unwrap_or(Value::Null),
                "isDelivery": kind == "DELIVERY",
                "isRead": kind == "READ",
                "isViewed": kind == "VIEWED",
                "timestamps": timestamps,
            });
        }
        // Sync/call/story/etc. messages aren't read by forest; announce the
        // bare envelope so timestamps still flow.
        _ => {}
    }

    Some(json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {"envelope": envelope, "account": account},
    }))
}

/// Flatten a proto-JSON `DataMessage` into signal-cli's `dataMessage` shape.
fn data_message_json(
    content: &Value,
    downloads: Option<&Value>,
    group_id_b64: Option<String>,
) -> Value {
    let mut out = json!({
        "timestamp": as_number(content.get("timestamp")),
        "message": content.get("body").cloned().unwrap_or(Value::Null),
    });
    if let Some(expire) = content.get("expireTimer") {
        out["expiresInSeconds"] = expire.clone();
    }

    if let Some(attachments) = content.get("attachments").and_then(Value::as_array) {
        let files: HashMap<usize, &str> = downloads
            .and_then(Value::as_array)
            .map(|d| {
                d.iter()
                    .filter_map(|entry| {
                        Some((
                            entry.get("index")?.as_u64()? as usize,
                            entry.get("path")?.as_str()?,
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        out["attachments"] = attachments
            .iter()
            .enumerate()
            .map(|(index, pointer)| {
                let id = pointer
                    .get("cdnId")
                    .or_else(|| pointer.get("cdnKey"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let mut attachment = json!({
                    "contentType": pointer.get("contentType"),
                    "filename": pointer.get("fileName"),
                    "id": id,
                    "size": pointer.get("size"),
                });
                if let Some(file) = files.get(&index) {
                    attachment["file"] = (*file).into();
                }
                attachment
            })
            .collect();
    }

    let mentions: Vec<Value> = content
        .get("bodyRanges")
        .and_then(Value::as_array)
        .map(|ranges| {
            ranges
                .iter()
                .filter(|range| range.get("mentionAci").is_some())
                .map(|range| {
                    let aci = range.get("mentionAci").cloned().unwrap_or(Value::Null);
                    json!({
                        "name": aci,
                        "uuid": aci,
                        "start": range.get("start").cloned().unwrap_or(json!(0)),
                        "length": range.get("length").cloned().unwrap_or(json!(0)),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    if !mentions.is_empty() {
        out["mentions"] = mentions.into();
    }

    if content.get("groupV2").is_some() {
        out["groupInfo"] = json!({"groupId": group_id_b64, "type": "DELIVER"});
    }
    if let Some(quote) = content.get("quote") {
        out["quote"] = json!({
            "id": as_number(quote.get("id")),
            "authorUuid": quote.get("authorAci"),
            "text": quote.get("text"),
        });
    }
    if let Some(reaction) = content.get("reaction") {
        out["reaction"] = json!({
            "emoji": reaction.get("emoji"),
            "targetAuthorUuid": reaction.get("targetAuthorAci"),
            "targetSentTimestamp": as_number(reaction.get("targetSentTimestamp")),
            "isRemove": reaction
                .get("remove")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        });
    }
    if let Some(notification) = content.get("payment").and_then(|p| p.get("notification")) {
        out["payment"] = json!({
            "receipt": notification
                .get("mobileCoin")
                .and_then(|m| m.get("receipt"))
                .cloned()
                .unwrap_or(Value::Null),
            "note": notification.get("note").cloned().unwrap_or(Value::Null),
        });
    }
    if let Some(sticker) = content.get("sticker") {
        let pack_id_hex = sticker
            .get("packId")
            .and_then(Value::as_str)
            .and_then(|b64| BASE64_STANDARD.decode(b64).ok())
            .map(hex::encode);
        out["sticker"] = json!({
            "packId": pack_id_hex,
            "stickerId": sticker.get("stickerId").cloned().unwrap_or(json!(0)),
        });
    }
    out
}

/// Consume native events and forward signal-cli `receive` notifications.
/// Non-message events (queueEmpty, reconnecting, …) have no signal-cli
/// equivalent and are logged to stderr instead.
pub async fn notify_task(
    manager: App,
    account: String,
    mut events: tokio::sync::mpsc::UnboundedReceiver<Value>,
    out: tokio::sync::mpsc::UnboundedSender<String>,
) {
    while let Some(event) = events.recv().await {
        if event.get("type").and_then(Value::as_str) != Some("message") {
            tracing::debug!(event = %event, "suppressed native event in compat mode");
            continue;
        }
        let (source_name, source_number) = match event
            .pointer("/envelope/sender")
            .and_then(Value::as_str)
        {
            Some(sender) => lookup_source(&manager, sender).await,
            None => (None, None),
        };
        // The incoming message carries its own master key; derive the
        // signal-cli group id directly rather than scanning the store.
        let group_id = event
            .pointer("/content/groupV2/masterKey")
            .and_then(Value::as_str)
            .and_then(|b64| BASE64_STANDARD.decode(b64).ok())
            .and_then(|bytes| GroupMasterKeyBytes::try_from(bytes.as_slice()).ok())
            .map(|master_key| group_id_b64(&master_key));
        if let Some(notification) =
            receive_notification(&account, &event, source_name, source_number, group_id)
        {
            let _ = out.send(notification.to_string());
        }
    }
}

/// Contact name and phone number for a sender service id, if stored.
async fn lookup_source(manager: &App, sender: &str) -> (Option<String>, Option<String>) {
    let Ok(service_id) = app::parse_service_id(sender) else {
        return (None, None);
    };
    match manager.store().contact_by_id(&service_id).await {
        Ok(Some(contact)) => {
            let name = (!contact.name.is_empty()).then_some(contact.name);
            let number = contact.phone_number.as_ref().map(|p| p.to_string());
            (name, number)
        }
        _ => (None, None),
    }
}

/// Proto-JSON renders 64-bit integers as strings; signal-cli emits numbers.
fn as_number(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(s)) => match s.parse::<u64>() {
            Ok(n) => json!(n),
            Err(_) => s.parse::<i64>().map(|n| json!(n)).unwrap_or(Value::Null),
        },
        Some(v) => v.clone(),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message_event(content: Value) -> Value {
        json!({
            "type": "message",
            "envelope": {
                "sender": "11111111-1111-4111-8111-111111111111",
                "senderDevice": 2,
                "destination": "22222222-2222-4222-8222-222222222222",
                "timestamp": 1752650000000i64,
                "serverTimestamp": 1752650000100i64,
            },
            "contentType": "dataMessage",
            "content": content,
        })
    }

    #[test]
    fn receive_envelope_matches_what_forest_reads() {
        // Field paths from forest/message.py::StdioMessage.
        let event = message_event(json!({
            "body": "hello /command",
            "timestamp": "1752650000000",
            "expireTimer": 3600,
            "quote": {
                "id": "1752649990000",
                "authorAci": "33333333-3333-4333-8333-333333333333",
                "text": "original",
            },
            "reaction": {
                "emoji": "🔥",
                "targetAuthorAci": "33333333-3333-4333-8333-333333333333",
                "targetSentTimestamp": "1752649990000",
                "remove": true,
            },
            "bodyRanges": [
                {"start": 0, "length": 5, "mentionAci": "44444444-4444-4444-8444-444444444444"},
                {"start": 6, "length": 2, "style": "BOLD"},
            ],
            "payment": {"notification": {"mobileCoin": {"receipt": "cmVjZWlwdA=="}, "note": "thanks"}},
        }));
        let notification = receive_notification(
            "+15551234567",
            &event,
            Some("Alice".into()),
            Some("+15557654321".into()),
            None,
        )
        .unwrap();

        assert_eq!(notification["method"], "receive");
        assert!(notification.get("id").is_none());
        let params = &notification["params"];
        assert_eq!(params["account"], "+15551234567");
        let envelope = &params["envelope"];
        assert_eq!(envelope["sourceUuid"], "11111111-1111-4111-8111-111111111111");
        assert_eq!(envelope["sourceName"], "Alice");
        assert_eq!(envelope["sourceNumber"], "+15557654321");
        assert_eq!(envelope["sourceDevice"], 2);
        assert_eq!(envelope["timestamp"], 1752650000000i64);

        let dm = &envelope["dataMessage"];
        assert_eq!(dm["message"], "hello /command");
        assert_eq!(dm["timestamp"], 1752650000000i64); // number, not proto-JSON string
        assert_eq!(dm["expiresInSeconds"], 3600);
        assert_eq!(dm["quote"]["id"], 1752649990000i64);
        assert_eq!(dm["quote"]["authorUuid"], "33333333-3333-4333-8333-333333333333");
        assert_eq!(dm["quote"]["text"], "original");
        assert_eq!(dm["reaction"]["emoji"], "🔥");
        assert_eq!(dm["reaction"]["targetAuthorUuid"], "33333333-3333-4333-8333-333333333333");
        assert_eq!(dm["reaction"]["targetSentTimestamp"], 1752649990000i64);
        assert_eq!(dm["reaction"]["isRemove"], true);
        // Only mention ranges become mentions; style ranges are dropped.
        let mentions = dm["mentions"].as_array().unwrap();
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0]["uuid"], "44444444-4444-4444-8444-444444444444");
        assert_eq!(mentions[0]["start"], 0);
        assert_eq!(mentions[0]["length"], 5);
        assert_eq!(dm["payment"]["receipt"], "cmVjZWlwdA==");
        assert_eq!(dm["payment"]["note"], "thanks");
    }

    #[test]
    fn group_messages_carry_group_info_and_attachments_get_local_paths() {
        let mut event = message_event(json!({
            "body": "in a group",
            "timestamp": "1752650000000",
            "groupV2": {"masterKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "revision": 7},
            "attachments": [
                {"cdnId": "12345", "contentType": "image/jpeg", "fileName": "cat.jpg", "size": 999},
                {"cdnKey": "abcdef", "contentType": "application/pdf"},
            ],
        }));
        event["downloads"] = json!([{"index": 1, "path": "/tmp/att/x.pdf"}]);

        let notification = receive_notification(
            "+15551234567",
            &event,
            None,
            None,
            Some("R0lER0lER0lER0lER0lER0lER0lER0lER0lER0lE".into()),
        )
        .unwrap();
        let dm = &notification["params"]["envelope"]["dataMessage"];
        assert_eq!(
            dm["groupInfo"],
            json!({"groupId": "R0lER0lER0lER0lER0lER0lER0lER0lER0lER0lE", "type": "DELIVER"})
        );
        // forest falls back to uuid when sourceName is missing; we emit null.
        assert_eq!(notification["params"]["envelope"]["sourceName"], Value::Null);

        let attachments = dm["attachments"].as_array().unwrap();
        assert_eq!(attachments[0]["id"], "12345");
        assert_eq!(attachments[0]["filename"], "cat.jpg");
        assert_eq!(attachments[0]["size"], 999);
        assert!(attachments[0].get("file").is_none());
        assert_eq!(attachments[1]["id"], "abcdef");
        assert_eq!(attachments[1]["file"], "/tmp/att/x.pdf");
    }

    #[test]
    fn typing_message_maps_action() {
        let event = json!({
            "type": "message",
            "envelope": {"sender": "11111111-1111-4111-8111-111111111111", "senderDevice": 1,
                         "timestamp": 5i64, "serverTimestamp": 6i64},
            "contentType": "typingMessage",
            "content": {"timestamp": "5", "action": "STARTED"},
        });
        let notification =
            receive_notification("+1555", &event, None, None, None).unwrap();
        let envelope = &notification["params"]["envelope"];
        assert_eq!(envelope["typingMessage"]["action"], "STARTED");
        assert_eq!(envelope["typingMessage"]["timestamp"], 5);
        assert!(envelope.get("dataMessage").is_none());
    }

    #[test]
    fn update_account_params_parse() {
        // forest: signal_rpc_request("updateAccount", username=username)
        let set: UpdateAccountParams =
            serde_json::from_value(json!({"username": "ada"})).unwrap();
        assert_eq!(set.username.as_deref(), Some("ada"));
        assert!(!set.delete_username);

        let del: UpdateAccountParams =
            serde_json::from_value(json!({"delete-username": true})).unwrap();
        assert!(del.username.is_none());
        assert!(del.delete_username);
    }

    #[test]
    fn group_id_derivation_matches_signal_cli_shape() {
        // signal-cli: GroupSecretParams.deriveFromMasterKey(mk)
        //   .getPublicParams().getGroupIdentifier() -> 32 bytes, base64.
        let master_key = [7u8; 32];
        let id = group_id_b64(&master_key);
        let decoded = BASE64_STANDARD.decode(&id).unwrap();
        assert_eq!(decoded.len(), 32);
        // Deterministic and distinct from the master key itself.
        assert_eq!(id, group_id_b64(&master_key));
        assert_ne!(decoded, master_key.to_vec());
    }

    #[test]
    fn non_message_events_are_suppressed() {
        assert!(receive_notification("+1555", &json!({"type": "queueEmpty"}), None, None, None)
            .is_none());
        assert!(receive_notification(
            "+1555",
            &json!({"type": "reconnecting", "attempt": 1, "delayMs": 1000}),
            None,
            None,
            None
        )
        .is_none());
    }

    #[test]
    fn send_params_accept_forest_key_spellings() {
        // Exact param dicts forest builds (core.py:467-488, 515-547).
        let send: SendParams = serde_json::from_value(json!({
            "message": "hi",
            "end_session": true,
            "group-id": "R0lE",
            "attachments": ["/tmp/a.png"],
            "sticker": "00ff:3",
        }))
        .unwrap();
        assert_eq!(send.message.as_deref(), Some("hi"));
        assert!(send.end_session);
        assert_eq!(send.group_id.as_deref(), Some("R0lE"));
        assert_eq!(send.sticker.as_deref(), Some("00ff:3"));

        let camel: SendParams = serde_json::from_value(json!({
            "message": "hi", "endSession": true, "groupId": "R0lE",
        }))
        .unwrap();
        assert!(camel.end_session);
        assert_eq!(camel.group_id.as_deref(), Some("R0lE"));

        let reaction: SendReactionParams = serde_json::from_value(json!({
            "emoji": "🔥",
            "target-author": "33333333-3333-4333-8333-333333333333",
            "target-timestamp": 1752649990000i64,
            "group": "R0lE",
            "recipient": null,
        }))
        .unwrap();
        assert_eq!(reaction.target_timestamp, 1752649990000);
        assert!(reaction.recipient.is_none());

        // forest sends single-element lists for sendTyping.
        let typing: SendTypingParams = serde_json::from_value(json!({
            "recipient": ["33333333-3333-4333-8333-333333333333"], "stop": true,
        }))
        .unwrap();
        assert!(typing.stop);
        assert_eq!(
            typing.recipient.and_then(StringOrList::first).as_deref(),
            Some("33333333-3333-4333-8333-333333333333")
        );
    }
}

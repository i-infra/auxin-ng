//! JSONL event emission: every observable event is one JSON object per line
//! on stdout, so the process can be driven by `jq`, a pipe, or any language.

use presage::libsignal_service::content::Content;
use serde::Serialize;
use serde_json::Value;

use crate::proto_json;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonEnvelope {
    pub sender: String,
    pub sender_device: u32,
    pub destination: String,
    pub timestamp: i64,
    pub server_timestamp: i64,
    pub unidentified_sender: bool,
    pub needs_receipt: bool,
    pub was_plaintext: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_guid: Option<String>,
}

impl From<&Content> for JsonEnvelope {
    fn from(content: &Content) -> Self {
        let m = &content.metadata;
        JsonEnvelope {
            sender: m.sender.service_id_string(),
            sender_device: m.sender_device.into(),
            destination: m.destination.service_id_string(),
            timestamp: m.timestamp.timestamp_millis(),
            server_timestamp: m.server_timestamp.timestamp_millis(),
            unidentified_sender: m.unidentified_sender,
            needs_receipt: m.needs_receipt,
            was_plaintext: m.was_plaintext,
            server_guid: m.server_guid.map(|u| u.to_string()),
        }
    }
}

/// A locally downloaded attachment, keyed by its index in
/// `DataMessage.attachments`.
#[derive(Serialize, Clone)]
pub struct DownloadedAttachment {
    pub index: usize,
    pub path: String,
}

/// A single JSONL event. `content` is the canonical proto-JSON of the
/// message body; `content_type` names its protobuf type in camelCase.
#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", tag = "type")]
pub enum Event {
    #[serde(rename = "message")]
    Message {
        envelope: JsonEnvelope,
        content_type: &'static str,
        content: Value,
        /// Local paths of pre-downloaded attachments (only populated when
        /// downloads happen before emission, i.e. signal-cli compat mode).
        #[serde(skip_serializing_if = "Vec::is_empty")]
        downloads: Vec<DownloadedAttachment>,
    },
    #[serde(rename = "queueEmpty")]
    QueueEmpty,
    #[serde(rename = "contactsSynced")]
    ContactsSynced,
    #[serde(rename = "attachmentDownloaded")]
    AttachmentDownloaded {
        sender: String,
        timestamp: i64,
        path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_name: Option<String>,
    },
    #[serde(rename = "error")]
    Error { message: String },
    /// A message from this sender failed to decrypt; a session reset with
    /// them may be needed.
    #[serde(rename = "decryptionError")]
    DecryptionError { sender: String },
    /// The server dropped the websocket; a new connection is being made.
    #[serde(rename = "reconnecting")]
    Reconnecting { attempt: u32, delay_ms: u64 },
    /// Auto-accept greeted a new sender (delivery receipt + profile key).
    #[serde(rename = "accepted")]
    Accepted { sender: String },
}

impl Event {
    pub fn message(content: &Content, downloads: Vec<DownloadedAttachment>) -> Event {
        match proto_json::body_to_json(&content.body) {
            Ok((content_type, json)) => Event::Message {
                envelope: content.into(),
                content_type,
                content: json,
                downloads,
            },
            Err(error) => Event::Error {
                message: format!("failed to serialize message body: {error:#}"),
            },
        }
    }

    /// Print as one line of JSON on stdout.
    pub fn emit(&self) {
        match serde_json::to_string(self) {
            Ok(line) => println!("{line}"),
            Err(error) => eprintln!("{{\"type\":\"error\",\"message\":\"emit failed: {error}\"}}"),
        }
    }

    /// The event as a JSON value (used by the JSON-RPC daemon notifications).
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(
            |error| serde_json::json!({"type": "error", "message": error.to_string()}),
        )
    }
}

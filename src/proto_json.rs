//! Lossless protobuf <-> JSON passthrough for Signal `Content` bodies.
//!
//! Presage's proto types are prost-generated and carry no serde support, so we
//! round-trip them through `prost_reflect::DynamicMessage` using a descriptor
//! set compiled from the vendored `.proto` files (see `build.rs`). This yields
//! canonical proto-JSON in both directions: received messages are emitted as
//! JSON, and `send --content` accepts the same JSON back.

use anyhow::{anyhow, bail, Context, Result};
use presage::libsignal_service::content::ContentBody;
use presage::proto;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use serde_json::Value;
use std::sync::OnceLock;

static DESCRIPTOR_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/signal_descriptors.bin"));

pub fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(DESCRIPTOR_BYTES).expect("bundled descriptor set is valid")
    })
}

fn descriptor(type_name: &str) -> Result<MessageDescriptor> {
    pool()
        .get_message_by_name(type_name)
        .ok_or_else(|| anyhow!("unknown protobuf type: {type_name}"))
}

/// Serialize an encoded protobuf message to canonical proto-JSON.
pub fn proto_bytes_to_json(type_name: &str, bytes: &[u8]) -> Result<Value> {
    let message = DynamicMessage::decode(descriptor(type_name)?, bytes)
        .with_context(|| format!("decoding {type_name}"))?;
    let mut buf = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut buf);
    message
        .serialize_with_options(
            &mut serializer,
            &SerializeOptions::new().skip_default_fields(true),
        )
        .with_context(|| format!("serializing {type_name} to JSON"))?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Parse canonical proto-JSON into encoded protobuf bytes.
pub fn json_to_proto_bytes(type_name: &str, json: &Value) -> Result<Vec<u8>> {
    let mut deserializer = serde_json::Deserializer::from_str(&json.to_string());
    let message = DynamicMessage::deserialize(descriptor(type_name)?, &mut deserializer)
        .with_context(|| format!("parsing JSON as {type_name}"))?;
    deserializer.end()?;
    Ok(message.encode_to_vec())
}

/// The fully-qualified proto type name and a short camelCase tag for a body.
pub fn body_type(body: &ContentBody) -> (&'static str, &'static str) {
    match body {
        ContentBody::NullMessage(_) => ("signalservice.NullMessage", "nullMessage"),
        ContentBody::DataMessage(_) => ("signalservice.DataMessage", "dataMessage"),
        ContentBody::SynchronizeMessage(_) => ("signalservice.SyncMessage", "syncMessage"),
        ContentBody::CallMessage(_) => ("signalservice.CallMessage", "callMessage"),
        ContentBody::ReceiptMessage(_) => ("signalservice.ReceiptMessage", "receiptMessage"),
        ContentBody::TypingMessage(_) => ("signalservice.TypingMessage", "typingMessage"),
        ContentBody::DecryptionErrorMessage(_) => (
            "signalservice.DecryptionErrorMessage",
            "decryptionErrorMessage",
        ),
        ContentBody::StoryMessage(_) => ("signalservice.StoryMessage", "storyMessage"),
        ContentBody::PniSignatureMessage(_) => {
            ("signalservice.PniSignatureMessage", "pniSignatureMessage")
        }
        ContentBody::EditMessage(_) => ("signalservice.EditMessage", "editMessage"),
    }
}

/// Serialize any received content body to canonical proto-JSON.
pub fn body_to_json(body: &ContentBody) -> Result<(&'static str, Value)> {
    let (type_name, tag) = body_type(body);
    let bytes = match body {
        ContentBody::NullMessage(m) => m.encode_to_vec(),
        ContentBody::DataMessage(m) => m.encode_to_vec(),
        ContentBody::SynchronizeMessage(m) => m.encode_to_vec(),
        ContentBody::CallMessage(m) => m.encode_to_vec(),
        ContentBody::ReceiptMessage(m) => m.encode_to_vec(),
        ContentBody::TypingMessage(m) => m.encode_to_vec(),
        ContentBody::DecryptionErrorMessage(m) => m.encode_to_vec(),
        ContentBody::StoryMessage(m) => m.encode_to_vec(),
        ContentBody::PniSignatureMessage(m) => m.encode_to_vec(),
        ContentBody::EditMessage(m) => m.encode_to_vec(),
    };
    Ok((tag, proto_bytes_to_json(type_name, &bytes)?))
}

/// Build a sendable `ContentBody` from raw proto-JSON.
///
/// `type_name` accepts the fully-qualified name with or without the
/// `signalservice.` package prefix.
pub fn json_to_body(type_name: &str, json: &Value) -> Result<ContentBody> {
    let qualified = if type_name.contains('.') {
        type_name.to_string()
    } else {
        format!("signalservice.{type_name}")
    };
    let bytes = json_to_proto_bytes(&qualified, json)?;
    let body = match qualified.as_str() {
        "signalservice.DataMessage" => {
            ContentBody::DataMessage(proto::DataMessage::decode(&bytes[..])?)
        }
        "signalservice.SyncMessage" => {
            ContentBody::SynchronizeMessage(proto::SyncMessage::decode(&bytes[..])?)
        }
        "signalservice.CallMessage" => {
            ContentBody::CallMessage(proto::CallMessage::decode(&bytes[..])?)
        }
        "signalservice.ReceiptMessage" => {
            ContentBody::ReceiptMessage(proto::ReceiptMessage::decode(&bytes[..])?)
        }
        "signalservice.TypingMessage" => {
            ContentBody::TypingMessage(proto::TypingMessage::decode(&bytes[..])?)
        }
        "signalservice.NullMessage" => {
            ContentBody::NullMessage(proto::NullMessage::decode(&bytes[..])?)
        }
        "signalservice.StoryMessage" => {
            ContentBody::StoryMessage(proto::StoryMessage::decode(&bytes[..])?)
        }
        "signalservice.EditMessage" => {
            ContentBody::EditMessage(proto::EditMessage::decode(&bytes[..])?)
        }
        other => bail!("cannot send content of type {other}"),
    };
    Ok(body)
}

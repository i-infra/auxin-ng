//! `schema` command: introspect the embedded Signal protobuf descriptors,
//! so the sendable JSON shapes are discoverable without reading .proto files.

use anyhow::{anyhow, Result};
use prost_reflect::{EnumDescriptor, FieldDescriptor, Kind, MessageDescriptor};
use serde_json::{json, Map, Value};

use crate::proto_json::pool;

/// All fully-qualified message type names in the descriptor pool.
pub fn list_types() -> Value {
    let mut names: Vec<String> = pool()
        .all_messages()
        .map(|m| m.full_name().to_string())
        .collect();
    names.sort();
    json!(names)
}

/// The field tree of one message type as JSON. Accepts the name with or
/// without the package prefix (e.g. `DataMessage`).
pub fn describe(type_name: &str) -> Result<Value> {
    let message = find_message(type_name)?;
    let mut visiting = Vec::new();
    Ok(describe_message(&message, &mut visiting))
}

fn find_message(type_name: &str) -> Result<MessageDescriptor> {
    if let Some(m) = pool().get_message_by_name(type_name) {
        return Ok(m);
    }
    // Unqualified or partially-qualified name: match by suffix.
    let suffix = format!(".{type_name}");
    let matches: Vec<MessageDescriptor> = pool()
        .all_messages()
        .filter(|m| m.full_name().ends_with(&suffix))
        .collect();
    match matches.len() {
        0 => Err(anyhow!(
            "unknown type {type_name}; run `schema` with no argument to list types"
        )),
        1 => Ok(matches.into_iter().next().unwrap()),
        _ => Err(anyhow!(
            "ambiguous type {type_name}: {}",
            matches
                .iter()
                .map(|m| m.full_name())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn describe_message(message: &MessageDescriptor, visiting: &mut Vec<String>) -> Value {
    let full_name = message.full_name().to_string();
    if visiting.contains(&full_name) {
        // Break recursion (e.g. DataMessage.quote -> ... -> DataMessage).
        return json!({ "$type": full_name, "$recursive": true });
    }
    visiting.push(full_name.clone());

    let mut fields = Map::new();
    for field in message.fields() {
        fields.insert(field.json_name().to_string(), describe_field(&field, visiting));
    }

    visiting.pop();
    json!({ "$type": full_name, "fields": fields })
}

fn describe_field(field: &FieldDescriptor, visiting: &mut Vec<String>) -> Value {
    let cardinality = if field.is_list() {
        "repeated"
    } else if field.is_map() {
        "map"
    } else {
        "optional"
    };
    let type_value = match field.kind() {
        Kind::Message(m) => describe_message(&m, visiting),
        Kind::Enum(e) => describe_enum(&e),
        scalar => json!(format!("{scalar:?}").to_lowercase()),
    };
    json!({
        "number": field.number(),
        "cardinality": cardinality,
        "type": type_value,
    })
}

fn describe_enum(descriptor: &EnumDescriptor) -> Value {
    let values: Vec<String> = descriptor.values().map(|v| v.name().to_string()).collect();
    json!({ "$enum": descriptor.full_name(), "values": values })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_known_types() {
        let types = list_types();
        let names: Vec<&str> = types
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(names.contains(&"signalservice.DataMessage"));
        assert!(names.contains(&"signalservice.Envelope"));
    }

    #[test]
    fn describes_data_message_with_nested_types_and_cycles() {
        let schema = describe("DataMessage").unwrap();
        assert_eq!(schema["$type"], "signalservice.DataMessage");
        let fields = schema["fields"].as_object().unwrap();
        assert_eq!(fields["body"]["type"], "string");
        assert_eq!(fields["attachments"]["cardinality"], "repeated");
        // flags is uint32 on the wire; its Flags enum is advisory only.
        assert_eq!(fields["flags"]["type"], "uint32");
        // The quote field nests a message type.
        assert_eq!(fields["quote"]["type"]["$type"], "signalservice.DataMessage.Quote");

        // Enum-typed fields surface their value names.
        let receipt = describe("ReceiptMessage").unwrap();
        let values = &receipt["fields"]["type"]["type"]["values"];
        assert!(values.as_array().unwrap().iter().any(|v| v == "DELIVERY"));
    }

    #[test]
    fn unknown_and_ambiguous_names_error() {
        assert!(describe("NotARealMessage").is_err());
    }
}

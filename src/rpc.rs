//! JSON-RPC 2.0 daemon over stdin/stdout.
//!
//! Requests are read one per line from stdin; responses and incoming-message
//! notifications are written one per line to stdout. Notifications use the
//! method names `message`, `queueEmpty`, `contactsSynced`,
//! `attachmentDownloaded`, and `error`, with the JSONL event as params.

use std::path::PathBuf;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use crate::app::{self, App, ReceiveOptions, Recipient};

#[derive(Deserialize)]
struct Request {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    method: String,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    id: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendParams {
    /// Service id string (ACI uuid or `PNI:uuid`) — exclusive with `group`.
    recipient: Option<String>,
    /// Hex-encoded group master key.
    group: Option<String>,
    message: Option<String>,
    /// Raw proto-JSON content, exclusive with `message`.
    content: Option<Value>,
    #[serde(default = "default_content_type")]
    content_type: String,
    #[serde(default)]
    attachments: Vec<PathBuf>,
}

fn default_content_type() -> String {
    "DataMessage".into()
}

fn response(id: Value, result: Result<Value>) -> Value {
    match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": format!("{error:#}")},
        }),
    }
}

fn method_not_found(id: Value, method: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": -32601, "message": format!("method not found: {method}")},
    })
}

async fn handle_send(manager: &mut App, params: Value) -> Result<Value> {
    let params: SendParams = serde_json::from_value(params)?;
    let recipient = match (&params.recipient, &params.group) {
        (Some(id), None) => Recipient::Contact(app::parse_service_id(id)?),
        (None, Some(key)) => Recipient::Group(app::parse_group_master_key(key)?),
        _ => anyhow::bail!("pass exactly one of `recipient` or `group`"),
    };
    let body = app::build_body(
        manager,
        &recipient,
        params.message,
        params.content.as_ref(),
        &params.content_type,
        params.attachments,
    )
    .await?;
    app::send(manager, recipient, body).await
}

pub async fn run(manager: App, attachments_dir: Option<PathBuf>) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Single writer task keeps responses and notifications line-atomic.
    let writer = tokio::task::spawn_local(async move {
        while let Some(line) = rx.recv().await {
            println!("{line}");
        }
    });

    // Incoming messages stream as notifications from a cloned manager.
    let notify_tx = tx.clone();
    let mut receive_manager = manager.clone();
    tokio::task::spawn_local(async move {
        let result = app::receive(
            &mut receive_manager,
            ReceiveOptions {
                stop_after_empty_queue: false,
                attachments_dir,
            },
            |event| {
                let value = event.to_value();
                let method = value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("event")
                    .to_string();
                let notification =
                    json!({"jsonrpc": "2.0", "method": method, "params": value});
                let _ = notify_tx.send(notification.to_string());
            },
        )
        .await;
        if let Err(error) = result {
            let _ = notify_tx.send(
                json!({
                    "jsonrpc": "2.0",
                    "method": "error",
                    "params": {"message": format!("receive loop terminated: {error:#}")},
                })
                .to_string(),
            );
        }
    });

    let mut manager = manager;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let request: Request = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                let _ = tx.send(
                    json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {"code": -32700, "message": format!("parse error: {error}")},
                    })
                    .to_string(),
                );
                continue;
            }
        };

        let reply = match request.method.as_str() {
            "send" => response(request.id, handle_send(&mut manager, request.params).await),
            "whoami" => response(request.id, app::whoami(&manager).await),
            "listContacts" => response(request.id, app::list_contacts(&manager).await),
            "listGroups" => response(request.id, app::list_groups(&manager).await),
            "version" => response(
                request.id,
                Ok(json!({"name": "auxin-ng", "version": env!("CARGO_PKG_VERSION")})),
            ),
            method => method_not_found(request.id, method),
        };
        let _ = tx.send(reply.to_string());
    }

    drop(tx);
    let _ = writer.await;
    Ok(())
}

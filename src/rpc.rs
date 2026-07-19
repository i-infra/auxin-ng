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
use crate::compat;

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

pub async fn run(
    manager: App,
    attachments_dir: Option<PathBuf>,
    auto_accept: bool,
    signal_cli_compat: bool,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Single writer task keeps responses and notifications line-atomic.
    let writer = tokio::task::spawn_local(async move {
        while let Some(line) = rx.recv().await {
            println!("{line}");
        }
    });

    // In compat mode, message events are reshaped into signal-cli `receive`
    // notifications by a separate task (it needs async store lookups).
    let event_tx = if signal_cli_compat {
        let account = manager.registration_data().phone_number.to_string();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<Value>();
        tokio::task::spawn_local(compat::notify_task(
            manager.clone(),
            account,
            event_rx,
            tx.clone(),
        ));
        Some(event_tx)
    } else {
        None
    };

    // Incoming messages stream as notifications from a cloned manager.
    let notify_tx = tx.clone();
    let mut receive_manager = manager.clone();
    let receive_task = tokio::task::spawn_local(async move {
        let result = app::receive(
            &mut receive_manager,
            ReceiveOptions {
                stop_after_empty_queue: false,
                attachments_dir,
                auto_accept,
                download_before_emit: signal_cli_compat,
            },
            |event| {
                let value = event.to_value();
                if let Some(event_tx) = &event_tx {
                    let _ = event_tx.send(value);
                    return;
                }
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
            // signal-cli dialect: `send` params differ from the native shape
            // (base64 group ids, sticker/end_session params), so it shadows
            // the native method wholesale.
            "send" if signal_cli_compat => response(
                request.id,
                compat::handle_send(&mut manager, request.params).await,
            ),
            "sendReaction" if signal_cli_compat => response(
                request.id,
                compat::handle_send_reaction(&mut manager, request.params).await,
            ),
            "sendTyping" if signal_cli_compat => response(
                request.id,
                compat::handle_send_typing(&mut manager, request.params).await,
            ),
            "sendPaymentNotification" if signal_cli_compat => response(
                request.id,
                compat::handle_send_payment_notification(&mut manager, request.params).await,
            ),
            "updateProfile" if signal_cli_compat => response(
                request.id,
                compat::handle_update_profile(&mut manager, request.params).await,
            ),
            "updateAccount" if signal_cli_compat => response(
                request.id,
                compat::handle_update_account(&mut manager, request.params).await,
            ),
            "listContacts" if signal_cli_compat => response(
                request.id,
                compat::handle_list_contacts(&mut manager, request.params).await,
            ),
            "send" => response(request.id, handle_send(&mut manager, request.params).await),
            "whoami" => response(request.id, app::whoami(&manager).await),
            "getProfile" => {
                let recipient = request
                    .params
                    .get("recipient")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                response(
                    request.id,
                    app::get_profile(&mut manager, recipient.as_deref()).await,
                )
            }
            "listContacts" => response(request.id, app::list_contacts(&manager).await),
            "listGroups" => response(request.id, app::list_groups(&manager).await),
            "lookupUsername" => {
                let result = match request.params.get("username").and_then(Value::as_str) {
                    Some(username) => manager
                        .lookup_username(username)
                        .await
                        .map(|aci| {
                            json!({"uuid": aci.map(|aci| aci.service_id_string())})
                        })
                        .map_err(Into::into),
                    None => Err(anyhow::anyhow!("missing `username` param")),
                };
                response(request.id, result)
            }
            "version" => response(
                request.id,
                Ok(json!({"name": "auxin-ng", "version": env!("CARGO_PKG_VERSION")})),
            ),
            method => method_not_found(request.id, method),
        };
        let _ = tx.send(reply.to_string());
    }

    // stdin is closed: stop the receive loop (it holds a writer clone, so
    // the writer would otherwise never drain) and flush pending output.
    receive_task.abort();
    drop(tx);
    let _ = writer.await;
    Ok(())
}

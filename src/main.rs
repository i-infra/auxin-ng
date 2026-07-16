//! auxin-ng: a scriptable, protocol-transparent Signal CLI built on presage.
//!
//! Spiritual successor to auxin (mobilecoinofficial/auxin): JSONL output for
//! every event, raw protobuf-JSON passthrough in both directions, and a
//! JSON-RPC 2.0 daemon mode — on top of a maintained Signal stack.

mod app;
mod emit;
mod proto_json;
mod rpc;

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use futures::{channel::oneshot, future};
use presage::libsignal_service::configuration::SignalServers;
use presage::libsignal_service::prelude::phonenumber::PhoneNumber;
use presage::manager::{Manager, RegistrationOptions};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use url::Url;

use crate::app::Recipient;
use crate::emit::Event;

#[derive(Parser)]
#[clap(name = "auxin-ng", version, about = "Scriptable, protocol-transparent Signal CLI")]
struct Args {
    /// Path to the sqlite state database.
    #[clap(long, global = true)]
    db_path: Option<PathBuf>,

    /// Passphrase to encrypt the local state database.
    #[clap(long, short = 'p', global = true)]
    passphrase: Option<String>,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Link this instance as a secondary device (prints a QR code).
    LinkDevice {
        #[clap(long, short = 's', default_value = "production")]
        servers: SignalServers,
        #[clap(long, short = 'n', default_value = "auxin-ng")]
        device_name: String,
    },
    /// Register a phone number as a primary device.
    Register {
        #[clap(long, short = 's', default_value = "production")]
        servers: SignalServers,
        /// Phone number in E.164 format.
        #[clap(long)]
        phone_number: PhoneNumber,
        /// Captcha from https://signalcaptchas.org/registration/generate.html
        #[clap(long)]
        captcha: Url,
        #[clap(long)]
        use_voice_call: bool,
        /// Register again even if already registered.
        #[clap(long)]
        force: bool,
    },
    /// Print account identifiers as JSON.
    Whoami,
    /// Send a message; prints the sent content as proto-JSON.
    Send {
        /// Recipient service id (ACI uuid, or PNI:uuid).
        #[clap(long, conflicts_with = "group")]
        to: Option<String>,
        /// Group master key (hex).
        #[clap(long)]
        group: Option<String>,
        /// Plain text message body.
        #[clap(long, short = 'm', conflicts_with = "content")]
        message: Option<String>,
        /// Raw proto-JSON content (default type DataMessage).
        #[clap(long)]
        content: Option<String>,
        /// Protobuf type of --content, e.g. DataMessage or TypingMessage.
        #[clap(long, default_value = "DataMessage")]
        content_type: String,
        /// File to attach; may be repeated.
        #[clap(long = "attach")]
        attachments: Vec<PathBuf>,
        /// Build and print the message JSON without sending it.
        #[clap(long)]
        simulate: bool,
    },
    /// Stream incoming messages as JSONL on stdout.
    Receive {
        /// Exit once the server-side queue has been drained.
        #[clap(long, short = 'q')]
        stop_after_empty_queue: bool,
        /// Directory to download attachments into (downloads off when unset).
        #[clap(long)]
        attachments_dir: Option<PathBuf>,
    },
    /// JSON-RPC 2.0 daemon on stdin/stdout, streaming incoming messages
    /// as notifications.
    Jsonrpc {
        /// Directory to download attachments into (downloads off when unset).
        #[clap(long)]
        attachments_dir: Option<PathBuf>,
    },
    /// List known contacts as JSON.
    ListContacts,
    /// List known groups as JSON.
    ListGroups,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::metadata::LevelFilter::WARN.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();

    let args = Args::parse();
    let db_path = args.db_path.clone().unwrap_or_else(app::default_db_path);
    let store = app::open_store(&db_path, args.passphrase.as_deref()).await?;

    let local = tokio::task::LocalSet::new();
    local.run_until(run(args.command, store)).await
}

fn print_json(value: &Value) {
    println!("{value}");
}

async fn run(command: Command, store: presage_store_sqlite::SqliteStore) -> Result<()> {
    match command {
        Command::LinkDevice {
            servers,
            device_name,
        } => {
            let (link_tx, link_rx) = oneshot::channel();
            let (manager, _) = future::join(
                Manager::link_secondary_device(store, servers, device_name, link_tx),
                async move {
                    if let Ok(url) = link_rx.await {
                        eprintln!("Scan this QR code with Signal on your phone:");
                        let _ = qr2term::print_qr(url.to_string());
                        eprintln!("Or use the URL: {url}");
                    }
                },
            )
            .await;
            let manager = manager.context("linking failed")?;
            print_json(&app::whoami(&manager).await?);
        }
        Command::Register {
            servers,
            phone_number,
            captcha,
            use_voice_call,
            force,
        } => {
            let manager = Manager::register(
                store,
                RegistrationOptions {
                    signal_servers: servers,
                    phone_number,
                    use_voice_call,
                    captcha: captcha.host_str(),
                    force,
                },
            )
            .await?;
            eprintln!("Enter the verification code you received (then RETURN):");
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            let Some(code) = lines.next_line().await? else {
                bail!("no verification code provided");
            };
            let manager = manager.confirm_verification_code(code.trim()).await?;
            print_json(&serde_json::json!({
                "registered": manager.registration_data().service_ids.to_string(),
            }));
        }
        Command::Whoami => {
            let manager = Manager::load_registered(store).await?;
            print_json(&app::whoami(&manager).await?);
        }
        Command::Send {
            to,
            group,
            message,
            content,
            content_type,
            attachments,
            simulate,
        } => {
            let mut manager = Manager::load_registered(store).await?;
            let recipient = match (&to, &group) {
                (Some(id), None) => Recipient::Contact(app::parse_service_id(id)?),
                (None, Some(key)) => Recipient::Group(app::parse_group_master_key(key)?),
                _ => bail!("pass exactly one of --to or --group"),
            };
            let content: Option<Value> = content
                .map(|raw| serde_json::from_str(&raw).context("--content is not valid JSON"))
                .transpose()?;
            let body = app::build_body(
                &manager,
                &recipient,
                message,
                content.as_ref(),
                &content_type,
                attachments,
            )
            .await?;
            if simulate {
                let (tag, json) = proto_json::body_to_json(&body)?;
                print_json(&serde_json::json!({
                    "simulated": true,
                    "contentType": tag,
                    "content": json,
                }));
                return Ok(());
            }
            let result = app::send(&mut manager, recipient, body).await?;
            print_json(&result);
        }
        Command::Receive {
            stop_after_empty_queue,
            attachments_dir,
        } => {
            let mut manager = Manager::load_registered(store).await?;
            app::receive(
                &mut manager,
                app::ReceiveOptions {
                    stop_after_empty_queue,
                    attachments_dir,
                },
                |event| event.emit(),
            )
            .await?;
        }
        Command::Jsonrpc { attachments_dir } => {
            let manager = Manager::load_registered(store).await?;
            rpc::run(manager, attachments_dir).await?;
        }
        Command::ListContacts => {
            let manager = Manager::load_registered(store).await?;
            print_json(&app::list_contacts(&manager).await?);
        }
        Command::ListGroups => {
            let manager = Manager::load_registered(store).await?;
            print_json(&app::list_groups(&manager).await?);
        }
    }
    Ok(())
}

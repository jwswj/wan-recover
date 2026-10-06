use anyhow::{Context, Result, ensure};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, watch};
use wan_recover::{
    config::{Config, read_secret},
    gateway::{Gateway, WanSource},
    meross::{Meross, Relay},
    server::{self, HttpState},
    store::Store,
    worker::{Status, Worker},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "wan_recover=info".into()),
        )
        .init();
    let args: Vec<_> = std::env::args().collect();
    let config = Config::load(args.get(1).map(String::as_str).unwrap_or("config.toml"))?;
    let relay = config.meross.as_ref().map(Meross::new).transpose()?;
    if args.get(2).map(String::as_str) == Some("check-relay") {
        let relay = relay.context("Meross target must be configured")?;
        println!(
            "{}",
            if relay.read().await? {
                "Verified target: relay ON"
            } else {
                "Verified target: relay OFF"
            }
        );
        return Ok(());
    }
    ensure!(
        args.len() <= 2,
        "usage: wan-recover [config.toml] [check-relay]"
    );
    let token = read_secret(&config.token_file)?;
    ensure!(
        token.len() >= 32
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "webhook token must be at least 32 URL-safe characters"
    );
    let (store, state) = Store::open(&config.data_dir)?;
    let status = Arc::new(Mutex::new(Status::default()));
    let (queue, rx) = mpsc::channel(32);
    let (stop, stopping) = watch::channel(false);
    let worker = Worker {
        relay,
        gateway: config
            .gateway
            .clone()
            .map(|g| Arc::new(Gateway(g)) as Arc<dyn WanSource>),
        policy: config.policy.clone(),
        enabled: config.enabled,
        state,
        store,
        status: status.clone(),
    };
    worker.validate_target()?;
    let app = server::router(HttpState {
        token,
        mapping: config.events,
        max_age: config.policy.max_event_age_seconds,
        queue,
        status,
        capture_webhook: !config.enabled,
        last_webhook: Arc::new(Mutex::new(None)),
    });
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(enabled = config.enabled, listen = %config.listen, "starting WAN recovery service");
    let mut task = tokio::spawn(worker.run(rx, stopping.clone()));
    let mut server = tokio::spawn(async move {
        let mut stopping = stopping;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                while !*stopping.borrow() {
                    if stopping.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
    });
    tokio::select! {
        outcome = &mut task => {
            let _ = stop.send(true);
            server.await??;
            outcome??;
        },
        outcome = &mut server => {
            let _ = stop.send(true);
            task.await??;
            outcome??;
        },
        _ = shutdown() => {
            let _ = stop.send(true);
            task.await??;
            server.await??;
        }
    }
    Ok(())
}

async fn shutdown() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
}

use crate::{
    events::{Event, Signal},
    gateway::{Health, WanSource},
    meross::Relay,
    policy::{PendingOn, Policy, State},
    store::Store,
    unix_time,
};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::{error, info, warn};

pub struct Inbound {
    pub event: Event,
    pub ack: oneshot::Sender<bool>,
}

#[derive(Default, Clone, Serialize)]
pub struct Status {
    pub enabled: bool,
    pub heartbeat: u64,
    pub last_received_at: Option<u64>,
    pub received_webhooks: u64,
    pub last_event_at: Option<u64>,
    pub down_since: Option<u64>,
    pub outage_attempted: bool,
    pub cycles_last_day: usize,
    pub pending_power_on: bool,
    pub last_action: String,
    pub gateway_state: String,
    pub gateway_last_success: Option<u64>,
}

pub type SharedStatus = Arc<Mutex<Status>>;

pub struct Worker<R: Relay> {
    pub relay: Option<R>,
    pub gateway: Option<Arc<dyn WanSource>>,
    pub policy: Policy,
    pub enabled: bool,
    pub state: State,
    pub store: Store,
    pub status: SharedStatus,
}

impl<R: Relay> Worker<R> {
    pub fn validate_target(&self) -> Result<()> {
        if let Some(pending) = &self.state.pending_on {
            let relay = self
                .relay
                .as_ref()
                .context("pending power-on requires original Meross config")?;
            ensure!(
                pending.target == relay.target(),
                "pending power-on belongs to another target; restore original configuration"
            );
        }
        Ok(())
    }

    async fn action(&self, action: &str) {
        self.status.lock().await.last_action = action.to_owned();
        info!(action, "worker action");
    }

    async fn publish(&self) {
        let mut s = self.status.lock().await;
        s.enabled = self.enabled;
        s.heartbeat = unix_time();
        s.last_event_at = self.state.latest_event.map(|(at, _)| at);
        s.down_since = self.state.down_since;
        s.outage_attempted = self.state.attempted;
        s.cycles_last_day = self.state.cycles.len();
        s.pending_power_on = self.state.pending_on.is_some();
    }

    pub async fn restore(&mut self) -> Result<()> {
        if self.state.pending_on.is_none() {
            return Ok(());
        }
        self.validate_target()?;
        let relay = self.relay.as_ref().context("missing relay")?;
        // An OFF timeout is ambiguous; ON is required even if OFF never acknowledged.
        relay.set(true).await?;
        ensure!(relay.read().await?, "relay did not confirm power-on");
        let pending = self.state.pending_on.take();
        if let Err(e) = self.store.save(&self.state) {
            self.state.pending_on = pending;
            return Err(e);
        }
        self.action("power_on_verified").await;
        Ok(())
    }

    async fn reconcile(&mut self, now: u64) -> Result<Option<Health>> {
        let Some(gateway) = &self.gateway else {
            return Ok(None);
        };
        let monitoring_gap = self
            .status
            .lock()
            .await
            .gateway_last_success
            .is_some_and(|at| now.saturating_sub(at) > 20);
        let result = gateway.observe().await;
        let health = match result {
            Ok(health) => {
                let mut status = self.status.lock().await;
                status.gateway_last_success = Some(now);
                if status.gateway_state != health.name() {
                    info!(state = health.name(), "gateway WAN state changed");
                }
                status.gateway_state = health.name().into();
                health
            }
            Err(e) => {
                let mut status = self.status.lock().await;
                if status.gateway_state != "unreachable" {
                    warn!(error = %e, "gateway health unavailable; new power cycles inhibited");
                }
                status.gateway_state = "unreachable".into();
                Health::Unknown
            }
        };
        let previous = self.state.clone();
        if monitoring_gap {
            self.state.down_since = None;
            self.state.up_since = None;
        }
        match health {
            Health::Healthy => {
                self.state.event(
                    Event {
                        signal: Signal::Up,
                        at: now,
                    },
                    now,
                );
            }
            Health::FailedOver => {
                self.state.event(
                    Event {
                        signal: Signal::Down,
                        at: now,
                    },
                    now,
                );
            }
            Health::Unknown => {
                // A monitoring gap cannot count towards outage grace or stable recovery.
                self.state.down_since = None;
                self.state.up_since = None;
            }
        }
        if self.state != previous {
            self.store.save(&self.state)?;
        }
        Ok(Some(health))
    }

    pub async fn step(&mut self, now: u64) -> Result<()> {
        if let Some(pending) = &self.state.pending_on {
            if now >= pending.not_before
                && let Err(e) = self.restore().await
            {
                error!(error = %e, "power-on failed; durable recovery remains pending");
                self.action("power_on_retry_pending").await;
            }
            self.publish().await;
            return Ok(());
        }
        self.reconcile(now).await?;
        self.state.tick(now, &self.policy);
        if !self.state.eligible(now, &self.policy) {
            self.publish().await;
            return Ok(());
        }
        if !self.enabled {
            self.state.attempted = true;
            self.store.save(&self.state)?;
            self.action("would_power_cycle").await;
            self.publish().await;
            return Ok(());
        }
        let relay = self.relay.as_ref().context("missing relay")?;
        match relay.read().await {
            Ok(true) => {}
            Ok(false) => {
                self.state.attempted = true;
                self.store.save(&self.state)?;
                self.action("skipped_already_off").await;
                self.publish().await;
                return Ok(());
            }
            Err(e) => {
                self.state.attempted = true;
                self.store.save(&self.state)?;
                warn!(error = %e, "preflight failed; skipping this outage");
                self.action("skipped_preflight_failed").await;
                self.publish().await;
                return Ok(());
            }
        }
        // Relay preflight may take several seconds: read the gateway again before OFF.
        if self.gateway.is_some() && self.reconcile(unix_time()).await? != Some(Health::FailedOver)
        {
            self.action("cancelled_gateway_recheck").await;
            self.publish().await;
            return Ok(());
        }
        self.state.attempted = true;
        let relay = self.relay.as_ref().context("missing relay")?;
        self.state.cycles.push(now);
        self.state.pending_on = Some(PendingOn {
            target: relay.target(),
            not_before: now + self.policy.off_seconds,
        });
        // This fsync MUST complete before issuing OFF.
        self.store.save(&self.state)?;
        let result = relay.set(false).await;
        if let Err(e) = result {
            warn!(error = %e, "OFF failed or timed out; attempting ON immediately");
            if let Err(e) = self.restore().await {
                error!(error = %e, "immediate power-on failed; recovery will retry");
                self.action("power_on_retry_pending").await;
            }
        } else {
            self.state.pending_on.as_mut().unwrap().not_before =
                unix_time() + self.policy.off_seconds;
            self.store.save(&self.state)?;
            self.action("power_off_acknowledged").await;
        }
        self.publish().await;
        Ok(())
    }

    pub async fn run(
        mut self,
        rx: mpsc::Receiver<Inbound>,
        stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let result = self.run_loop(rx, stop).await;
        // Storage errors and other ordinary failures must also attempt restoration.
        // SIGKILL / node loss is handled by the next process using the journal.
        if result.is_err()
            && let Err(e) = self.restore().await
        {
            error!(error = %e, "shutdown recovery failed; next start must restore power");
        }
        result
    }

    async fn run_loop(
        &mut self,
        mut rx: mpsc::Receiver<Inbound>,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
        self.validate_target()?;
        self.state.restart();
        self.store.save(&self.state)?;
        // Startup restoration overrides observation mode and off-time delays.
        if let Err(e) = self.restore().await {
            error!(error = %e, "startup power-on failed; retrying");
        }
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.publish().await;
        loop {
            tokio::select! {
                biased;
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        self.restore().await?;
                        return Ok(());
                    }
                }
                Some(input) = rx.recv() => {
                    // With reconciliation configured, only the live gateway may change policy state.
                    let changed = self.gateway.is_none() && self.state.event(input.event, unix_time());
                    if changed { self.store.save(&self.state)?; }
                    let _ = input.ack.send(changed);
                    self.publish().await;
                }
                _ = interval.tick() => self.step(unix_time()).await?,
            }
        }
    }
}

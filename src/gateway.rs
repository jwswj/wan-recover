use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{future::Future, net::IpAddr, path::PathBuf, pin::Pin, process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    pub address: IpAddr,
    pub key_file: PathBuf,
    pub known_hosts_file: PathBuf,
    pub primary_interface: String,
    pub backup_interface: String,
}

impl GatewayConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.address, IpAddr::V4(ip) if ip.is_private()),
            "gateway must have a private IPv4 address"
        );
        ensure!(
            self.primary_interface != self.backup_interface,
            "WAN interfaces must differ"
        );
        for name in [&self.primary_interface, &self.backup_interface] {
            ensure!(
                !name.is_empty()
                    && name.len() <= 32
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-'),
                "invalid interface name"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Health {
    Healthy,
    FailedOver,
    Unknown,
}

impl Health {
    pub fn name(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::FailedOver => "failed_over",
            Self::Unknown => "unknown",
        }
    }
}

pub type Observation<'a> = Pin<Box<dyn Future<Output = Result<Health>> + Send + 'a>>;
pub trait WanSource: Send + Sync {
    fn observe(&self) -> Observation<'_>;
}

pub struct Gateway(pub GatewayConfig);

impl WanSource for Gateway {
    fn observe(&self) -> Observation<'_> {
        Box::pin(async move {
            let mut child = Command::new("/usr/bin/ssh")
                .args([
                    "-F",
                    "/dev/null",
                    "-T",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "StrictHostKeyChecking=yes",
                    "-o",
                    "GlobalKnownHostsFile=/dev/null",
                    "-o",
                    "ConnectTimeout=3",
                    "-o",
                    "ConnectionAttempts=1",
                    "-o",
                    "LogLevel=ERROR",
                ])
                .arg("-o")
                .arg(format!(
                    "UserKnownHostsFile={}",
                    self.0.known_hosts_file.display()
                ))
                .arg("-i")
                .arg(&self.0.key_file)
                .arg(format!("root@{}", self.0.address))
                .arg("wan-health")
                .env_remove("SSH_AUTH_SOCK")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .context("cannot start gateway SSH reader")?;
            let mut output = Vec::new();
            tokio::time::timeout(Duration::from_secs(8), async {
                child
                    .stdout
                    .take()
                    .context("missing SSH output")?
                    .take(16385)
                    .read_to_end(&mut output)
                    .await?;
                ensure!(output.len() <= 16384, "gateway response too large");
                ensure!(child.wait().await?.success(), "gateway SSH reader failed");
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("gateway SSH reader timed out")??;
            classify(
                &output,
                &self.0.primary_interface,
                &self.0.backup_interface,
                crate::unix_time(),
            )
        })
    }
}

#[derive(Deserialize)]
struct Snapshot {
    sampled_at: u64,
    enabled: bool,
    groups: Vec<Group>,
    wans: Vec<Wan>,
    links: Vec<Link>,
}
#[derive(Deserialize)]
struct Group {
    interfaces: Vec<String>,
    metric: u64,
    active: bool,
    health: String,
}
#[derive(Deserialize)]
struct Wan {
    interface: String,
    metric: u64,
    health: String,
}
#[derive(Deserialize)]
struct Link {
    interface: String,
    enabled: bool,
    current_enabled: bool,
    plugged: bool,
    wan_status: String,
    timestamp_ms: u64,
}

fn unique<T>(values: &[T], predicate: impl Fn(&T) -> bool) -> Result<&T> {
    let mut matches = values.iter().filter(|v| predicate(v));
    let value = matches.next().context("configured WAN missing")?;
    ensure!(matches.next().is_none(), "ambiguous WAN configuration");
    Ok(value)
}

/// Only an explicit primary failure with a healthy, active backup permits recovery.
/// The two API reads can race a transition; inconsistent samples are unknown.
pub fn classify(bytes: &[u8], primary: &str, backup: &str, now: u64) -> Result<Health> {
    let s: Snapshot = serde_json::from_slice(bytes).context("invalid gateway snapshot")?;
    ensure!(
        s.sampled_at <= now.saturating_add(5) && now.saturating_sub(s.sampled_at) <= 10,
        "stale gateway snapshot or clock skew"
    );
    let p = unique(&s.wans, |w| w.interface == primary)?;
    let b = unique(&s.wans, |w| w.interface == backup)?;
    let pg = unique(&s.groups, |g| g.interfaces.iter().any(|i| i == primary))?;
    let bg = unique(&s.groups, |g| g.interfaces.iter().any(|i| i == backup))?;
    let pl = unique(&s.links, |l| l.interface == primary)?;
    let bl = unique(&s.links, |l| l.interface == backup)?;
    for l in [pl, bl] {
        let at = l.timestamp_ms / 1000;
        ensure!(
            at <= now.saturating_add(5) && now.saturating_sub(at) <= 10,
            "stale gateway interface state"
        );
    }
    if !s.enabled
        || !pl.enabled
        || !pl.current_enabled
        || !bl.enabled
        || !bl.current_enabled
        || p.metric >= b.metric
        || pg.metric != p.metric
        || bg.metric != b.metric
        || pg.interfaces.len() != 1
        || bg.interfaces.len() != 1
    {
        return Ok(Health::Unknown);
    }
    if p.health == "good"
        && pg.health == "good"
        && pg.active
        && pl.plugged
        && pl.wan_status == "active"
        && !bg.active
    {
        return Ok(Health::Healthy);
    }
    if p.health == "bad"
        && pg.health == "bad"
        && !pg.active
        && b.health == "good"
        && bg.health == "good"
        && bg.active
        && bl.plugged
        && bl.wan_status == "active"
    {
        return Ok(Health::FailedOver);
    }
    Ok(Health::Unknown)
}

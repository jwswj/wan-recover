use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub token_file: PathBuf,
    #[serde(default)]
    pub enabled: bool,
    pub events: crate::events::Mapping,
    #[serde(default)]
    pub policy: crate::policy::Policy,
    pub meross: Option<MerossConfig>,
    pub gateway: Option<crate::gateway::GatewayConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MerossConfig {
    pub address: IpAddr,
    pub uuid: String,
    pub key_file: PathBuf,
    pub channel: u8,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let config: Self =
            toml::from_str(&std::fs::read_to_string(path)?).context("invalid configuration")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.policy.validate()?;
        if let Some(gateway) = &self.gateway {
            gateway.validate()?;
        }
        ensure!(
            !self.enabled || self.gateway.is_some(),
            "enabled mode requires a live gateway health reader"
        );
        self.events.validate()?;
        ensure!(
            !self.enabled || self.meross.is_some(),
            "enabled mode requires a Meross target"
        );
        if let Some(m) = &self.meross {
            ensure!(
                m.uuid.len() == 32 && m.uuid.bytes().all(|b| b.is_ascii_hexdigit()),
                "Meross UUID must be 32 hex characters"
            );
            ensure!(
                matches!(m.address, IpAddr::V4(ip) if ip.is_private() || ip.is_loopback())
                    || matches!(m.address, IpAddr::V6(ip) if ip.is_unique_local() || ip.is_loopback()),
                "Meross must have a private LAN address"
            );
        }
        Ok(())
    }
}

pub fn read_secret(path: &std::path::Path) -> Result<String> {
    let value = std::fs::read_to_string(path).context("cannot read secret file")?;
    let value = value.trim_end_matches(['\r', '\n']).to_owned();
    ensure!(!value.is_empty(), "secret file is empty");
    Ok(value)
}

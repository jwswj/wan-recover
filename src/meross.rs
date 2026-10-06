use crate::{
    config::{MerossConfig, read_secret},
    unix_time,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{future::Future, time::Duration};
use subtle::ConstantTimeEq;

pub trait Relay: Send + Sync {
    fn target(&self) -> String;
    fn read(&self) -> impl Future<Output = Result<bool>> + Send;
    fn set(&self, on: bool) -> impl Future<Output = Result<()>> + Send;
}

pub struct Meross {
    client: reqwest::Client,
    url: String,
    key: String,
    uuid: String,
    channel: u8,
}

pub fn signature(id: &str, key: &str, timestamp: u64) -> String {
    format!("{:x}", md5::compute(format!("{id}{key}{timestamp}")))
}

impl Meross {
    pub fn new(config: &MerossConfig) -> Result<Self> {
        let address = std::net::SocketAddr::new(config.address, 80);
        Self::with_url(config, format!("http://{address}/config"))
    }

    /// Also permits a loopback mock server in protocol tests.
    pub fn with_url(config: &MerossConfig, url: String) -> Result<Self> {
        let key = read_secret(&config.key_file)?;
        ensure!(
            key.len() == 32 && key.bytes().all(|b| b.is_ascii_hexdigit()),
            "Meross device key must be 32 hex characters"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(5))
                .build()?,
            url,
            key,
            uuid: config.uuid.clone(),
            channel: config.channel,
        })
    }

    async fn request(&self, namespace: &str, method: &str, payload: Value) -> Result<Value> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let timestamp = unix_time();
        let packet = json!({"header": {
            "messageId": id, "namespace": namespace, "method": method,
            "payloadVersion": 1, "from": "http://wan-recover/config",
            "timestamp": timestamp, "timestampMs": 0,
            "sign": signature(&id, &self.key, timestamp)
        }, "payload": payload});
        let mut response = self
            .client
            .post(&self.url)
            .json(&packet)
            .send()
            .await
            .context("Meross request failed")?
            .error_for_status()
            .context("Meross HTTP error")?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len() + chunk.len() <= 65536,
                "Meross response too large"
            );
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes).context("Meross response is not JSON")?;
        let h = &body["header"];
        ensure!(
            h["messageId"].as_str() == Some(id.as_str()),
            "Meross response ID mismatch"
        );
        ensure!(
            h["namespace"].as_str() == Some(namespace),
            "Meross response namespace mismatch"
        );
        let ts = h["timestamp"]
            .as_u64()
            .context("Meross response has no timestamp")?;
        let sign = h["sign"]
            .as_str()
            .context("Meross response has no signature")?;
        ensure!(
            bool::from(
                sign.as_bytes()
                    .ct_eq(signature(&id, &self.key, ts).as_bytes())
            ),
            "Meross response signature invalid"
        );
        if h["method"] == "ERROR" {
            bail!("Meross rejected request (check device key and capability)");
        }
        ensure!(
            h["method"].as_str() == Some(format!("{method}ACK").as_str()),
            "Meross response method mismatch"
        );
        Ok(body["payload"].clone())
    }
}

impl Relay for Meross {
    fn target(&self) -> String {
        format!("{}#{}:{}", self.url, self.uuid, self.channel)
    }

    async fn read(&self) -> Result<bool> {
        let all = self
            .request("Appliance.System.All", "GET", json!({"all": {}}))
            .await?;
        ensure!(
            all.pointer("/all/system/hardware/uuid")
                .and_then(Value::as_str)
                == Some(self.uuid.as_str()),
            "Meross UUID mismatch; refusing to control this device"
        );
        let relays = all
            .pointer("/all/digest/togglex")
            .and_then(Value::as_array)
            .context("device does not expose ToggleX relay state")?;
        let relay = relays
            .iter()
            .find(|r| r["channel"].as_u64() == Some(u64::from(self.channel)))
            .context("configured relay channel is absent")?;
        match relay["onoff"].as_u64() {
            Some(0) => Ok(false),
            Some(1) => Ok(true),
            _ => bail!("invalid relay state"),
        }
    }

    async fn set(&self, on: bool) -> Result<()> {
        // Pin physical identity immediately before every command, including recovery.
        self.read().await?;
        self.request(
            "Appliance.Control.ToggleX",
            "SET",
            json!({"togglex": {"channel": self.channel, "onoff": u8::from(on)}}),
        )
        .await?;
        Ok(())
    }
}

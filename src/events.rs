use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Down,
    Up,
}

/// Exact alarm-name matching keeps changes in UniFi's payload schema fail-closed.
/// The alarms themselves must be scoped to the primary WAN in UniFi.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub alarm_pointer: String,
    pub timestamp_pointer: String,
    pub down_alarm: String,
    pub up_alarm: String,
    #[serde(default)]
    pub timestamp_milliseconds: bool,
}

impl Mapping {
    pub fn validate(&self) -> Result<()> {
        for pointer in [&self.alarm_pointer, &self.timestamp_pointer] {
            ensure!(
                pointer.starts_with('/') && pointer.len() > 1,
                "event fields must be JSON pointers"
            );
        }
        ensure!(
            !self.down_alarm.is_empty()
                && !self.up_alarm.is_empty()
                && self.down_alarm != self.up_alarm,
            "two distinct primary WAN alarm names are required"
        );
        Ok(())
    }

    pub fn parse(&self, body: &Value, now: u64, max_age: u64) -> Result<Option<Event>> {
        let signal = match body.pointer(&self.alarm_pointer).and_then(Value::as_str) {
            Some(name) if name == self.down_alarm => Signal::Down,
            Some(name) if name == self.up_alarm => Signal::Up,
            _ => return Ok(None),
        };
        let Some(mut at) = body
            .pointer(&self.timestamp_pointer)
            .and_then(Value::as_u64)
        else {
            bail!("matched alarm has no integer timestamp");
        };
        if self.timestamp_milliseconds {
            at /= 1000;
        }
        ensure!(
            at <= now.saturating_add(10),
            "event timestamp is in the future"
        );
        ensure!(now.saturating_sub(at) <= max_age, "event is too old");
        Ok(Some(Event { signal, at }))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub signal: Signal,
    pub at: u64,
}

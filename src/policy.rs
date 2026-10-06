use crate::events::{Event, Signal};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub grace_seconds: u64,
    pub off_seconds: u64,
    pub cooldown_seconds: u64,
    pub stable_seconds: u64,
    pub max_cycles_per_day: usize,
    pub max_event_age_seconds: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            grace_seconds: 90,
            off_seconds: 15,
            cooldown_seconds: 1800,
            stable_seconds: 300,
            max_cycles_per_day: 3,
            max_event_age_seconds: 120,
        }
    }
}

impl Policy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (30..=600).contains(&self.grace_seconds),
            "grace must be 30..600 seconds"
        );
        ensure!(
            (5..=60).contains(&self.off_seconds),
            "off time must be 5..60 seconds"
        );
        ensure!(
            (600..=86400).contains(&self.cooldown_seconds),
            "cooldown must be 600..86400 seconds"
        );
        ensure!(
            (60..=3600).contains(&self.stable_seconds),
            "stable time must be 60..3600 seconds"
        );
        ensure!(
            (1..=10).contains(&self.max_cycles_per_day),
            "daily cycle limit must be 1..10"
        );
        ensure!(
            (30..=300).contains(&self.max_event_age_seconds),
            "event age limit must be 30..300 seconds"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub version: u8,
    pub latest_event: Option<(u64, Signal)>,
    pub down_since: Option<u64>,
    pub up_since: Option<u64>,
    pub attempted: bool,
    pub cycles: Vec<u64>,
    pub pending_on: Option<PendingOn>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PendingOn {
    pub target: String,
    pub not_before: u64,
}

impl State {
    pub fn fresh() -> Self {
        Self {
            version: 1,
            ..Self::default()
        }
    }

    pub fn event(&mut self, event: Event, now: u64) -> bool {
        // Restore wins ties; down cannot overwrite an equally recent restore.
        if let Some((at, signal)) = self.latest_event
            && (event.at < at
                || (event.at == at && (signal == Signal::Up || event.signal == Signal::Down)))
        {
            return false;
        }
        self.latest_event = Some((event.at, event.signal));
        match event.signal {
            Signal::Down => {
                self.up_since = None;
                self.down_since.get_or_insert(now);
            }
            Signal::Up => {
                self.down_since = None;
                self.up_since.get_or_insert(now);
            }
        }
        true
    }

    pub fn tick(&mut self, now: u64, policy: &Policy) {
        self.cycles.retain(|t| now.saturating_sub(*t) < 86400);
        if self
            .up_since
            .is_some_and(|t| now.saturating_sub(t) >= policy.stable_seconds)
        {
            self.attempted = false;
        }
    }

    pub fn eligible(&self, now: u64, policy: &Policy) -> bool {
        self.pending_on.is_none()
            && !self.attempted
            && self.latest_event.is_some_and(|(at, signal)| {
                signal == Signal::Down
                    && now.saturating_sub(at) <= policy.grace_seconds + policy.max_event_age_seconds
            })
            && self
                .down_since
                .is_some_and(|t| now.saturating_sub(t) >= policy.grace_seconds)
            && self.cycles.len() < policy.max_cycles_per_day
            && self
                .cycles
                .last()
                .is_none_or(|t| now.saturating_sub(*t) >= policy.cooldown_seconds)
    }

    // On restart, historical down events cannot initiate a new power cycle.
    // Cooldown, attempts, replay watermark, and pending restoration survive.
    pub fn restart(&mut self) {
        self.down_since = None;
        self.up_since = None;
    }
}

use anyhow::{Result, bail};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use wan_recover::{
    events::{Event, Mapping, Signal},
    meross::Relay,
    policy::{PendingOn, Policy, State},
    store::Store,
    unix_time,
    worker::{Status, Worker},
};

fn event(signal: Signal, at: u64) -> Event {
    Event { signal, at }
}
fn down(at: u64) -> State {
    let mut state = State::fresh();
    state.event(event(Signal::Down, at), at);
    state
}

#[test]
fn grace_and_restore_cancellation() {
    let p = Policy::default();
    let mut s = down(1000);
    assert!(!s.eligible(1089, &p));
    assert!(s.eligible(1090, &p));
    s.event(event(Signal::Up, 1090), 1090);
    assert!(!s.eligible(1091, &p));
}

#[test]
fn duplicates_cannot_postpone_grace_or_override_restore() {
    let mut s = down(1000);
    assert!(!s.event(event(Signal::Down, 1000), 1050));
    s.event(event(Signal::Down, 1020), 1020);
    assert_eq!(s.down_since, Some(1000));
    assert!(s.event(event(Signal::Up, 1020), 1020));
    assert!(!s.event(event(Signal::Down, 1020), 1020));
    assert!(!s.event(event(Signal::Down, 1010), 1020));
}

#[test]
fn flapping_does_not_rearm_until_stable() {
    let p = Policy::default();
    let mut s = down(1000);
    s.attempted = true;
    s.event(event(Signal::Up, 1100), 1100);
    s.tick(1399, &p);
    assert!(s.attempted);
    s.event(event(Signal::Down, 1399), 1399);
    s.tick(1700, &p);
    assert!(s.attempted);
    s.event(event(Signal::Up, 1800), 1800);
    s.tick(2100, &p);
    assert!(!s.attempted);
}

#[test]
fn cooldown_and_rolling_day_limit() {
    let p = Policy::default();
    let mut s = down(10000);
    s.cycles.push(9000);
    assert!(!s.eligible(10090, &p));
    // A suppressed alarm cannot trigger much later without another fresh down event.
    assert!(!s.eligible(10800, &p));
    s.event(event(Signal::Down, 10800), 10800);
    assert!(s.eligible(10800, &p));
    s.cycles = vec![1000, 5000, 9000];
    assert!(!s.eligible(10800, &p));
    s.tick(95400, &p);
    assert!(s.cycles.is_empty());
}

#[test]
fn restart_preserves_limits_but_requires_a_fresh_event() {
    let mut s = down(1000);
    s.cycles = vec![900];
    s.pending_on = Some(PendingOn {
        target: "relay".into(),
        not_before: 1100,
    });
    s.attempted = true;
    s.restart();
    assert_eq!(s.down_since, None);
    assert_eq!(s.cycles, vec![900]);
    assert!(s.pending_on.is_some());
    assert!(s.attempted);
    assert!(!s.event(event(Signal::Down, 1000), 1100));
}

#[test]
fn clock_rollback_is_conservative() {
    let p = Policy::default();
    let mut s = down(1000);
    s.cycles.push(2000);
    s.tick(900, &p);
    assert_eq!(s.cycles, vec![2000]);
    assert!(!s.eligible(900, &p));
}

pub fn mapping() -> Mapping {
    Mapping {
        alarm_pointer: "/alarm/name".into(),
        timestamp_pointer: "/timestamp".into(),
        down_alarm: "Primary WAN offline".into(),
        up_alarm: "Primary WAN restored".into(),
        timestamp_milliseconds: false,
    }
}

#[test]
fn accepts_only_exact_primary_alarm_and_fresh_timestamp() {
    let m = mapping();
    assert!(
        m.parse(
            &serde_json::json!({"alarm":{"name":"Starlink offline"},"timestamp":1000}),
            1000,
            120
        )
        .unwrap()
        .is_none()
    );
    assert!(
        m.parse(
            &serde_json::json!({"alarm":{"name":"Test"},"timestamp":1000}),
            1000,
            120
        )
        .unwrap()
        .is_none()
    );
    let v = serde_json::json!({"alarm":{"name":"Primary WAN offline"},"timestamp":1000});
    assert_eq!(
        m.parse(&v, 1000, 120).unwrap().unwrap().signal,
        Signal::Down
    );
    assert!(m.parse(&v, 1121, 120).is_err());
    assert!(m.parse(&v, 989, 120).is_err());
    assert!(
        m.parse(
            &serde_json::json!({"alarm":{"name":"Primary WAN offline"}}),
            1000,
            120
        )
        .is_err()
    );
}

#[test]
fn explicit_millisecond_timestamps() {
    let mut m = mapping();
    m.timestamp_milliseconds = true;
    let e = m
        .parse(
            &serde_json::json!({"alarm":{"name":"Primary WAN restored"},"timestamp":1000123}),
            1000,
            120,
        )
        .unwrap()
        .unwrap();
    assert_eq!(e.at, 1000);
    assert_eq!(e.signal, Signal::Up);
}

#[test]
fn state_is_durable_locked_and_corruption_fails_closed() {
    let dir = TempDir::new().unwrap();
    let (store, _) = Store::open(dir.path()).unwrap();
    assert!(Store::open(dir.path()).is_err());
    let mut state = down(1000);
    state.pending_on = Some(PendingOn {
        target: "relay".into(),
        not_before: 1100,
    });
    store.save(&state).unwrap();
    drop(store);
    let (store, restored) = Store::open(dir.path()).unwrap();
    assert_eq!(state, restored);
    drop(store);
    std::fs::write(dir.path().join("state.json"), b"partial data").unwrap();
    assert!(Store::open(dir.path()).is_err());
}

#[derive(Default)]
struct FakeState {
    on: bool,
    calls: Vec<bool>,
    fail_off: bool,
    fail_on: bool,
    fail_read: bool,
}

#[derive(Clone)]
struct FakeRelay {
    state: Arc<Mutex<FakeState>>,
    journal: PathBuf,
}

impl Relay for FakeRelay {
    fn target(&self) -> String {
        "fixture-relay".into()
    }
    async fn read(&self) -> Result<bool> {
        let s = self.state.lock().unwrap();
        if s.fail_read {
            bail!("read failed");
        }
        Ok(s.on)
    }
    async fn set(&self, on: bool) -> Result<()> {
        // Prove an OFF can never precede a durable restoration marker and rate limit.
        if !on {
            let state: State = serde_json::from_slice(&std::fs::read(&self.journal)?).unwrap();
            assert!(state.pending_on.is_some());
            assert!(!state.cycles.is_empty());
            assert!(state.attempted);
        }
        let mut s = self.state.lock().unwrap();
        s.calls.push(on);
        if on && s.fail_on {
            bail!("ON unreachable");
        }
        s.on = on;
        if !on && s.fail_off {
            bail!("OFF applied, acknowledgement lost");
        }
        Ok(())
    }
}

fn worker(enabled: bool) -> (TempDir, Worker<FakeRelay>, FakeRelay) {
    let dir = TempDir::new().unwrap();
    let (store, _) = Store::open(dir.path()).unwrap();
    let relay = FakeRelay {
        state: Arc::new(Mutex::new(FakeState {
            on: true,
            ..Default::default()
        })),
        journal: dir.path().join("state.json"),
    };
    let w = Worker {
        relay: Some(relay.clone()),
        gateway: None,
        policy: Policy::default(),
        enabled,
        state: down(unix_time() - 90),
        store,
        status: Arc::new(tokio::sync::Mutex::new(Status::default())),
    };
    (dir, w, relay)
}

#[tokio::test]
async fn full_cycle_and_one_attempt_per_outage() {
    let (_dir, mut w, relay) = worker(true);
    w.step(unix_time()).await.unwrap();
    assert_eq!(relay.state.lock().unwrap().calls, vec![false]);
    let at = w.state.pending_on.as_ref().unwrap().not_before;
    w.step(at - 1).await.unwrap();
    assert!(!relay.state.lock().unwrap().on);
    w.step(at).await.unwrap();
    assert!(relay.state.lock().unwrap().on);
    assert!(w.state.pending_on.is_none());
    w.step(at + 3600).await.unwrap();
    assert_eq!(relay.state.lock().unwrap().calls, vec![false, true]);
}

#[tokio::test]
async fn observation_mode_never_contacts_relay() {
    let (_dir, mut w, relay) = worker(false);
    relay.state.lock().unwrap().fail_read = true;
    w.step(unix_time()).await.unwrap();
    assert!(relay.state.lock().unwrap().calls.is_empty());
    assert_eq!(w.status.lock().await.last_action, "would_power_cycle");
    assert!(w.state.cycles.is_empty());
}

#[tokio::test]
async fn manually_off_relay_is_left_alone() {
    let (_dir, mut w, relay) = worker(true);
    relay.state.lock().unwrap().on = false;
    w.step(unix_time()).await.unwrap();
    assert!(relay.state.lock().unwrap().calls.is_empty());
    assert_eq!(w.status.lock().await.last_action, "skipped_already_off");
}

#[tokio::test]
async fn preflight_failure_prevents_switching() {
    let (_dir, mut w, relay) = worker(true);
    relay.state.lock().unwrap().fail_read = true;
    w.step(unix_time()).await.unwrap();
    assert!(relay.state.lock().unwrap().calls.is_empty());
    assert!(w.state.attempted);
}

#[tokio::test]
async fn lost_off_ack_still_restores_power() {
    let (_dir, mut w, relay) = worker(true);
    relay.state.lock().unwrap().fail_off = true;
    w.step(unix_time()).await.unwrap();
    assert_eq!(relay.state.lock().unwrap().calls, vec![false, true]);
    assert!(relay.state.lock().unwrap().on);
    assert!(w.state.pending_on.is_none());
}

#[tokio::test]
async fn failed_on_retries_and_never_sends_another_off() {
    let (_dir, mut w, relay) = worker(true);
    relay.state.lock().unwrap().fail_on = true;
    w.step(unix_time()).await.unwrap();
    let at = w.state.pending_on.as_ref().unwrap().not_before;
    w.step(at).await.unwrap();
    w.step(at + 5).await.unwrap();
    assert!(w.state.pending_on.is_some());
    relay.state.lock().unwrap().fail_on = false;
    w.step(at + 10).await.unwrap();
    assert!(w.state.pending_on.is_none());
    assert_eq!(
        relay.state.lock().unwrap().calls,
        vec![false, true, true, true]
    );
}

#[tokio::test]
async fn crash_then_disabled_restart_restores_original_target() {
    let (dir, mut w, relay) = worker(true);
    w.step(unix_time()).await.unwrap();
    drop(w);
    let (store, state) = Store::open(dir.path()).unwrap();
    let w = Worker {
        relay: Some(relay.clone()),
        gateway: None,
        policy: Policy::default(),
        enabled: false,
        state,
        store,
        status: Arc::new(tokio::sync::Mutex::new(Status::default())),
    };
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    let (stop, stopping) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(w.run(rx, stopping));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !relay.state.lock().unwrap().on {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    handle.await.unwrap().unwrap();
    assert_eq!(relay.state.lock().unwrap().calls, vec![false, true]);
}

#[tokio::test]
async fn changed_target_cannot_inherit_pending_on() {
    let (_dir, mut w, relay) = worker(true);
    w.state.pending_on = Some(PendingOn {
        target: "different-target".into(),
        not_before: 0,
    });
    assert!(w.restore().await.is_err());
    assert!(relay.state.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn storage_failure_before_off_prevents_actuation() {
    let (dir, mut w, relay) = worker(true);
    std::fs::create_dir(dir.path().join("state.tmp")).unwrap();
    assert!(w.step(unix_time()).await.is_err());
    assert!(relay.state.lock().unwrap().calls.is_empty());
}

struct FakeGateway(Mutex<std::collections::VecDeque<Option<wan_recover::gateway::Health>>>);
impl wan_recover::gateway::WanSource for FakeGateway {
    fn observe(&self) -> wan_recover::gateway::Observation<'_> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected gateway poll")
                .ok_or_else(|| anyhow::anyhow!("gateway unreachable"))
        })
    }
}
fn gateway(w: &mut Worker<FakeRelay>, samples: Vec<Option<wan_recover::gateway::Health>>) {
    w.gateway = Some(Arc::new(FakeGateway(Mutex::new(samples.into()))));
}

#[tokio::test]
async fn gateway_failure_or_unknown_cancels_grace_without_rearming() {
    use wan_recover::gateway::Health::*;
    for sample in [None, Some(Unknown)] {
        let (_dir, mut w, relay) = worker(true);
        gateway(&mut w, vec![sample, Some(FailedOver)]);
        w.step(unix_time()).await.unwrap();
        assert!(w.state.down_since.is_none());
        w.step(unix_time() + 5).await.unwrap();
        assert_eq!(w.state.down_since, Some(unix_time() + 5));
        assert!(relay.state.lock().unwrap().calls.is_empty());
        assert!(!w.state.attempted);
    }
}
#[tokio::test]
async fn restore_or_monitor_failure_during_preflight_prevents_off() {
    use wan_recover::gateway::Health::*;
    for sample in [Some(Healthy), Some(Unknown), None] {
        let (_dir, mut w, relay) = worker(true);
        gateway(&mut w, vec![Some(FailedOver), sample]);
        w.step(unix_time()).await.unwrap();
        assert!(relay.state.lock().unwrap().calls.is_empty());
        assert!(w.state.cycles.is_empty());
        assert!(!w.state.attempted);
        assert_eq!(
            w.status.lock().await.last_action,
            "cancelled_gateway_recheck"
        );
    }
}
#[tokio::test]
async fn sustained_failover_cycles_once_and_restores_without_gateway() {
    use wan_recover::gateway::Health::*;
    let (_dir, mut w, relay) = worker(true);
    gateway(&mut w, vec![Some(FailedOver), Some(FailedOver)]);
    w.step(unix_time()).await.unwrap();
    assert_eq!(relay.state.lock().unwrap().calls, vec![false]);
    let at = w.state.pending_on.as_ref().unwrap().not_before;
    // No more gateway samples: restoration must not depend on a gateway read.
    w.step(at).await.unwrap();
    assert_eq!(relay.state.lock().unwrap().calls, vec![false, true]);
}
#[tokio::test]
async fn monitoring_gap_cannot_count_as_stable_restoration() {
    use wan_recover::gateway::Health::*;
    let (_dir, mut w, _) = worker(true);
    w.state = State::fresh();
    w.state.attempted = true;
    let now = unix_time();
    w.state.event(event(Signal::Up, now - 299), now - 299);
    gateway(&mut w, vec![None, Some(Healthy)]);
    w.step(now).await.unwrap();
    w.step(now + 5).await.unwrap();
    assert!(w.state.attempted);
    assert_eq!(w.state.up_since, Some(now + 5));
}

#[tokio::test]
async fn suspended_worker_cannot_count_missing_polls_toward_grace() {
    use wan_recover::gateway::Health::*;
    let (_dir, mut w, relay) = worker(true);
    let now = unix_time();
    w.status.lock().await.gateway_last_success = Some(now - 90);
    gateway(&mut w, vec![Some(FailedOver)]);
    w.step(now).await.unwrap();
    assert_eq!(w.state.down_since, Some(now));
    assert!(relay.state.lock().unwrap().calls.is_empty());
}

use serde_json::{Value, json};
use wan_recover::gateway::{Health, classify};

fn healthy() -> Value {
    serde_json::from_str(include_str!("fixtures/gateway-healthy.json")).unwrap()
}
fn parse(v: &Value) -> anyhow::Result<Health> {
    classify(
        &serde_json::to_vec(v)?,
        "eth8",
        "eth9",
        v["sampled_at"].as_u64().unwrap(),
    )
}
// Failure snapshots below are synthetic mutations of the real healthy capture.
fn failover() -> Value {
    let mut v = healthy();
    v["wans"][0]["health"] = json!("bad");
    v["groups"][0]["health"] = json!("bad");
    v["groups"][0]["active"] = json!(false);
    v["groups"][1]["active"] = json!(true);
    v["links"][0]["wan_status"] = json!("failed");
    v["links"][1]["wan_status"] = json!("active");
    v
}

#[test]
fn observed_live_gateway_reports_primary_healthy() {
    assert_eq!(parse(&healthy()).unwrap(), Health::Healthy);
}
#[test]
fn requires_explicit_primary_failure_and_working_failover() {
    let v = failover();
    assert_eq!(parse(&v).unwrap(), Health::FailedOver);
    for pointer in [
        "/wans/0/health",
        "/groups/0/health",
        "/wans/1/health",
        "/groups/1/health",
    ] {
        let mut changed = v.clone();
        *changed.pointer_mut(pointer).unwrap() = json!("unknown");
        assert_eq!(parse(&changed).unwrap(), Health::Unknown);
    }
    for pointer in [
        "/enabled",
        "/links/0/enabled",
        "/links/0/current_enabled",
        "/links/1/enabled",
        "/links/1/plugged",
        "/groups/1/active",
    ] {
        let mut changed = v.clone();
        *changed.pointer_mut(pointer).unwrap() = json!(false);
        assert_eq!(parse(&changed).unwrap(), Health::Unknown);
    }
}
#[test]
fn inconsistent_transition_or_changed_routing_cannot_cycle() {
    let mut v = failover();
    v["groups"][0]["active"] = json!(true);
    assert_eq!(parse(&v).unwrap(), Health::Unknown);
    let mut v = failover();
    v["wans"][0]["metric"] = json!(2);
    assert_eq!(parse(&v).unwrap(), Health::Unknown);
    let mut v = failover();
    v["groups"][0]["interfaces"] = json!(["eth8", "eth10"]);
    assert_eq!(parse(&v).unwrap(), Health::Unknown);
}
#[test]
fn stale_future_missing_and_duplicate_data_fail_closed() {
    let v = failover();
    let bytes = serde_json::to_vec(&v).unwrap();
    let at = v["sampled_at"].as_u64().unwrap();
    assert!(classify(&bytes, "eth8", "eth9", at + 11).is_err());
    assert!(classify(&bytes, "eth8", "eth9", at - 6).is_err());
    let mut v = failover();
    v["links"][0]["timestamp_ms"] = json!((at - 11) * 1000);
    assert!(parse(&v).is_err());
    let mut v = failover();
    v["wans"][0]["interface"] = json!("eth10");
    assert!(parse(&v).is_err());
    let mut v = failover();
    let duplicate = v["wans"][0].clone();
    v["wans"].as_array_mut().unwrap().push(duplicate);
    assert!(parse(&v).is_err());
}

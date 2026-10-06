use axum::{
    Json, Router,
    body::Body,
    extract::State as AxumState,
    http::{Request, StatusCode},
    routing::post,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tower::ServiceExt;
use wan_recover::{
    config::MerossConfig,
    events::Mapping,
    meross::{Meross, Relay, signature},
    server::{self, HttpState},
    unix_time,
    worker::{Inbound, Status},
};

const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn app() -> (Router, mpsc::Receiver<Inbound>) {
    let (queue, rx) = mpsc::channel(1);
    (
        server::router(HttpState {
            token: TOKEN.into(),
            mapping: Mapping {
                alarm_pointer: "/alarm/name".into(),
                timestamp_pointer: "/timestamp".into(),
                down_alarm: "Primary WAN offline".into(),
                up_alarm: "Primary WAN restored".into(),
                timestamp_milliseconds: false,
            },
            max_age: 120,
            queue,
            status: Arc::new(Mutex::new(Status::default())),
            capture_webhook: true,
            last_webhook: Arc::new(Mutex::new(None)),
        }),
        rx,
    )
}

fn request(uri: &str, auth: Option<&str>, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    request.body(Body::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn webhooks_and_status_require_auth() {
    let (app, mut rx) = app();
    for auth in [None, Some("Bearer incorrect")] {
        let response = app
            .clone()
            .oneshot(request("/hooks/unifi", auth, json!({})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn healthy_process_does_not_claim_ready_before_worker_starts() {
    let (app, _) = app();
    for (uri, status) in [
        ("/healthz", StatusCode::OK),
        ("/readyz", StatusCode::SERVICE_UNAVAILABLE),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            status
        );
    }
}

#[tokio::test]
async fn accepts_only_after_worker_ack_and_supports_path_token() {
    for uri in ["/hooks/unifi".to_owned(), format!("/hooks/unifi/{TOKEN}")] {
        let (app, mut rx) = app();
        let task = tokio::spawn(app.oneshot(request(
            &uri,
            Some(&format!("Bearer {TOKEN}")),
            json!({"alarm":{"name":"Primary WAN offline"},"timestamp":unix_time()}),
        )));
        let input = rx.recv().await.unwrap();
        assert!(!task.is_finished());
        input.ack.send(true).unwrap();
        assert_eq!(task.await.unwrap().unwrap().status(), StatusCode::ACCEPTED);
    }
}

#[tokio::test]
async fn stale_or_other_wan_events_cannot_enqueue() {
    let (app, mut rx) = app();
    let auth = format!("Bearer {TOKEN}");
    let response = app
        .clone()
        .oneshot(request(
            "/hooks/unifi",
            Some(&auth),
            json!({"alarm":{"name":"Primary WAN offline"},"timestamp":unix_time()-121}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let response = app
        .oneshot(request(
            "/hooks/unifi",
            Some(&auth),
            json!({"alarm":{"name":"Starlink offline"},"timestamp":unix_time()}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn oversized_and_malformed_json_are_rejected() {
    let (app, _) = app();
    let auth = format!("Bearer {TOKEN}");
    assert_eq!(
        app.clone()
            .oneshot(request(
                "/hooks/unifi",
                Some(&auth),
                json!({"large":"x".repeat(65536)})
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let req = Request::builder()
        .method("POST")
        .uri("/hooks/unifi")
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(Body::from("{"))
        .unwrap();
    assert_eq!(
        app.oneshot(req).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn unavailable_worker_returns_retryable_failure() {
    let (app, rx) = app();
    drop(rx);
    let response = app
        .oneshot(request(
            "/hooks/unifi",
            Some(&format!("Bearer {TOKEN}")),
            json!({"alarm":{"name":"Primary WAN offline"},"timestamp":unix_time()}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

const UUID: &str = "0123456789abcdef0123456789abcdef";
const KEY: &str = "1234567890abcdef1234567890abcdef";

#[derive(Default)]
struct Device {
    on: bool,
    requests: Vec<Value>,
    bad_signature: bool,
    wrong_uuid: bool,
    wrong_id: bool,
    wrong_namespace: bool,
    wrong_method: bool,
    error: bool,
}

async fn meross_mock(
    AxumState(device): AxumState<Arc<Mutex<Device>>>,
    Json(packet): Json<Value>,
) -> Json<Value> {
    let mut device = device.lock().await;
    let id = packet["header"]["messageId"].as_str().unwrap();
    let ts = packet["header"]["timestamp"].as_u64().unwrap();
    assert_eq!(packet["header"]["sign"], signature(id, KEY, ts));
    let namespace = packet["header"]["namespace"].as_str().unwrap();
    let method = packet["header"]["method"].as_str().unwrap();
    let payload = if method == "SET" {
        assert_eq!(namespace, "Appliance.Control.ToggleX");
        assert_eq!(packet["payload"]["togglex"]["channel"], 0);
        device.on = packet["payload"]["togglex"]["onoff"] == 1;
        json!({})
    } else {
        assert_eq!(namespace, "Appliance.System.All");
        json!({"all": {"system": {"hardware": {"uuid": if device.wrong_uuid { "another-device" } else { UUID }}}, "digest": {"togglex":[{"channel":0,"onoff":u8::from(device.on)}]}}})
    };
    let response = json!({"header": {
        "messageId": if device.wrong_id { "wrong" } else { id }, "timestamp": ts,
        "namespace": if device.wrong_namespace { "wrong" } else { namespace },
        "method": if device.error { "ERROR".to_owned() } else if device.wrong_method { "PUSH".to_owned() } else { format!("{method}ACK") },
        "sign": if device.bad_signature { "invalid".into() } else { signature(id, KEY, ts) }
    },"payload":payload});
    device.requests.push(packet);
    Json(response)
}

async fn device() -> (
    tempfile::TempDir,
    Meross,
    Arc<Mutex<Device>>,
    tokio::task::JoinHandle<()>,
) {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("key");
    std::fs::write(&key, KEY).unwrap();
    let device = Arc::new(Mutex::new(Device {
        on: true,
        ..Default::default()
    }));
    let app = Router::new()
        .route("/config", post(meross_mock))
        .with_state(device.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let relay = Meross::with_url(
        &MerossConfig {
            address: "127.0.0.1".parse().unwrap(),
            uuid: UUID.into(),
            key_file: key,
            channel: 0,
        },
        format!("http://{address}/config"),
    )
    .unwrap();
    (dir, relay, device, task)
}

#[test]
fn signing_matches_known_md5_vector() {
    assert_eq!(signature("", "", 0), "cfcd208495d565ef66e7dff9f98764da");
}

#[tokio::test]
async fn actual_http_signing_identity_and_channel_commands() {
    let (_dir, relay, device, task) = device().await;
    assert!(relay.read().await.unwrap());
    relay.set(false).await.unwrap();
    assert!(!relay.read().await.unwrap());
    relay.set(true).await.unwrap();
    assert!(relay.read().await.unwrap());
    let d = device.lock().await;
    assert_eq!(
        d.requests
            .iter()
            .filter(|r| r["header"]["method"] == "SET")
            .count(),
        2
    );
    task.abort();
}

#[tokio::test]
async fn protocol_mismatches_and_device_errors_are_rejected() {
    for mode in 0..6 {
        let (_dir, relay, device, task) = device().await;
        {
            let mut d = device.lock().await;
            match mode {
                0 => d.bad_signature = true,
                1 => d.wrong_id = true,
                2 => d.wrong_namespace = true,
                3 => d.wrong_method = true,
                4 => d.error = true,
                _ => d.wrong_uuid = true,
            }
        }
        assert!(relay.set(false).await.is_err());
        assert!(
            device
                .lock()
                .await
                .requests
                .iter()
                .all(|r| r["header"]["method"] != "SET")
        );
        task.abort();
    }
}

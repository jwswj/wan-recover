use crate::{
    events::Mapping,
    unix_time,
    worker::{Inbound, SharedStatus},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct HttpState {
    pub token: String,
    pub mapping: Mapping,
    pub max_age: u64,
    pub queue: mpsc::Sender<Inbound>,
    pub status: SharedStatus,
    pub capture_webhook: bool,
    pub last_webhook: std::sync::Arc<tokio::sync::Mutex<Option<Value>>>,
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    bool::from(supplied.as_bytes().ct_eq(token.as_bytes()))
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready))
        .route("/status", get(status))
        .route("/last-webhook", get(last_webhook))
        .route("/hooks/unifi", post(webhook))
        .route("/hooks/unifi/{token}", post(webhook_path))
        .layer(DefaultBodyLimit::max(65536))
        .with_state(state)
}

async fn ready(State(s): State<HttpState>) -> StatusCode {
    let status = s.status.lock().await;
    if status.heartbeat != 0 && unix_time().saturating_sub(status.heartbeat) <= 60 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn status(State(s): State<HttpState>, headers: HeaderMap) -> Result<Json<Value>, StatusCode> {
    if !authorized(&headers, &s.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(
        serde_json::to_value(s.status.lock().await.clone()).unwrap(),
    ))
}

async fn last_webhook(
    State(s): State<HttpState>,
    headers: HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    if !authorized(&headers, &s.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if !s.capture_webhook {
        return Err(StatusCode::NOT_FOUND);
    }
    s.last_webhook
        .lock()
        .await
        .clone()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn webhook(
    State(s): State<HttpState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    if !authorized(&headers, &s.token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    accept(s, body).await
}

async fn webhook_path(
    State(s): State<HttpState>,
    Path(token): Path<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    if !bool::from(token.as_bytes().ct_eq(s.token.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    accept(s, body).await
}

async fn accept(s: HttpState, body: Value) -> Result<(StatusCode, Json<Value>), StatusCode> {
    {
        let mut status = s.status.lock().await;
        status.last_received_at = Some(unix_time());
        status.received_webhooks = status.received_webhooks.saturating_add(1);
    }
    // One bounded, in-memory body helps inspect actual firmware payloads.
    // Capture is disabled in active mode; headers and tokens are never captured.
    if s.capture_webhook {
        *s.last_webhook.lock().await = Some(body.clone());
    }
    let event = s
        .mapping
        .parse(&body, unix_time(), s.max_age)
        .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    let Some(event) = event else {
        tracing::info!("unmatched UniFi webhook ignored");
        return Ok((StatusCode::OK, Json(json!({"result": "ignored"}))));
    };
    let (ack, reply) = oneshot::channel();
    s.queue
        .try_send(Inbound { event, ack })
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let changed = tokio::time::timeout(std::time::Duration::from_secs(30), reply)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"result": if changed { "recorded" } else { "duplicate_or_out_of_order" }})),
    ))
}

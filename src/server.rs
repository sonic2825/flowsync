use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{Datelike, Duration as ChronoDuration, Local, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{
    broadcast,
    mpsc::{channel, Receiver, Sender},
    Mutex,
};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::{AppConfig, RemoteConfig};
use crate::engine::{
    parse_bandwidth_limit, parse_stream_chunk_size, run_transfer_with_reporter, ProgressReporter,
    SyncMode, SyncOptions, TransferEvent,
};
use crate::remote::{resolve_location_and_operator, Location};
mod api;
mod db;
mod events;
mod scheduler;

use self::api::*;
use self::events::*;
use self::scheduler::*;

const TASK_EVENT_QUEUE_CAPACITY: usize = 5000;
static DROPPED_TASK_EVENTS: AtomicU64 = AtomicU64::new(0);

#[derive(RustEmbed)]
#[folder = "web/dist"]
struct WebAssets;

#[derive(Clone)]
struct Db {
    path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct EventCleanupPolicy {
    pub retention_days: u32,
    pub max_rows: u64,
    pub interval_secs: u64,
}

#[derive(Debug, Clone)]
struct TaskEventWrite {
    task_id: i64,
    run_id: Option<i64>,
    event_type: String,
    payload: String,
    created_at: String,
}

#[derive(Clone)]
struct AppState {
    db: Db,
    scheduler: JobScheduler,
    scheduled_jobs: Arc<Mutex<HashMap<i64, Uuid>>>,
    running_tasks: Arc<Mutex<HashSet<i64>>>,
    ws_tx: broadcast::Sender<String>,
    event_tx: Sender<TaskEventWrite>,
    system: Arc<Mutex<System>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TaskRecord {
    id: i64,
    name: String,
    source: String,
    destination: String,
    mode: String,
    cron_expr: Option<String>,
    enabled: bool,
    transfers: u32,
    checkers: u32,
    checksum: bool,
    ignore_existing: bool,
    dry_run: bool,
    include: Vec<String>,
    exclude: Vec<String>,
    bandwidth_limit: Option<String>,
    chunk_size: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize, Clone)]
struct TaskListItem {
    #[serde(flatten)]
    task: TaskRecord,
    status: String,
}

impl TaskRecord {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let includes_json: String = row.get(12)?;
        let excludes_json: String = row.get(13)?;
        Ok(Self {
            id: row.get(0)?,
            name: row.get(1)?,
            source: row.get(2)?,
            destination: row.get(3)?,
            mode: row.get(4)?,
            cron_expr: row.get(5)?,
            enabled: i64_to_bool(row.get(6)?),
            transfers: row.get::<_, i64>(7)? as u32,
            checkers: row.get::<_, i64>(8)? as u32,
            checksum: i64_to_bool(row.get(9)?),
            ignore_existing: i64_to_bool(row.get(10)?),
            dry_run: i64_to_bool(row.get(11)?),
            include: serde_json::from_str(&includes_json).unwrap_or_default(),
            exclude: serde_json::from_str(&excludes_json).unwrap_or_default(),
            bandwidth_limit: row.get(14)?,
            chunk_size: row.get(15)?,
            created_at: row.get(16)?,
            updated_at: row.get(17)?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RemoteRecord {
    id: i64,
    name: String,
    r#type: String,
    config_json: serde_json::Value,
    created_at: String,
    updated_at: String,
}

impl RemoteRecord {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let config_raw: String = row.get(3)?;
        let config_json = serde_json::from_str(&config_raw)
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
        Ok(Self {
            id: row.get(0)?,
            name: row.get(1)?,
            r#type: row.get(2)?,
            config_json,
            created_at: row.get(4)?,
            updated_at: row.get(5)?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateRemoteRequest {
    name: String,
    r#type: String,
    #[serde(default)]
    config_json: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateRemoteRequest {
    r#type: String,
    #[serde(default)]
    config_json: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateTaskRequest {
    name: String,
    source: String,
    destination: String,
    mode: String,
    cron_expr: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_transfers")]
    transfers: u32,
    #[serde(default = "default_checkers")]
    checkers: u32,
    #[serde(default)]
    checksum: bool,
    #[serde(default)]
    ignore_existing: bool,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    bandwidth_limit: Option<String>,
    #[serde(default = "default_chunk_size")]
    chunk_size: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateTaskRequest {
    name: String,
    source: String,
    destination: String,
    mode: String,
    cron_expr: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_transfers")]
    transfers: u32,
    #[serde(default = "default_checkers")]
    checkers: u32,
    #[serde(default)]
    checksum: bool,
    #[serde(default)]
    ignore_existing: bool,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    bandwidth_limit: Option<String>,
    #[serde(default = "default_chunk_size")]
    chunk_size: String,
}

#[derive(Debug, Clone)]
struct RunErrorRecord {
    path: Option<String>,
    message: String,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct RunRecord {
    id: i64,
    task_id: i64,
    trigger_type: String,
    started_at: String,
    ended_at: Option<String>,
    status: String,
    copied_files: u64,
    failed_files: u64,
    copied_bytes: u64,
    error_message: Option<String>,
}

impl RunRecord {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            task_id: row.get(1)?,
            trigger_type: row.get(2)?,
            started_at: row.get(3)?,
            ended_at: row.get(4)?,
            status: row.get(5)?,
            copied_files: row.get::<_, i64>(6)? as u64,
            failed_files: row.get::<_, i64>(7)? as u64,
            copied_bytes: row.get::<_, i64>(8)? as u64,
            error_message: row.get(9)?,
        })
    }
}

#[derive(Debug, Serialize)]
struct TaskEventRecord {
    id: i64,
    task_id: i64,
    run_id: Option<i64>,
    event_type: String,
    payload: serde_json::Value,
    created_at: String,
}

impl TaskEventRecord {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let payload_raw: String = row.get(4)?;
        let payload =
            serde_json::from_str(&payload_raw).unwrap_or_else(|_| json!({ "raw": payload_raw }));
        Ok(Self {
            id: row.get(0)?,
            task_id: row.get(1)?,
            run_id: row.get(2)?,
            event_type: row.get(3)?,
            payload,
            created_at: row.get(5)?,
        })
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
struct ListRunsQuery {
    task_id: Option<i64>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
struct ListTaskEventsQuery {
    task_id: Option<i64>,
    run_id: Option<i64>,
    limit: Option<u32>,
    keyword: Option<String>,
}

#[derive(Debug, Serialize)]
struct DashboardResponse {
    running_tasks: usize,
    today_transferred_bytes: u64,
    today_runs: u64,
    cpu_usage_percent: f32,
    memory_used_bytes: u64,
    memory_total_bytes: u64,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: value.to_string(),
        }
    }
}

pub async fn run_server(
    host: String,
    port: u16,
    db_path: String,
    event_cleanup_policy: EventCleanupPolicy,
) -> Result<()> {
    let db = Db::new(PathBuf::from(db_path));
    db.init().await?;
    db.cleanup_task_events(event_cleanup_policy).await?;

    let scheduler = JobScheduler::new()
        .await
        .context("create scheduler failed")?;
    let (ws_tx, _) = broadcast::channel(1024);
    let (event_tx, event_rx) = channel::<TaskEventWrite>(TASK_EVENT_QUEUE_CAPACITY);

    let state = Arc::new(AppState {
        db: db.clone(),
        scheduler,
        scheduled_jobs: Arc::new(Mutex::new(HashMap::new())),
        running_tasks: Arc::new(Mutex::new(HashSet::new())),
        ws_tx,
        event_tx,
        system: Arc::new(Mutex::new(System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything()),
        ))),
    });

    tokio::spawn(task_event_writer_worker(db.clone(), event_rx));

    load_schedules(state.clone()).await?;
    state
        .scheduler
        .start()
        .await
        .context("failed to start scheduler")?;

    let app = Router::new()
        .route("/api/health", get(api_health))
        .route(
            "/api/remotes",
            get(api_list_remotes).post(api_create_remote),
        )
        .route(
            "/api/remotes/:name",
            put(api_update_remote).delete(api_delete_remote),
        )
        .route("/api/tasks", get(api_list_tasks).post(api_create_task))
        .route(
            "/api/tasks/:id",
            put(api_update_task).delete(api_delete_task),
        )
        .route("/api/tasks/:id/run", post(api_run_task_now))
        .route("/api/tasks/:id/pause", post(api_pause_task))
        .route("/api/tasks/:id/resume", post(api_resume_task))
        .route("/api/runs", get(api_list_runs))
        .route("/api/events", get(api_list_events))
        .route("/api/dashboard", get(api_dashboard))
        .route("/ws", get(ws_handler))
        .route("/", get(index_handler))
        .fallback(get(index_handler))
        .with_state(state);

    if event_cleanup_policy.retention_days > 0 || event_cleanup_policy.max_rows > 0 {
        let db_for_cleanup = db.clone();
        tokio::spawn(async move {
            let interval_secs = event_cleanup_policy.interval_secs.max(30);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                ticker.tick().await;
                if let Err(err) = db_for_cleanup
                    .cleanup_task_events(event_cleanup_policy)
                    .await
                {
                    warn!(error = %err, "task events cleanup failed");
                }
            }
        });
    }

    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .with_context(|| format!("invalid bind address: {host}:{port}"))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("web server listening on http://{}", addr);
    axum::serve(listener, app)
        .await
        .context("web server failed")?;
    Ok(())
}

fn normalize_task_request(req: &mut CreateTaskRequest) -> Result<(), ApiError> {
    req.name = req.name.trim().to_string();
    req.source = req.source.trim().to_string();
    req.destination = req.destination.trim().to_string();
    req.mode = req.mode.trim().to_ascii_lowercase();
    req.chunk_size = req.chunk_size.trim().to_string();
    if req.name.is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }
    if req.source.is_empty() || req.destination.is_empty() {
        return Err(ApiError::bad_request("source and destination are required"));
    }
    parse_mode(&req.mode).map_err(|e| ApiError::bad_request(e.to_string()))?;
    if req.transfers == 0 {
        req.transfers = 1;
    }
    if req.checkers == 0 {
        req.checkers = 1;
    }
    if req.chunk_size.is_empty() {
        req.chunk_size = default_chunk_size();
    }
    parse_stream_chunk_size(Some(req.chunk_size.as_str()))
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(expr) = req.cron_expr.as_mut() {
        let t = expr.trim().to_string();
        *expr = t;
    }
    Ok(())
}

fn normalize_task_update_request(req: &mut UpdateTaskRequest) -> Result<(), ApiError> {
    req.name = req.name.trim().to_string();
    req.source = req.source.trim().to_string();
    req.destination = req.destination.trim().to_string();
    req.mode = req.mode.trim().to_ascii_lowercase();
    req.chunk_size = req.chunk_size.trim().to_string();
    if req.name.is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }
    if req.source.is_empty() || req.destination.is_empty() {
        return Err(ApiError::bad_request("source and destination are required"));
    }
    parse_mode(&req.mode).map_err(|e| ApiError::bad_request(e.to_string()))?;
    if req.transfers == 0 {
        req.transfers = 1;
    }
    if req.checkers == 0 {
        req.checkers = 1;
    }
    if req.chunk_size.is_empty() {
        req.chunk_size = default_chunk_size();
    }
    parse_stream_chunk_size(Some(req.chunk_size.as_str()))
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(expr) = req.cron_expr.as_mut() {
        let t = expr.trim().to_string();
        *expr = t;
    }
    Ok(())
}

fn normalize_create_remote_request(req: &mut CreateRemoteRequest) -> Result<(), ApiError> {
    req.name = req.name.trim().to_string();
    req.r#type = req.r#type.trim().to_ascii_lowercase();
    if req.name.is_empty() {
        return Err(ApiError::bad_request("remote name is required"));
    }
    if req.r#type.is_empty() {
        return Err(ApiError::bad_request("remote type is required"));
    }
    if !matches!(req.r#type.as_str(), "fs" | "s3" | "sftp") {
        return Err(ApiError::bad_request(
            "remote type must be one of: fs|s3|sftp (ftp is not supported in this build)",
        ));
    }
    if !req.config_json.is_object() {
        return Err(ApiError::bad_request("config_json must be a JSON object"));
    }
    Ok(())
}

fn normalize_update_remote_request(req: &mut UpdateRemoteRequest) -> Result<(), ApiError> {
    req.r#type = req.r#type.trim().to_ascii_lowercase();
    if req.r#type.is_empty() {
        return Err(ApiError::bad_request("remote type is required"));
    }
    if !matches!(req.r#type.as_str(), "fs" | "s3" | "sftp") {
        return Err(ApiError::bad_request(
            "remote type must be one of: fs|s3|sftp (ftp is not supported in this build)",
        ));
    }
    if !req.config_json.is_object() {
        return Err(ApiError::bad_request("config_json must be a JSON object"));
    }
    Ok(())
}

fn remote_record_to_config(remote: &RemoteRecord) -> Result<RemoteConfig> {
    let mut raw = match &remote.config_json {
        serde_json::Value::Object(map) => map.clone(),
        _ => {
            return Err(anyhow!(
                "remote `{}` config_json must be object",
                remote.name
            ))
        }
    };
    raw.insert(
        "type".to_string(),
        serde_json::Value::String(remote.r#type.clone()),
    );
    let value = serde_json::Value::Object(raw);
    serde_json::from_value(value)
        .with_context(|| format!("invalid remote config for `{}`", remote.name))
}

fn bool_to_i64(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn i64_to_bool(value: i64) -> bool {
    value != 0
}

fn default_true() -> bool {
    true
}

fn default_transfers() -> u32 {
    4
}

fn default_checkers() -> u32 {
    8
}

fn default_chunk_size() -> String {
    "8MB".to_string()
}

fn ensure_tasks_chunk_size_column(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(tasks)")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == "chunk_size" {
            return Ok(());
        }
    }
    conn.execute(
        "ALTER TABLE tasks ADD COLUMN chunk_size TEXT NOT NULL DEFAULT '8MB'",
        [],
    )?;
    Ok(())
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::broadcast;

    fn test_db_path() -> PathBuf {
        let ts = now_unix_millis();
        std::env::temp_dir().join(format!("flowsync-test-{ts}.db"))
    }

    #[tokio::test]
    async fn execute_task_should_release_running_state_on_failure() {
        let db_path = test_db_path();
        let db = Db::new(db_path.clone());
        db.init().await.expect("init test db");

        let created = db
            .create_task(CreateTaskRequest {
                name: "cleanup-check".to_string(),
                source: "missing_remote:/src".to_string(),
                destination: "missing_remote:/dst".to_string(),
                mode: "copy".to_string(),
                cron_expr: None,
                enabled: true,
                transfers: 1,
                checkers: 1,
                checksum: false,
                ignore_existing: false,
                dry_run: false,
                include: Vec::new(),
                exclude: Vec::new(),
                bandwidth_limit: None,
                chunk_size: "8MB".to_string(),
            })
            .await
            .expect("create test task");

        let scheduler = JobScheduler::new().await.expect("create scheduler");
        let (ws_tx, _) = broadcast::channel(16);
        let (event_tx, _event_rx) = channel::<TaskEventWrite>(16);
        let state = Arc::new(AppState {
            db: db.clone(),
            scheduler,
            scheduled_jobs: Arc::new(Mutex::new(HashMap::new())),
            running_tasks: Arc::new(Mutex::new(HashSet::new())),
            ws_tx,
            event_tx,
            system: Arc::new(Mutex::new(System::new_with_specifics(
                RefreshKind::new()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            ))),
        });

        let result = execute_task(state.clone(), created.clone(), "manual").await;
        assert!(result.is_err(), "missing remote should fail task execution");

        let running = state.running_tasks.lock().await;
        assert!(
            !running.contains(&created.id),
            "failed task must be removed from running_tasks"
        );

        let _ = tokio::fs::remove_file(db_path).await;
    }

    #[test]
    fn persist_task_event_should_drop_progress_event_when_queue_is_full() {
        DROPPED_TASK_EVENTS.store(0, Ordering::Relaxed);
        let (tx, mut rx) = channel::<TaskEventWrite>(1);
        tx.try_send(TaskEventWrite {
            task_id: 42,
            run_id: None,
            event_type: "seed".to_string(),
            payload: "{}".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        })
        .expect("seed queue");

        persist_task_event(
            &tx,
            42,
            Some(7),
            json!({
                "event": "task_progress",
                "at": "2026-01-01T00:00:01Z",
                "progress_pct": 50
            }),
        );

        let dropped = DROPPED_TASK_EVENTS.load(Ordering::Relaxed);
        assert_eq!(dropped, 1, "full queue should count dropped event");
        let first = rx.try_recv().expect("seed event should still exist");
        assert_eq!(first.event_type, "seed");
        assert!(rx.try_recv().is_err(), "progress event should be dropped");
    }

    #[test]
    fn persist_task_event_should_fill_created_at_when_missing() {
        let (tx, mut rx) = channel::<TaskEventWrite>(4);
        persist_task_event(
            &tx,
            1,
            None,
            json!({
                "event": "task_started"
            }),
        );

        let rec = rx.try_recv().expect("event should be queued");
        assert_eq!(rec.event_type, "task_started");
        assert!(
            !rec.created_at.is_empty(),
            "created_at should fallback to current timestamp"
        );
    }
}

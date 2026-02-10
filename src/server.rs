use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{Datelike, Duration as ChronoDuration, Local, TimeZone, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{
    broadcast,
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex,
};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::{AppConfig, RemoteConfig};
use crate::engine::{ProgressReporter, SyncMode, SyncOptions, TransferEvent, run_transfer_with_reporter};
use crate::remote::{Location, resolve_location_and_operator};

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

impl Db {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn open(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)
            .with_context(|| format!("failed to open sqlite db: {}", self.path.display()))?;
        conn.execute_batch(
            "PRAGMA foreign_keys=ON;
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;",
        )
        .context("failed to initialize sqlite pragmas")?;
        Ok(conn)
    }

    async fn init(&self) -> Result<()> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = db.open()?;
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS remotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    type TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    cron_expr TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    transfers INTEGER NOT NULL DEFAULT 4,
                    checkers INTEGER NOT NULL DEFAULT 8,
                    checksum INTEGER NOT NULL DEFAULT 0,
                    ignore_existing INTEGER NOT NULL DEFAULT 0,
                    dry_run INTEGER NOT NULL DEFAULT 0,
                    include_patterns TEXT NOT NULL DEFAULT '[]',
                    exclude_patterns TEXT NOT NULL DEFAULT '[]',
                    bandwidth_limit TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS task_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    trigger_type TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT NOT NULL,
                    copied_files INTEGER NOT NULL DEFAULT 0,
                    failed_files INTEGER NOT NULL DEFAULT 0,
                    copied_bytes INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS run_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    path TEXT,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES task_runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS task_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    run_id INTEGER,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY(run_id) REFERENCES task_runs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_task_events_task_id_id ON task_events(task_id, id);
                CREATE INDEX IF NOT EXISTS idx_task_events_run_id_id ON task_events(run_id, id);
                "#,
            )
            .context("failed to init sqlite schema")?;
            Ok(())
        })
        .await
        .context("sqlite init join error")?
    }

    async fn list_remotes(&self) -> Result<Vec<RemoteRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<RemoteRecord>> {
            let conn = db.open()?;
            let mut stmt = conn.prepare(
                "SELECT id,name,type,config_json,created_at,updated_at FROM remotes ORDER BY name ASC",
            )?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(RemoteRecord::from_row(row)?);
            }
            Ok(out)
        })
        .await
        .context("list remotes join error")?
    }

    async fn create_remote(&self, req: CreateRemoteRequest) -> Result<RemoteRecord> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<RemoteRecord> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO remotes (name,type,config_json,created_at,updated_at) VALUES (?1,?2,?3,?4,?5)",
                params![req.name, req.r#type, req.config_json.to_string(), now, now],
            )?;
            conn.query_row(
                "SELECT id,name,type,config_json,created_at,updated_at FROM remotes WHERE name=?1",
                params![req.name],
                RemoteRecord::from_row,
            )
            .context("read inserted remote")
        })
        .await
        .context("create remote join error")?
    }

    async fn update_remote(&self, name: &str, req: UpdateRemoteRequest) -> Result<Option<RemoteRecord>> {
        let db = self.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<RemoteRecord>> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            let affected = conn.execute(
                "UPDATE remotes SET type=?1,config_json=?2,updated_at=?3 WHERE name=?4",
                params![req.r#type, req.config_json.to_string(), now, name],
            )?;
            if affected == 0 {
                return Ok(None);
            }
            let remote = conn
                .query_row(
                    "SELECT id,name,type,config_json,created_at,updated_at FROM remotes WHERE name=?1",
                    params![name],
                    RemoteRecord::from_row,
                )
                .optional()?;
            Ok(remote)
        })
        .await
        .context("update remote join error")?
    }

    async fn delete_remote(&self, name: &str) -> Result<bool> {
        let db = self.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let conn = db.open()?;
            let affected = conn.execute("DELETE FROM remotes WHERE name=?1", params![name])?;
            Ok(affected > 0)
        })
        .await
        .context("delete remote join error")?
    }

    async fn load_app_config(&self) -> Result<AppConfig> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<AppConfig> {
            let mut cfg = AppConfig::load().unwrap_or(AppConfig {
                remotes: HashMap::new(),
            });
            let conn = db.open()?;
            let mut stmt = conn.prepare(
                "SELECT id,name,type,config_json,created_at,updated_at FROM remotes ORDER BY name ASC",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let remote = RemoteRecord::from_row(row)?;
                let remote_cfg = remote_record_to_config(&remote)?;
                cfg.remotes.insert(remote.name, remote_cfg);
            }
            Ok(cfg)
        })
        .await
        .context("load app config join error")?
    }

    async fn list_tasks(&self) -> Result<Vec<TaskRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<TaskRecord>> {
            let conn = db.open()?;
            let mut stmt = conn.prepare(
                "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,created_at,updated_at
                 FROM tasks ORDER BY id DESC",
            )?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(TaskRecord::from_row(row)?);
            }
            Ok(out)
        })
        .await
        .context("list tasks join error")?
    }

    async fn list_latest_task_status(&self) -> Result<HashMap<i64, String>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<HashMap<i64, String>> {
            let conn = db.open()?;
            let mut stmt = conn.prepare(
                "SELECT t.id,
                        (SELECT r.status FROM task_runs r WHERE r.task_id=t.id ORDER BY r.id DESC LIMIT 1) AS last_status
                 FROM tasks t",
            )?;
            let mut rows = stmt.query([])?;
            let mut out = HashMap::new();
            while let Some(row) = rows.next()? {
                let task_id: i64 = row.get(0)?;
                let last_status: Option<String> = row.get(1)?;
                out.insert(task_id, last_status.unwrap_or_else(|| "idle".to_string()));
            }
            Ok(out)
        })
        .await
        .context("list latest task status join error")?
    }

    async fn get_task(&self, id: i64) -> Result<Option<TaskRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<TaskRecord>> {
            let conn = db.open()?;
            let task = conn
                .query_row(
                    "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,created_at,updated_at
                     FROM tasks WHERE id=?1",
                    params![id],
                    TaskRecord::from_row,
                )
                .optional()?;
            Ok(task)
        })
        .await
        .context("get task join error")?
    }

    async fn create_task(&self, req: CreateTaskRequest) -> Result<TaskRecord> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<TaskRecord> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            let includes = serde_json::to_string(&req.include).context("serialize include")?;
            let excludes = serde_json::to_string(&req.exclude).context("serialize exclude")?;
            conn.execute(
                "INSERT INTO tasks (name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
                params![
                    req.name,
                    req.source,
                    req.destination,
                    req.mode,
                    req.cron_expr,
                    bool_to_i64(req.enabled),
                    req.transfers,
                    req.checkers,
                    bool_to_i64(req.checksum),
                    bool_to_i64(req.ignore_existing),
                    bool_to_i64(req.dry_run),
                    includes,
                    excludes,
                    req.bandwidth_limit,
                    now,
                    now
                ],
            )?;
            let id = conn.last_insert_rowid();
            conn.query_row(
                "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,created_at,updated_at
                 FROM tasks WHERE id=?1",
                params![id],
                TaskRecord::from_row,
            )
            .context("read inserted task")
        })
        .await
        .context("create task join error")?
    }

    async fn update_task(&self, id: i64, req: UpdateTaskRequest) -> Result<Option<TaskRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<TaskRecord>> {
            let conn = db.open()?;
            let exists: Option<i64> = conn
                .query_row("SELECT id FROM tasks WHERE id=?1", params![id], |r| r.get(0))
                .optional()?;
            if exists.is_none() {
                return Ok(None);
            }
            let now = Utc::now().to_rfc3339();
            let includes = serde_json::to_string(&req.include).context("serialize include")?;
            let excludes = serde_json::to_string(&req.exclude).context("serialize exclude")?;
            conn.execute(
                "UPDATE tasks SET name=?1,source=?2,destination=?3,mode=?4,cron_expr=?5,enabled=?6,transfers=?7,checkers=?8,checksum=?9,ignore_existing=?10,dry_run=?11,include_patterns=?12,exclude_patterns=?13,bandwidth_limit=?14,updated_at=?15
                 WHERE id=?16",
                params![
                    req.name,
                    req.source,
                    req.destination,
                    req.mode,
                    req.cron_expr,
                    bool_to_i64(req.enabled),
                    req.transfers,
                    req.checkers,
                    bool_to_i64(req.checksum),
                    bool_to_i64(req.ignore_existing),
                    bool_to_i64(req.dry_run),
                    includes,
                    excludes,
                    req.bandwidth_limit,
                    now,
                    id
                ],
            )?;
            let task = conn
                .query_row(
                    "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,created_at,updated_at
                     FROM tasks WHERE id=?1",
                    params![id],
                    TaskRecord::from_row,
                )
                .optional()?;
            Ok(task)
        })
        .await
        .context("update task join error")?
    }

    async fn set_task_enabled(&self, id: i64, enabled: bool) -> Result<Option<TaskRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<TaskRecord>> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            let changed = conn.execute(
                "UPDATE tasks SET enabled=?1,updated_at=?2 WHERE id=?3",
                params![bool_to_i64(enabled), now, id],
            )?;
            if changed == 0 {
                return Ok(None);
            }
            let task = conn
                .query_row(
                    "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,created_at,updated_at
                     FROM tasks WHERE id=?1",
                    params![id],
                    TaskRecord::from_row,
                )
                .optional()?;
            Ok(task)
        })
        .await
        .context("set task enabled join error")?
    }

    async fn delete_task(&self, id: i64) -> Result<bool> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let conn = db.open()?;
            let affected = conn.execute("DELETE FROM tasks WHERE id=?1", params![id])?;
            Ok(affected > 0)
        })
        .await
        .context("delete task join error")?
    }

    async fn create_run(&self, task_id: i64, trigger: &str) -> Result<i64> {
        let db = self.clone();
        let trigger = trigger.to_string();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO task_runs (task_id,trigger_type,started_at,status) VALUES (?1,?2,?3,'running')",
                params![task_id, trigger, now],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await
        .context("create run join error")?
    }

    async fn finish_run(
        &self,
        run_id: i64,
        status: &str,
        copied_files: u64,
        failed_files: u64,
        copied_bytes: u64,
        error_message: Option<String>,
    ) -> Result<()> {
        let db = self.clone();
        let status = status.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "UPDATE task_runs
                 SET ended_at=?1,status=?2,copied_files=?3,failed_files=?4,copied_bytes=?5,error_message=?6
                 WHERE id=?7",
                params![
                    now,
                    status,
                    copied_files as i64,
                    failed_files as i64,
                    copied_bytes as i64,
                    error_message,
                    run_id
                ],
            )?;
            Ok(())
        })
        .await
        .context("finish run join error")?
    }

    async fn insert_run_errors(&self, run_id: i64, records: Vec<RunErrorRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = db.open()?;
            let tx = conn.transaction()?;
            for rec in records {
                tx.execute(
                    "INSERT INTO run_errors (run_id,path,message,created_at) VALUES (?1,?2,?3,?4)",
                    params![run_id, rec.path, rec.message, rec.created_at],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .context("insert run errors join error")?
    }

    async fn insert_task_events_batch(&self, records: Vec<TaskEventWrite>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = db.open()?;
            let tx = conn.transaction()?;
            let mut stmt = tx.prepare(
                "INSERT INTO task_events (task_id,run_id,event_type,payload,created_at) VALUES (?1,?2,?3,?4,?5)",
            )?;
            for rec in records {
                stmt.execute(params![
                    rec.task_id,
                    rec.run_id,
                    rec.event_type,
                    rec.payload,
                    rec.created_at
                ])?;
            }
            drop(stmt);
            tx.commit()?;
            Ok(())
        })
        .await
        .context("insert task events batch join error")?
    }

    async fn list_task_events(&self, query: ListTaskEventsQuery) -> Result<Vec<TaskEventRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<TaskEventRecord>> {
            let conn = db.open()?;
            let limit = query.limit.unwrap_or(200).clamp(1, 2000) as i64;
            let keyword = query
                .keyword
                .as_ref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| format!("%{}%", v));
            let mut out = Vec::new();
            if let Some(run_id) = query.run_id {
                let mut stmt = conn.prepare(
                    "SELECT id,task_id,run_id,event_type,payload,created_at
                     FROM task_events
                     WHERE run_id=?1
                     ORDER BY id DESC
                     LIMIT ?2",
                )?;
                let mut rows = stmt.query(params![run_id, limit])?;
                while let Some(row) = rows.next()? {
                    out.push(TaskEventRecord::from_row(row)?);
                }
            } else if let Some(task_id) = query.task_id {
                let (sql, args): (&str, Vec<rusqlite::types::Value>) = if let Some(kw) = keyword {
                    (
                        "SELECT id,task_id,run_id,event_type,payload,created_at
                         FROM task_events
                         WHERE task_id=?1 AND (payload LIKE ?2 OR event_type LIKE ?2)
                         ORDER BY id DESC
                         LIMIT ?3",
                        vec![task_id.into(), kw.into(), limit.into()],
                    )
                } else {
                    (
                        "SELECT id,task_id,run_id,event_type,payload,created_at
                         FROM task_events
                         WHERE task_id=?1
                         ORDER BY id DESC
                         LIMIT ?2",
                        vec![task_id.into(), limit.into()],
                    )
                };
                let mut stmt = conn.prepare(sql)?;
                let mut rows = stmt.query(rusqlite::params_from_iter(args))?;
                while let Some(row) = rows.next()? {
                    out.push(TaskEventRecord::from_row(row)?);
                }
            } else {
                let mut stmt = conn.prepare(
                    "SELECT id,task_id,run_id,event_type,payload,created_at
                     FROM task_events
                     ORDER BY id DESC
                     LIMIT ?1",
                )?;
                let mut rows = stmt.query(params![limit])?;
                while let Some(row) = rows.next()? {
                    out.push(TaskEventRecord::from_row(row)?);
                }
            }
            out.reverse();
            Ok(out)
        })
        .await
        .context("list task events join error")?
    }

    async fn cleanup_task_events(&self, policy: EventCleanupPolicy) -> Result<()> {
        if policy.retention_days == 0 && policy.max_rows == 0 {
            return Ok(());
        }
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = db.open()?;
            if policy.retention_days > 0 {
                let cutoff = (Utc::now() - ChronoDuration::days(policy.retention_days as i64)).to_rfc3339();
                conn.execute("DELETE FROM task_events WHERE created_at < ?1", params![cutoff])?;
            }
            if policy.max_rows > 0 {
                let count: i64 = conn.query_row("SELECT COUNT(*) FROM task_events", [], |r| r.get(0))?;
                let max_rows = policy.max_rows as i64;
                if count > max_rows {
                    let remove = count - max_rows;
                    conn.execute(
                        "DELETE FROM task_events WHERE id IN (SELECT id FROM task_events ORDER BY id ASC LIMIT ?1)",
                        params![remove],
                    )?;
                }
            }
            Ok(())
        })
        .await
        .context("cleanup task events join error")?
    }

    async fn list_runs(&self, query: ListRunsQuery) -> Result<Vec<RunRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<RunRecord>> {
            let conn = db.open()?;
            let limit = query.limit.unwrap_or(50).clamp(1, 500);
            let sql_all = "SELECT id,task_id,trigger_type,started_at,ended_at,status,copied_files,failed_files,copied_bytes,error_message
                           FROM task_runs ORDER BY id DESC LIMIT ?1";
            let sql_task = "SELECT id,task_id,trigger_type,started_at,ended_at,status,copied_files,failed_files,copied_bytes,error_message
                            FROM task_runs WHERE task_id=?1 ORDER BY id DESC LIMIT ?2";
            let mut out = Vec::new();
            if let Some(task_id) = query.task_id {
                let mut stmt = conn.prepare(sql_task)?;
                let mut rows = stmt.query(params![task_id, limit as i64])?;
                while let Some(row) = rows.next()? {
                    out.push(RunRecord::from_row(row)?);
                }
            } else {
                let mut stmt = conn.prepare(sql_all)?;
                let mut rows = stmt.query(params![limit as i64])?;
                while let Some(row) = rows.next()? {
                    out.push(RunRecord::from_row(row)?);
                }
            }
            Ok(out)
        })
        .await
        .context("list runs join error")?
    }

    async fn dashboard_from_runs(&self) -> Result<(u64, u64)> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(u64, u64)> {
            let conn = db.open()?;
            let now_local = Local::now();
            let local_day_start = Local
                .with_ymd_and_hms(now_local.year(), now_local.month(), now_local.day(), 0, 0, 0)
                .single()
                .ok_or_else(|| anyhow!("failed to construct local day start"))?;
            let day_start_utc = local_day_start.with_timezone(&Utc).to_rfc3339();

            let bytes: Option<i64> = conn
                .query_row(
                    "SELECT SUM(copied_bytes) FROM task_runs WHERE started_at >= ?1 AND status='success'",
                    params![day_start_utc],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let runs: Option<i64> = conn
                .query_row(
                    "SELECT COUNT(*) FROM task_runs WHERE started_at >= ?1",
                    params![day_start_utc],
                    |r| r.get(0),
                )
                .optional()?;

            Ok((bytes.unwrap_or(0) as u64, runs.unwrap_or(0) as u64))
        })
        .await
        .context("dashboard join error")?
    }
}

#[derive(Clone)]
struct AppState {
    db: Db,
    scheduler: JobScheduler,
    scheduled_jobs: Arc<Mutex<HashMap<i64, Uuid>>>,
    running_tasks: Arc<Mutex<HashSet<i64>>>,
    ws_tx: broadcast::Sender<String>,
    event_tx: UnboundedSender<TaskEventWrite>,
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
            created_at: row.get(15)?,
            updated_at: row.get(16)?,
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
        let config_json = serde_json::from_str(&config_raw).unwrap_or(serde_json::Value::Object(
            serde_json::Map::new(),
        ));
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
        let payload = serde_json::from_str(&payload_raw).unwrap_or_else(|_| json!({ "raw": payload_raw }));
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

    let scheduler = JobScheduler::new().await.context("create scheduler failed")?;
    let (ws_tx, _) = broadcast::channel(1024);
    let (event_tx, event_rx) = unbounded_channel::<TaskEventWrite>();

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
        .route("/api/remotes", get(api_list_remotes).post(api_create_remote))
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
                if let Err(err) = db_for_cleanup.cleanup_task_events(event_cleanup_policy).await {
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
    axum::serve(listener, app).await.context("web server failed")?;
    Ok(())
}

async fn api_health() -> Json<serde_json::Value> {
    Json(json!({ "ok": true, "time": Utc::now().to_rfc3339() }))
}

async fn api_list_remotes(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<RemoteRecord>>, ApiError> {
    Ok(Json(state.db.list_remotes().await?))
}

async fn api_create_remote(
    State(state): State<Arc<AppState>>,
    Json(mut req): Json<CreateRemoteRequest>,
) -> Result<(StatusCode, Json<RemoteRecord>), ApiError> {
    normalize_create_remote_request(&mut req)?;
    let remote = state.db.create_remote(req).await?;
    Ok((StatusCode::CREATED, Json(remote)))
}

async fn api_update_remote(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(mut req): Json<UpdateRemoteRequest>,
) -> Result<Json<RemoteRecord>, ApiError> {
    normalize_update_remote_request(&mut req)?;
    let remote = state
        .db
        .update_remote(&name, req)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("remote `{name}` not found")))?;
    Ok(Json(remote))
}

async fn api_delete_remote(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let removed = state.db.delete_remote(&name).await?;
    if !removed {
        return Err(ApiError::not_found(format!("remote `{name}` not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn api_list_tasks(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<TaskListItem>>, ApiError> {
    let tasks = state.db.list_tasks().await?;
    let latest_status = state.db.list_latest_task_status().await?;
    let running = state.running_tasks.lock().await.clone();
    let out = tasks
        .into_iter()
        .map(|task| {
            let status = if running.contains(&task.id) {
                "running".to_string()
            } else {
                latest_status
                    .get(&task.id)
                    .cloned()
                    .unwrap_or_else(|| "idle".to_string())
            };
            TaskListItem { task, status }
        })
        .collect();
    Ok(Json(out))
}

async fn api_create_task(
    State(state): State<Arc<AppState>>,
    Json(mut req): Json<CreateTaskRequest>,
) -> Result<(StatusCode, Json<TaskRecord>), ApiError> {
    normalize_task_request(&mut req)?;
    let created = state.db.create_task(req).await?;
    sync_task_schedule(state.clone(), &created).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn api_update_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(mut req): Json<UpdateTaskRequest>,
) -> Result<Json<TaskRecord>, ApiError> {
    normalize_task_update_request(&mut req)?;
    let task = state
        .db
        .update_task(id, req)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("task {id} not found")))?;
    sync_task_schedule(state.clone(), &task).await?;
    Ok(Json(task))
}

async fn api_delete_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    unschedule_task(state.clone(), id).await?;
    let removed = state.db.delete_task(id).await?;
    if !removed {
        return Err(ApiError::not_found(format!("task {id} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn api_pause_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<TaskRecord>, ApiError> {
    let task = state
        .db
        .set_task_enabled(id, false)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("task {id} not found")))?;
    unschedule_task(state, id).await?;
    Ok(Json(task))
}

async fn api_resume_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<TaskRecord>, ApiError> {
    let task = state
        .db
        .set_task_enabled(id, true)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("task {id} not found")))?;
    sync_task_schedule(state.clone(), &task).await?;
    Ok(Json(task))
}

async fn api_run_task_now(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_task_by_id(state_clone, id, "manual").await {
            error!(task_id = id, error = %err, "manual run failed");
        }
    });
    Ok(StatusCode::ACCEPTED)
}

async fn api_list_runs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<Vec<RunRecord>>, ApiError> {
    Ok(Json(state.db.list_runs(query).await?))
}

async fn api_list_events(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListTaskEventsQuery>,
) -> Result<Json<Vec<TaskEventRecord>>, ApiError> {
    if query.task_id.is_none() && query.run_id.is_none() {
        return Err(ApiError::bad_request("task_id or run_id is required"));
    }
    Ok(Json(state.db.list_task_events(query).await?))
}

async fn api_dashboard(State(state): State<Arc<AppState>>) -> Result<Json<DashboardResponse>, ApiError> {
    let running_tasks = state.running_tasks.lock().await.len();
    let (today_bytes, today_runs) = state.db.dashboard_from_runs().await?;
    let (cpu_usage_percent, memory_used_bytes, memory_total_bytes) = {
        let mut sys = state.system.lock().await;
        sys.refresh_cpu();
        sys.refresh_memory();
        (
            sys.global_cpu_info().cpu_usage(),
            sys.used_memory(),
            sys.total_memory(),
        )
    };
    Ok(Json(DashboardResponse {
        running_tasks,
        today_transferred_bytes: today_bytes,
        today_runs,
        cpu_usage_percent,
        memory_used_bytes,
        memory_total_bytes,
    }))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_client(socket, state))
}

async fn ws_client(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.ws_tx.subscribe();
    while let Ok(payload) = rx.recv().await {
        if socket.send(Message::Text(payload)).await.is_err() {
            break;
        }
    }
}

async fn index_handler() -> Html<String> {
    match WebAssets::get("index.html") {
        Some(content) => Html(String::from_utf8_lossy(content.data.as_ref()).to_string()),
        None => Html("<h1>index.html not found</h1>".to_string()),
    }
}

async fn load_schedules(state: Arc<AppState>) -> Result<()> {
    let tasks = state.db.list_tasks().await?;
    for task in tasks {
        sync_task_schedule(state.clone(), &task).await?;
    }
    Ok(())
}

async fn sync_task_schedule(state: Arc<AppState>, task: &TaskRecord) -> Result<()> {
    unschedule_task(state.clone(), task.id).await?;
    if !task.enabled {
        return Ok(());
    }
    let cron_expr = match task.cron_expr.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        Some(v) => v.to_string(),
        None => return Ok(()),
    };

    let task_id = task.id;
    let state_for_job = state.clone();
    let job = Job::new_async(cron_expr.as_str(), move |_id, _lock| {
        let state_for_run = state_for_job.clone();
        Box::pin(async move {
            if let Err(err) = run_task_by_id(state_for_run, task_id, "schedule").await {
                error!(task_id = task_id, error = %err, "scheduled run failed");
            }
        })
    })
    .with_context(|| format!("invalid cron expression for task {}: {}", task.id, cron_expr))?;
    let job_id = job.guid();
    state
        .scheduler
        .add(job)
        .await
        .with_context(|| format!("failed adding scheduler job for task {}", task.id))?;
    state.scheduled_jobs.lock().await.insert(task.id, job_id);
    Ok(())
}

async fn unschedule_task(state: Arc<AppState>, task_id: i64) -> Result<()> {
    if let Some(job_id) = state.scheduled_jobs.lock().await.remove(&task_id) {
        if let Err(err) = state.scheduler.remove(&job_id).await {
            warn!(task_id = task_id, error = %err, "failed to remove scheduler job");
        }
    }
    Ok(())
}

async fn run_task_by_id(state: Arc<AppState>, task_id: i64, trigger: &'static str) -> Result<()> {
    let task = state
        .db
        .get_task(task_id)
        .await?
        .ok_or_else(|| anyhow!("task {} not found", task_id))?;
    if !task.enabled && trigger == "schedule" {
        return Ok(());
    }
    execute_task(state, task, trigger).await
}

async fn execute_task(state: Arc<AppState>, task: TaskRecord, trigger: &'static str) -> Result<()> {
    {
        let mut running = state.running_tasks.lock().await;
        if running.contains(&task.id) {
            return Err(anyhow!("task {} is already running", task.id));
        }
        running.insert(task.id);
    }

    let run_id = state.db.create_run(task.id, trigger).await?;
    let started_event = json!({
        "event": "task_started",
        "task_id": task.id,
        "run_id": run_id,
        "name": task.name,
        "trigger": trigger,
        "at": Utc::now().to_rfc3339(),
    });
    broadcast_json(&state.ws_tx, started_event.clone());
    persist_task_event(&state.event_tx, task.id, Some(run_id), started_event);

    let copied_files = Arc::new(AtomicU64::new(0));
    let failed_files = Arc::new(AtomicU64::new(0));
    let copied_bytes = Arc::new(AtomicU64::new(0));
    let last_progress_emit_ms = Arc::new(AtomicU64::new(0));
    let error_records = Arc::new(std::sync::Mutex::new(Vec::<RunErrorRecord>::new()));

    let ws_tx = state.ws_tx.clone();
    let event_tx = state.event_tx.clone();
    let copied_files_ref = copied_files.clone();
    let failed_files_ref = failed_files.clone();
    let copied_bytes_ref = copied_bytes.clone();
    let last_progress_emit_ms_ref = last_progress_emit_ms.clone();
    let errors_ref = error_records.clone();
    let task_id = task.id;
    let run_id_for_event = run_id;

    let reporter: ProgressReporter = Arc::new(move |event| match event {
        TransferEvent::Planned {
            copy_count,
            delete_count,
            total_size,
        } => {
            let event = json!({
                "event": "task_planned",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "copy_count": copy_count,
                "delete_count": delete_count,
                "total_bytes": total_size,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::Copied {
            rel_path,
            size,
            completed_bytes,
            total_bytes,
        } => {
            copied_files_ref.fetch_add(1, Ordering::Relaxed);
            copied_bytes_ref.store(completed_bytes, Ordering::Relaxed);
            let uploaded_event = json!({
                "event": "task_file_uploaded",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "file_bytes": size,
                "completed_bytes": completed_bytes,
                "total_bytes": total_bytes,
                "at": Utc::now().to_rfc3339(),
            });
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), uploaded_event);
            let now_ms = now_unix_millis();
            let last_emit = last_progress_emit_ms_ref.load(Ordering::Relaxed);
            let should_emit = completed_bytes >= total_bytes || now_ms.saturating_sub(last_emit) >= 250;
            if !should_emit {
                return;
            }
            last_progress_emit_ms_ref.store(now_ms, Ordering::Relaxed);
            let progress = if total_bytes == 0 {
                100.0
            } else {
                (completed_bytes as f64 / total_bytes as f64 * 100.0).min(100.0)
            };
            let event = json!({
                "event": "task_progress",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "file_bytes": size,
                "completed_bytes": completed_bytes,
                "total_bytes": total_bytes,
                "progress_pct": progress,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::CopyFailed { rel_path, error } => {
            failed_files_ref.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut guard) = errors_ref.lock() {
                guard.push(RunErrorRecord {
                    path: Some(rel_path.clone()),
                    message: error.clone(),
                    created_at: Utc::now().to_rfc3339(),
                });
            }
            let event = json!({
                "event": "task_error",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "message": error,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::DeleteFailed { rel_path, error } => {
            failed_files_ref.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut guard) = errors_ref.lock() {
                guard.push(RunErrorRecord {
                    path: Some(rel_path.clone()),
                    message: error.clone(),
                    created_at: Utc::now().to_rfc3339(),
                });
            }
            let event = json!({
                "event": "task_error",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "message": error,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::MoveSourceDeleteFailed { rel_path, error } => {
            failed_files_ref.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut guard) = errors_ref.lock() {
                guard.push(RunErrorRecord {
                    path: Some(rel_path.clone()),
                    message: error.clone(),
                    created_at: Utc::now().to_rfc3339(),
                });
            }
            let event = json!({
                "event": "task_error",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "message": error,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::Finished { failures } => {
            let event = json!({
                "event": "task_finished_signal",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "failures": failures,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::Deleted { rel_path } => {
            let event = json!({
                "event": "task_delete",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
        TransferEvent::MoveSourceDeleted { rel_path } => {
            let event = json!({
                "event": "task_move_source_deleted",
                "task_id": task_id,
                "run_id": run_id_for_event,
                "path": rel_path,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&ws_tx, event.clone());
            persist_task_event(&event_tx, task_id, Some(run_id_for_event), event);
        }
    });

    let result = async {
        let cfg = state
            .db
            .load_app_config()
            .await
            .context("failed to load app config from sqlite/file")?;
        let source = Location::parse(&task.source)
            .with_context(|| format!("invalid source location: {}", task.source))?;
        let destination = Location::parse(&task.destination)
            .with_context(|| format!("invalid destination location: {}", task.destination))?;
        let (source, source_op) = resolve_location_and_operator(&cfg, source)?;
        let (destination, target_op) = resolve_location_and_operator(&cfg, destination)?;
        let mode = parse_mode(&task.mode)?;
        let options = SyncOptions {
            dry_run: task.dry_run,
            transfers: task.transfers.max(1) as usize,
            checkers: task.checkers.max(1) as usize,
            checksum: task.checksum,
            ignore_existing: task.ignore_existing,
            include: task.include.clone(),
            exclude: task.exclude.clone(),
            bandwidth_limit: crate::engine::parse_bandwidth_limit(task.bandwidth_limit.as_deref())?,
        };
        run_transfer_with_reporter(
            mode,
            source_op,
            source,
            target_op,
            destination,
            options,
            Some(reporter),
        )
        .await
    }
    .await;

    let copied_files_val = copied_files.load(Ordering::Relaxed);
    let failed_files_val = failed_files.load(Ordering::Relaxed);
    let copied_bytes_val = copied_bytes.load(Ordering::Relaxed);
    let error_records_vec = error_records
        .lock()
        .map(|g| g.clone())
        .unwrap_or_else(|_| Vec::new());

    match &result {
        Ok(_) => {
            state
                .db
                .finish_run(
                    run_id,
                    "success",
                    copied_files_val,
                    failed_files_val,
                    copied_bytes_val,
                    None,
                )
                .await?;
            state.db.insert_run_errors(run_id, error_records_vec).await?;
            let event = json!({
                "event": "task_completed",
                "task_id": task.id,
                "run_id": run_id,
                "status": "success",
                "copied_files": copied_files_val,
                "failed_files": failed_files_val,
                "copied_bytes": copied_bytes_val,
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&state.ws_tx, event.clone());
            persist_task_event(&state.event_tx, task.id, Some(run_id), event);
        }
        Err(err) => {
            state
                .db
                .finish_run(
                    run_id,
                    "failed",
                    copied_files_val,
                    failed_files_val,
                    copied_bytes_val,
                    Some(err.to_string()),
                )
                .await?;
            state.db.insert_run_errors(run_id, error_records_vec).await?;
            let event = json!({
                "event": "task_completed",
                "task_id": task.id,
                "run_id": run_id,
                "status": "failed",
                "copied_files": copied_files_val,
                "failed_files": failed_files_val,
                "copied_bytes": copied_bytes_val,
                "error": err.to_string(),
                "at": Utc::now().to_rfc3339(),
            });
            broadcast_json(&state.ws_tx, event.clone());
            persist_task_event(&state.event_tx, task.id, Some(run_id), event);
        }
    }

    state.running_tasks.lock().await.remove(&task.id);
    result
}

fn parse_mode(mode: &str) -> Result<SyncMode> {
    match mode.to_ascii_lowercase().as_str() {
        "copy" => Ok(SyncMode::Copy),
        "sync" => Ok(SyncMode::Sync),
        "move" => Ok(SyncMode::Move),
        other => Err(anyhow!("invalid mode: {other}, expected copy|sync|move")),
    }
}

fn normalize_task_request(req: &mut CreateTaskRequest) -> Result<(), ApiError> {
    req.name = req.name.trim().to_string();
    req.source = req.source.trim().to_string();
    req.destination = req.destination.trim().to_string();
    req.mode = req.mode.trim().to_ascii_lowercase();
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
    if !matches!(req.r#type.as_str(), "fs" | "s3" | "ftp" | "sftp") {
        return Err(ApiError::bad_request(
            "remote type must be one of: fs|s3|ftp|sftp",
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
    if !matches!(req.r#type.as_str(), "fs" | "s3" | "ftp" | "sftp") {
        return Err(ApiError::bad_request(
            "remote type must be one of: fs|s3|ftp|sftp",
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
        _ => return Err(anyhow!("remote `{}` config_json must be object", remote.name)),
    };
    raw.insert(
        "type".to_string(),
        serde_json::Value::String(remote.r#type.clone()),
    );
    let value = serde_json::Value::Object(raw);
    serde_json::from_value(value)
        .with_context(|| format!("invalid remote config for `{}`", remote.name))
}

fn broadcast_json(sender: &broadcast::Sender<String>, value: serde_json::Value) {
    let _ = sender.send(value.to_string());
}

fn persist_task_event(
    sender: &UnboundedSender<TaskEventWrite>,
    task_id: i64,
    run_id: Option<i64>,
    value: serde_json::Value,
) {
    let event_type = value
        .get("event")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let created_at = value
        .get("at")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| "")
        .to_string();
    let payload = value.to_string();
    let record = TaskEventWrite {
        task_id,
        run_id,
        event_type,
        payload,
        created_at: if created_at.is_empty() {
            Utc::now().to_rfc3339()
        } else {
            created_at
        },
    };
    if sender.send(record).is_err() {
        warn!(task_id = task_id, run_id = ?run_id, "persist task event dropped: writer channel closed");
    }
}

async fn task_event_writer_worker(db: Db, mut rx: UnboundedReceiver<TaskEventWrite>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while batch.len() < 500 {
            match rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        if let Err(err) = persist_task_event_batch_with_retry(&db, batch).await {
            warn!(error = %err, "persist task event batch failed");
        }
    }
}

async fn persist_task_event_batch_with_retry(db: &Db, batch: Vec<TaskEventWrite>) -> Result<()> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=4 {
        match db.insert_task_events_batch(batch.clone()).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_error = Some(err);
                if attempt < 4 {
                    tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64)).await;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("unknown task event persistence failure")))
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
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

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

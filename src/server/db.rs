use super::*;
impl Db {
    pub(super) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(super) fn open(&self) -> Result<Connection> {
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

    pub(super) async fn init(&self) -> Result<()> {
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
                    chunk_size TEXT NOT NULL DEFAULT '8MB',
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
            ensure_tasks_chunk_size_column(&conn)?;
            Ok(())
        })
        .await
        .context("sqlite init join error")?
    }

    pub(super) async fn list_remotes(&self) -> Result<Vec<RemoteRecord>> {
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

    pub(super) async fn create_remote(&self, req: CreateRemoteRequest) -> Result<RemoteRecord> {
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

    pub(super) async fn update_remote(
        &self,
        name: &str,
        req: UpdateRemoteRequest,
    ) -> Result<Option<RemoteRecord>> {
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

    pub(super) async fn delete_remote(&self, name: &str) -> Result<bool> {
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

    pub(super) async fn load_app_config(&self) -> Result<AppConfig> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<AppConfig> {
            let mut cfg = AppConfig::load().context("failed to load config file")?;
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

    pub(super) async fn list_tasks(&self) -> Result<Vec<TaskRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<TaskRecord>> {
            let conn = db.open()?;
            let mut stmt = conn.prepare(
                "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,chunk_size,created_at,updated_at
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

    pub(super) async fn list_latest_task_status(&self) -> Result<HashMap<i64, String>> {
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

    pub(super) async fn get_task(&self, id: i64) -> Result<Option<TaskRecord>> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<TaskRecord>> {
            let conn = db.open()?;
            let task = conn
                .query_row(
                    "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,chunk_size,created_at,updated_at
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

    pub(super) async fn create_task(&self, req: CreateTaskRequest) -> Result<TaskRecord> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<TaskRecord> {
            let conn = db.open()?;
            let now = Utc::now().to_rfc3339();
            let includes = serde_json::to_string(&req.include).context("serialize include")?;
            let excludes = serde_json::to_string(&req.exclude).context("serialize exclude")?;
            conn.execute(
                "INSERT INTO tasks (name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,chunk_size,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
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
                    req.chunk_size,
                    now,
                    now
                ],
            )?;
            let id = conn.last_insert_rowid();
            conn.query_row(
                "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,chunk_size,created_at,updated_at
                 FROM tasks WHERE id=?1",
                params![id],
                TaskRecord::from_row,
            )
            .context("read inserted task")
        })
        .await
        .context("create task join error")?
    }

    pub(super) async fn update_task(
        &self,
        id: i64,
        req: UpdateTaskRequest,
    ) -> Result<Option<TaskRecord>> {
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
                "UPDATE tasks SET name=?1,source=?2,destination=?3,mode=?4,cron_expr=?5,enabled=?6,transfers=?7,checkers=?8,checksum=?9,ignore_existing=?10,dry_run=?11,include_patterns=?12,exclude_patterns=?13,bandwidth_limit=?14,chunk_size=?15,updated_at=?16
                 WHERE id=?17",
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
                    req.chunk_size,
                    now,
                    id
                ],
            )?;
            let task = conn
                .query_row(
                    "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,chunk_size,created_at,updated_at
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

    pub(super) async fn set_task_enabled(
        &self,
        id: i64,
        enabled: bool,
    ) -> Result<Option<TaskRecord>> {
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
                    "SELECT id,name,source,destination,mode,cron_expr,enabled,transfers,checkers,checksum,ignore_existing,dry_run,include_patterns,exclude_patterns,bandwidth_limit,chunk_size,created_at,updated_at
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

    pub(super) async fn delete_task(&self, id: i64) -> Result<bool> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let conn = db.open()?;
            let affected = conn.execute("DELETE FROM tasks WHERE id=?1", params![id])?;
            Ok(affected > 0)
        })
        .await
        .context("delete task join error")?
    }

    pub(super) async fn create_run(&self, task_id: i64, trigger: &str) -> Result<i64> {
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

    pub(super) async fn finish_run(
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

    pub(super) async fn insert_run_errors(
        &self,
        run_id: i64,
        records: Vec<RunErrorRecord>,
    ) -> Result<()> {
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

    pub(super) async fn insert_task_events_batch(
        &self,
        records: Vec<TaskEventWrite>,
    ) -> Result<()> {
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

    pub(super) async fn list_task_events(
        &self,
        query: ListTaskEventsQuery,
    ) -> Result<Vec<TaskEventRecord>> {
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

    pub(super) async fn cleanup_task_events(&self, policy: EventCleanupPolicy) -> Result<()> {
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

    pub(super) async fn list_runs(&self, query: ListRunsQuery) -> Result<Vec<RunRecord>> {
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

    pub(super) async fn dashboard_from_runs(&self) -> Result<(u64, u64)> {
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

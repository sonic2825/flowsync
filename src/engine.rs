use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use globset::{Glob, GlobSet, GlobSetBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use opendal::{EntryMode, Operator};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::remote::Location;

pub const DEFAULT_STREAM_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const ACTION_PLAN_BATCH_SIZE: usize = 2_000;
const TARGET_INDEX_BATCH_SIZE: usize = 2_000;
const SQLITE_MAX_VARS: usize = 900;

#[derive(Debug, Clone)]
pub enum SyncMode {
    Copy,
    Sync,
    Move,
}

#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub dry_run: bool,
    pub transfers: usize,
    pub checkers: usize,
    pub checksum: bool,
    pub ignore_existing: bool,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub bandwidth_limit: Option<u64>,
    pub stream_chunk_size: usize,
}

#[derive(Debug, Clone)]
pub enum TransferEvent {
    Planned {
        copy_count: usize,
        delete_count: usize,
        total_size: u64,
    },
    Copied {
        rel_path: String,
        size: u64,
        completed_bytes: u64,
        total_bytes: u64,
    },
    CopyFailed {
        rel_path: String,
        error: String,
    },
    Deleted {
        rel_path: String,
    },
    DeleteFailed {
        rel_path: String,
        error: String,
    },
    MoveSourceDeleted {
        rel_path: String,
    },
    MoveSourceDeleteFailed {
        rel_path: String,
        error: String,
    },
    Finished {
        failures: usize,
    },
}

pub type ProgressReporter = Arc<dyn Fn(TransferEvent) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub rel_path: String,
    pub size: u64,
    pub mtime_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Action {
    rel_path: String,
    size: u64,
}

struct CompareContext<'a> {
    source_op: &'a Operator,
    source_base: &'a str,
    target_op: &'a Operator,
    target_base: &'a str,
    stream_chunk_size: usize,
}

struct BandwidthLimiter {
    bytes_per_sec: u64,
    state: tokio::sync::Mutex<BandwidthLimiterState>,
}

struct BandwidthLimiterState {
    available: f64,
    last_refill: Instant,
}

impl BandwidthLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            state: tokio::sync::Mutex::new(BandwidthLimiterState {
                available: bytes_per_sec as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn acquire(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.available = (state.available + elapsed * self.bytes_per_sec as f64)
                    .min(self.bytes_per_sec as f64);
                state.last_refill = now;

                let required = bytes as f64;
                if state.available >= required {
                    state.available -= required;
                    None
                } else {
                    let missing = required - state.available;
                    state.available = 0.0;
                    Some(Duration::from_secs_f64(missing / self.bytes_per_sec as f64))
                }
            };

            if let Some(wait) = wait {
                sleep(wait).await;
            } else {
                break;
            }
        }
    }
}

pub async fn list(op: &Operator, path: &str) -> Result<()> {
    let base = normalize_list_base(path);
    let mut lister = op.lister_with(&base).recursive(true).await?;
    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().mode() == EntryMode::FILE {
            println!("{}", entry.path());
        }
    }
    Ok(())
}

pub async fn run_transfer(
    mode: SyncMode,
    source_op: Operator,
    source: Location,
    target_op: Operator,
    target: Location,
    options: SyncOptions,
) -> Result<()> {
    run_transfer_with_reporter(mode, source_op, source, target_op, target, options, None).await
}

pub async fn run_transfer_with_reporter(
    mode: SyncMode,
    source_op: Operator,
    source: Location,
    target_op: Operator,
    target: Location,
    options: SyncOptions,
    reporter: Option<ProgressReporter>,
) -> Result<()> {
    let filter = build_filter(&options.include, &options.exclude)?;

    let target_index_path = build_target_index_path();
    let target_index = TargetIndex::create(target_index_path.clone()).await?;

    info!("scanning target files");
    if let Err(err) = populate_target_index(&target_index, &target_op, &target.path, &filter).await
    {
        warn!(error = %err, "scan target files failed, continue with empty target index");
    }

    let plan_path = build_plan_file_path();
    let result = async {
        info!("planning source actions");
        let mut plan_file = tokio::fs::File::create(&plan_path)
            .await
            .with_context(|| format!("failed to create action plan file: {}", plan_path.display()))?;

        let mut planned_copy_count = 0usize;
        let mut total_size = 0u64;
        let mut source_batch: Vec<FileEntry> = Vec::with_capacity(ACTION_PLAN_BATCH_SIZE);
        let source_base = normalize_list_base(&source.path);
        let mut source_lister = source_op.lister_with(&source_base).recursive(true).await?;

        while let Some(entry) = source_lister.try_next().await? {
            if entry.metadata().mode() != EntryMode::FILE {
                continue;
            }

            let full = entry.path().trim_start_matches('/').to_string();
            let rel = diff_rel(&source.path, &full);
            if !filter.hit(&rel) {
                continue;
            }

            let m = entry.metadata();
            let file = FileEntry {
                rel_path: rel,
                size: m.content_length(),
                mtime_ms: m
                    .last_modified()
                    .map(|ts| ts.into_inner().as_millisecond())
                    .unwrap_or_default(),
            };
            source_batch.push(file);

            if source_batch.len() >= ACTION_PLAN_BATCH_SIZE {
                if matches!(mode, SyncMode::Sync) {
                    let rel_paths = source_batch
                        .iter()
                        .map(|entry| entry.rel_path.clone())
                        .collect::<Vec<_>>();
                    target_index.remove_paths(&rel_paths).await?;
                }
                let actions = plan_copy_actions_batch(
                    source_batch.split_off(0),
                    &source_op,
                    &source.path,
                    &target_op,
                    &target.path,
                    &target_index,
                    &options,
                )
                .await?;
                planned_copy_count += actions.len();
                total_size += actions.iter().map(|a| a.size).sum::<u64>();
                write_plan_actions(&mut plan_file, &actions).await?;
            }
        }

        if !source_batch.is_empty() {
            if matches!(mode, SyncMode::Sync) {
                let rel_paths = source_batch
                    .iter()
                    .map(|entry| entry.rel_path.clone())
                    .collect::<Vec<_>>();
                target_index.remove_paths(&rel_paths).await?;
            }
            let actions = plan_copy_actions_batch(
                source_batch,
                &source_op,
                &source.path,
                &target_op,
                &target.path,
                &target_index,
                &options,
            )
            .await?;
            planned_copy_count += actions.len();
            total_size += actions.iter().map(|a| a.size).sum::<u64>();
            write_plan_actions(&mut plan_file, &actions).await?;
        }
        plan_file
            .flush()
            .await
            .context("failed to flush action plan file")?;
        drop(plan_file);

        let delete_count: usize = if matches!(mode, SyncMode::Sync) {
            target_index.remaining_count().await?
        } else {
            0
        };

        info!(
            copy_count = planned_copy_count,
            delete_count = delete_count,
            "planned actions"
        );
        emit(
            &reporter,
            TransferEvent::Planned {
                copy_count: planned_copy_count,
                delete_count,
                total_size,
            },
        );

        if options.dry_run {
            for_each_plan_action_batch(&plan_path, ACTION_PLAN_BATCH_SIZE, |batch| async move {
                for action in batch {
                    println!("[DRY-RUN] copy {}", action.rel_path);
                }
                Ok(())
            })
            .await?;
            if matches!(mode, SyncMode::Sync) {
                target_index
                    .for_each_remaining_batch(ACTION_PLAN_BATCH_SIZE, |batch| async move {
                        for rel_path in batch {
                            println!("[DRY-RUN] delete {}", rel_path);
                        }
                        Ok(())
                    })
                    .await?;
            }
            return Ok(());
        }

        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})",
            )?
            .progress_chars("=>-"),
        );

        let failures = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let copied_bytes = Arc::new(AtomicU64::new(0));
        let bandwidth_limiter = options
            .bandwidth_limit
            .filter(|limit| *limit > 0)
            .map(BandwidthLimiter::new)
            .map(Arc::new);

        for_each_plan_action_batch(&plan_path, ACTION_PLAN_BATCH_SIZE, |batch| {
            execute_copy_batch(
                batch,
                &source_op,
                &source.path,
                &target_op,
                &target.path,
                &options,
                total_size,
                bandwidth_limiter.clone(),
                failures.clone(),
                copied_bytes.clone(),
                pb.clone(),
                reporter.clone(),
            )
        })
        .await?;

        pb.finish_with_message("copy done");

        if matches!(mode, SyncMode::Sync) {
            target_index
                .for_each_remaining_batch(ACTION_PLAN_BATCH_SIZE, |batch| {
                    let target = target.clone();
                    let target_op = target_op.clone();
                    let failures = failures.clone();
                    let reporter = reporter.clone();
                    async move {
                        for rel_path in batch {
                            let full_path = join_path(&target.path, &rel_path);
                            if let Err(err) =
                                retry_async(|| async { target_op.delete(&full_path).await }, 3).await
                            {
                                warn!(path = %rel_path, error = %err, "delete failed");
                                failures
                                    .lock()
                                    .await
                                    .push(format!("delete {}: {}", rel_path, err));
                                emit(
                                    &reporter,
                                    TransferEvent::DeleteFailed {
                                        rel_path: rel_path.clone(),
                                        error: err.to_string(),
                                    },
                                );
                            } else {
                                emit(
                                    &reporter,
                                    TransferEvent::Deleted {
                                        rel_path: rel_path.clone(),
                                    },
                                );
                            }
                        }
                        Ok(())
                    }
                })
                .await?;
        }

        if matches!(mode, SyncMode::Move) {
            for_each_plan_action_batch(&plan_path, ACTION_PLAN_BATCH_SIZE, |batch| {
                let source_op = source_op.clone();
                let source_path = source.path.clone();
                let failures = failures.clone();
                let reporter = reporter.clone();
                async move {
                    for action in batch {
                        let full_path = join_path(&source_path, &action.rel_path);
                        if let Err(err) =
                            retry_async(|| async { source_op.delete(&full_path).await }, 3).await
                        {
                            warn!(path = %action.rel_path, error = %err, "move delete source failed");
                            failures
                                .lock()
                                .await
                                .push(format!("move delete {}: {}", action.rel_path, err));
                            emit(
                                &reporter,
                                TransferEvent::MoveSourceDeleteFailed {
                                    rel_path: action.rel_path.clone(),
                                    error: err.to_string(),
                                },
                            );
                        } else {
                            emit(
                                &reporter,
                                TransferEvent::MoveSourceDeleted {
                                    rel_path: action.rel_path.clone(),
                                },
                            );
                        }
                    }
                    Ok(())
                }
            })
            .await?;
        }

        let failures = failures.lock().await;
        emit(
            &reporter,
            TransferEvent::Finished {
                failures: failures.len(),
            },
        );
        if !failures.is_empty() {
            let report = failures.join("\n");
            tokio::fs::write("transfer_failures.log", report)
                .await
                .context("failed to write transfer_failures.log")?;
            return Err(anyhow!(
                "transfer completed with {} failures, see transfer_failures.log",
                failures.len()
            ));
        }

        Ok(())
    }
    .await;

    if let Err(err) = tokio::fs::remove_file(&plan_path).await {
        warn!(path = %plan_path.display(), error = %err, "failed to remove action plan file");
    }
    if let Err(err) = tokio::fs::remove_file(&target_index_path).await {
        warn!(path = %target_index_path.display(), error = %err, "failed to remove target index file");
    }

    result
}

fn emit(reporter: &Option<ProgressReporter>, event: TransferEvent) {
    if let Some(reporter) = reporter {
        reporter(event);
    }
}

fn build_filter(include: &[String], exclude: &[String]) -> Result<Filter> {
    let mut inc_builder = GlobSetBuilder::new();
    if include.is_empty() {
        inc_builder.add(Glob::new("**")?);
    } else {
        for pattern in include {
            inc_builder.add(Glob::new(pattern)?);
        }
    }

    let mut exc_builder = GlobSetBuilder::new();
    for pattern in exclude {
        exc_builder.add(Glob::new(pattern)?);
    }

    Ok(Filter {
        includes: inc_builder.build()?,
        excludes: exc_builder.build()?,
    })
}

struct Filter {
    includes: GlobSet,
    excludes: GlobSet,
}

impl Filter {
    fn hit(&self, rel_path: &str) -> bool {
        self.includes.is_match(rel_path) && !self.excludes.is_match(rel_path)
    }
}

#[derive(Clone)]
struct TargetIndex {
    path: PathBuf,
}

impl TargetIndex {
    async fn create(path: PathBuf) -> Result<Self> {
        let index = Self { path };
        let db_path = index.path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(&db_path).with_context(|| {
                format!("failed to open target index db: {}", db_path.display())
            })?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 CREATE TABLE IF NOT EXISTS target_index (
                    rel_path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    mtime_ms INTEGER NOT NULL
                 );",
            )
            .context("failed to initialize target index schema")?;
            Ok(())
        })
        .await
        .context("target index init join error")??;
        Ok(index)
    }

    async fn insert_batch(&self, entries: Vec<FileEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let db_path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(&db_path)
                .with_context(|| format!("failed to open target index db: {}", db_path.display()))?;
            let tx = conn.transaction()?;
            {
                let mut stmt = tx.prepare(
                    "INSERT OR REPLACE INTO target_index (rel_path,size,mtime_ms) VALUES (?1,?2,?3)",
                )?;
                for entry in entries {
                    stmt.execute(params![entry.rel_path, entry.size as i64, entry.mtime_ms])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .context("target index insert join error")?
    }

    async fn get_many(&self, rel_paths: &[String]) -> Result<HashMap<String, FileEntry>> {
        if rel_paths.is_empty() {
            return Ok(HashMap::new());
        }
        let db_path = self.path.clone();
        let rel_paths = rel_paths.to_vec();
        tokio::task::spawn_blocking(move || -> Result<HashMap<String, FileEntry>> {
            let conn = Connection::open(&db_path).with_context(|| {
                format!("failed to open target index db: {}", db_path.display())
            })?;
            let mut out = HashMap::new();
            for chunk in rel_paths.chunks(SQLITE_MAX_VARS) {
                let placeholders = std::iter::repeat_n("?", chunk.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "SELECT rel_path,size,mtime_ms FROM target_index WHERE rel_path IN ({placeholders})"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok(FileEntry {
                        rel_path: row.get::<_, String>(0)?,
                        size: row.get::<_, i64>(1)?.max(0) as u64,
                        mtime_ms: row.get::<_, i64>(2)?,
                    })
                })?;
                for row in rows {
                    let entry = row?;
                    out.insert(entry.rel_path.clone(), entry);
                }
            }
            Ok(out)
        })
        .await
        .context("target index get_many join error")?
    }

    async fn remove_paths(&self, rel_paths: &[String]) -> Result<()> {
        if rel_paths.is_empty() {
            return Ok(());
        }
        let db_path = self.path.clone();
        let rel_paths = rel_paths.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = Connection::open(&db_path).with_context(|| {
                format!("failed to open target index db: {}", db_path.display())
            })?;
            let tx = conn.transaction()?;
            for chunk in rel_paths.chunks(SQLITE_MAX_VARS) {
                let placeholders = std::iter::repeat_n("?", chunk.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!("DELETE FROM target_index WHERE rel_path IN ({placeholders})");
                tx.execute(&sql, rusqlite::params_from_iter(chunk.iter()))?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .context("target index remove paths join error")?
    }

    async fn remaining_count(&self) -> Result<usize> {
        let db_path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<usize> {
            let conn = Connection::open(&db_path).with_context(|| {
                format!("failed to open target index db: {}", db_path.display())
            })?;
            let count: i64 =
                conn.query_row("SELECT COUNT(1) FROM target_index", [], |row| row.get(0))?;
            Ok(count.max(0) as usize)
        })
        .await
        .context("target index count join error")?
    }

    async fn fetch_remaining_batch(
        &self,
        after_rowid: i64,
        limit: usize,
    ) -> Result<Vec<(i64, String)>> {
        let db_path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<(i64, String)>> {
            let conn = Connection::open(&db_path).with_context(|| {
                format!("failed to open target index db: {}", db_path.display())
            })?;
            let mut stmt = conn.prepare(
                "SELECT rowid, rel_path FROM target_index WHERE rowid > ?1 ORDER BY rowid LIMIT ?2",
            )?;
            let mut rows = stmt.query(params![after_rowid, limit as i64])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push((row.get::<_, i64>(0)?, row.get::<_, String>(1)?));
            }
            Ok(out)
        })
        .await
        .context("target index fetch batch join error")?
    }

    async fn for_each_remaining_batch<F, Fut>(
        &self,
        batch_size: usize,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<String>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut after_rowid = 0i64;
        loop {
            let rows = self
                .fetch_remaining_batch(after_rowid, batch_size.max(1))
                .await?;
            if rows.is_empty() {
                break;
            }
            after_rowid = rows.last().map(|(rowid, _)| *rowid).unwrap_or(after_rowid);
            let paths = rows.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
            handler(paths).await?;
        }
        Ok(())
    }
}

fn build_plan_file_path() -> std::path::PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("flowsync-plan-{}-{now}.jsonl", std::process::id()))
}

fn build_target_index_path() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "flowsync-target-index-{}-{now}.sqlite3",
        std::process::id()
    ))
}

async fn populate_target_index(
    index: &TargetIndex,
    op: &Operator,
    base_path: &str,
    filter: &Filter,
) -> Result<()> {
    let mut batch = Vec::with_capacity(TARGET_INDEX_BATCH_SIZE);
    let base = normalize_list_base(base_path);
    let mut lister = op.lister_with(&base).recursive(true).await?;

    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().mode() != EntryMode::FILE {
            continue;
        }

        let full = entry.path().trim_start_matches('/').to_string();
        let rel = diff_rel(base_path, &full);
        if !filter.hit(&rel) {
            continue;
        }

        let m = entry.metadata();
        batch.push(FileEntry {
            rel_path: rel,
            size: m.content_length(),
            mtime_ms: m
                .last_modified()
                .map(|ts| ts.into_inner().as_millisecond())
                .unwrap_or_default(),
        });

        if batch.len() >= TARGET_INDEX_BATCH_SIZE {
            index.insert_batch(std::mem::take(&mut batch)).await?;
        }
    }

    if !batch.is_empty() {
        index.insert_batch(batch).await?;
    }
    Ok(())
}

async fn plan_copy_actions_batch(
    source_batch: Vec<FileEntry>,
    source_op: &Operator,
    source_base: &str,
    target_op: &Operator,
    target_base: &str,
    target_index: &TargetIndex,
    options: &SyncOptions,
) -> Result<Vec<Action>> {
    let rel_paths = source_batch
        .iter()
        .map(|entry| entry.rel_path.clone())
        .collect::<Vec<_>>();
    let target_map = Arc::new(target_index.get_many(&rel_paths).await?);

    stream::iter(source_batch)
        .map(|source_file| {
            let source_op = source_op.clone();
            let target_op = target_op.clone();
            let source_base = source_base.to_string();
            let target_base = target_base.to_string();
            let target_map = Arc::clone(&target_map);
            let options = options.clone();

            async move {
                let compare_ctx = CompareContext {
                    source_op: &source_op,
                    source_base: &source_base,
                    target_op: &target_op,
                    target_base: &target_base,
                    stream_chunk_size: options.stream_chunk_size,
                };
                let should_copy = should_copy(
                    &source_file,
                    target_map.get(&source_file.rel_path),
                    &compare_ctx,
                    &options,
                )
                .await?;

                Ok::<Option<Action>, anyhow::Error>(if should_copy {
                    Some(Action {
                        rel_path: source_file.rel_path,
                        size: source_file.size,
                    })
                } else {
                    None
                })
            }
        })
        .buffer_unordered(options.checkers.max(1))
        .try_filter_map(|action| async move { Ok(action) })
        .try_collect()
        .await
}

async fn write_plan_actions(plan_file: &mut tokio::fs::File, actions: &[Action]) -> Result<()> {
    for action in actions {
        let line = serde_json::to_string(action).context("failed to encode plan action")?;
        plan_file
            .write_all(line.as_bytes())
            .await
            .context("failed to write plan action")?;
        plan_file
            .write_all(b"\n")
            .await
            .context("failed to write plan newline")?;
    }
    Ok(())
}

async fn for_each_plan_action_batch<F, Fut>(
    plan_path: &Path,
    batch_size: usize,
    mut handler: F,
) -> Result<()>
where
    F: FnMut(Vec<Action>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let file = tokio::fs::File::open(plan_path)
        .await
        .with_context(|| format!("failed to open action plan: {}", plan_path.display()))?;
    let mut lines = BufReader::new(file).lines();
    let mut batch: Vec<Action> = Vec::with_capacity(batch_size.max(1));

    while let Some(line) = lines.next_line().await? {
        let action: Action =
            serde_json::from_str(&line).with_context(|| format!("invalid action line: {line}"))?;
        batch.push(action);
        if batch.len() >= batch_size.max(1) {
            handler(std::mem::take(&mut batch)).await?;
        }
    }

    if !batch.is_empty() {
        handler(batch).await?;
    }
    Ok(())
}

async fn execute_copy_batch(
    actions: Vec<Action>,
    source_op: &Operator,
    source_base: &str,
    target_op: &Operator,
    target_base: &str,
    options: &SyncOptions,
    total_size: u64,
    bandwidth_limiter: Option<Arc<BandwidthLimiter>>,
    failures: Arc<tokio::sync::Mutex<Vec<String>>>,
    copied_bytes: Arc<AtomicU64>,
    pb: ProgressBar,
    reporter: Option<ProgressReporter>,
) -> Result<()> {
    stream::iter(actions)
        .for_each_concurrent(options.transfers.max(1), |action| {
            let source_op = source_op.clone();
            let source_base = source_base.to_string();
            let target_op = target_op.clone();
            let target_base = target_base.to_string();
            let failures = failures.clone();
            let copied_bytes = copied_bytes.clone();
            let pb = pb.clone();
            let reporter = reporter.clone();
            let bandwidth_limiter = bandwidth_limiter.clone();
            let stream_chunk_size = options.stream_chunk_size;

            async move {
                if let Err(err) = copy_one(
                    &source_op,
                    &source_base,
                    &target_op,
                    &target_base,
                    &action.rel_path,
                    bandwidth_limiter.as_deref(),
                    stream_chunk_size,
                )
                .await
                {
                    error!(path = %action.rel_path, error = %err, "copy failed");
                    failures
                        .lock()
                        .await
                        .push(format!("{}: {}", action.rel_path, err));
                    emit(
                        &reporter,
                        TransferEvent::CopyFailed {
                            rel_path: action.rel_path.clone(),
                            error: err.to_string(),
                        },
                    );
                } else {
                    pb.inc(action.size);
                    let completed =
                        copied_bytes.fetch_add(action.size, Ordering::Relaxed) + action.size;
                    emit(
                        &reporter,
                        TransferEvent::Copied {
                            rel_path: action.rel_path.clone(),
                            size: action.size,
                            completed_bytes: completed,
                            total_bytes: total_size,
                        },
                    );
                }
            }
        })
        .await;
    Ok(())
}

async fn should_copy(
    source: &FileEntry,
    target: Option<&FileEntry>,
    compare: &CompareContext<'_>,
    options: &SyncOptions,
) -> Result<bool> {
    if target.is_none() {
        return Ok(true);
    }
    if options.ignore_existing {
        return Ok(false);
    }

    let target = target.expect("target checked is_some");
    if source.size != target.size {
        return Ok(true);
    }

    if !options.checksum {
        if source.mtime_ms <= target.mtime_ms {
            return Ok(false);
        }
        return Ok(true);
    }

    let src_hash = file_hash(
        compare.source_op,
        &join_path(compare.source_base, &source.rel_path),
        compare.stream_chunk_size,
    )
    .await?;
    let dst_hash = file_hash(
        compare.target_op,
        &join_path(compare.target_base, &source.rel_path),
        compare.stream_chunk_size,
    )
    .await?;
    Ok(src_hash != dst_hash)
}

async fn file_hash(op: &Operator, path: &str, stream_chunk_size: usize) -> Result<String> {
    let reader = retry_async(
        || async { op.reader_with(path).chunk(stream_chunk_size).await },
        3,
    )
    .await
    .with_context(|| format!("failed to open reader for hash: {path}"))?;
    let mut stream = reader
        .into_bytes_stream(..)
        .await
        .with_context(|| format!("failed to create hash stream: {path}"))?;
    let mut digest = sha2::Sha256::new();
    while let Some(chunk) = stream
        .try_next()
        .await
        .with_context(|| format!("failed to read hash chunk: {path}"))?
    {
        digest.update(&chunk);
    }
    Ok(format!("{:x}", digest.finalize()))
}

async fn copy_one(
    source_op: &Operator,
    source_base: &str,
    target_op: &Operator,
    target_base: &str,
    rel_path: &str,
    bandwidth_limiter: Option<&BandwidthLimiter>,
    stream_chunk_size: usize,
) -> Result<()> {
    let source_path = join_path(source_base, rel_path);
    let target_path = join_path(target_base, rel_path);

    if let Some(parent) = Path::new(&target_path).parent() {
        let p = parent.to_string_lossy().replace('\\', "/");
        if !p.is_empty() {
            if let Err(err) = retry_async(|| async { target_op.create_dir(&p).await }, 2).await {
                warn!(
                    path = %target_path,
                    parent = %p,
                    error = ?err,
                    "create target dir failed, continue to write file"
                );
            }
        }
    }

    let reader = retry_async(
        || async {
            source_op
                .reader_with(&source_path)
                .chunk(stream_chunk_size)
                .await
        },
        3,
    )
    .await
    .with_context(|| format!("failed to open source reader: {source_path}"))?;
    let mut stream = reader
        .into_bytes_stream(..)
        .await
        .with_context(|| format!("failed to create source stream: {source_path}"))?;
    let mut writer = retry_async(
        || async {
            target_op
                .writer_with(&target_path)
                .chunk(stream_chunk_size)
                .await
        },
        3,
    )
    .await
    .with_context(|| format!("failed to open target writer: {target_path}"))?;

    while let Some(chunk) = stream
        .try_next()
        .await
        .with_context(|| format!("failed to read source chunk: {source_path}"))?
    {
        if let Some(limiter) = bandwidth_limiter {
            limiter.acquire(chunk.len() as u64).await;
        }

        writer
            .write(chunk)
            .await
            .with_context(|| format!("failed to write target chunk: {target_path}"))?;
    }

    writer
        .close()
        .await
        .with_context(|| format!("failed to finalize target write: {target_path}"))?;

    Ok(())
}

async fn retry_async<F, Fut, T, E>(mut f: F, retries: usize) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
{
    let mut attempt: usize = 0;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(err) => {
                attempt += 1;
                if attempt > retries {
                    return Err(err);
                }
                let wait = 100 * (1u64 << (attempt.min(8) - 1));
                sleep(Duration::from_millis(wait)).await;
            }
        }
    }
}

fn normalize_list_base(base: &str) -> String {
    let b = base.trim_matches('/');
    if b.is_empty() {
        "".to_string()
    } else {
        format!("{}/", b)
    }
}

fn diff_rel(base: &str, full: &str) -> String {
    let b = base.trim_matches('/');
    let f = full.trim_matches('/');
    if b.is_empty() {
        return f.to_string();
    }
    f.strip_prefix(b)
        .map(|s| s.trim_matches('/').to_string())
        .unwrap_or_else(|| f.to_string())
}

fn join_path(base: &str, rel: &str) -> String {
    let b = base.trim_matches('/');
    let r = rel.trim_matches('/');
    if b.is_empty() {
        return r.to_string();
    }
    if r.is_empty() {
        return b.to_string();
    }
    format!("{}/{}", b, r)
}

pub fn parse_bandwidth_limit(s: Option<&str>) -> Result<Option<u64>> {
    let Some(raw) = s else {
        return Ok(None);
    };
    let trimmed = raw.trim().to_uppercase();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let (num, unit) = split_num_unit(&trimmed);
    let value: f64 = num
        .parse()
        .with_context(|| format!("invalid bandwidth value: {raw}"))?;

    let multiplier = match unit {
        "" | "B" => 1f64,
        "K" | "KB" => 1024f64,
        "M" | "MB" => 1024f64 * 1024f64,
        "G" | "GB" => 1024f64 * 1024f64 * 1024f64,
        _ => return Err(anyhow!("invalid bandwidth unit: {unit}")),
    };

    Ok(Some((value * multiplier) as u64))
}

pub fn parse_stream_chunk_size(s: Option<&str>) -> Result<usize> {
    let raw = s.unwrap_or("8MB");
    let trimmed = raw.trim().to_uppercase();
    if trimmed.is_empty() {
        return Ok(DEFAULT_STREAM_CHUNK_SIZE);
    }

    let (num, unit) = split_num_unit(&trimmed);
    let value: f64 = num
        .parse()
        .with_context(|| format!("invalid chunk size value: {raw}"))?;
    if value <= 0.0 {
        return Err(anyhow!("chunk size must be greater than 0"));
    }

    let multiplier = match unit {
        "" | "B" => 1f64,
        "K" | "KB" | "KIB" => 1024f64,
        "M" | "MB" | "MIB" => 1024f64 * 1024f64,
        "G" | "GB" | "GIB" => 1024f64 * 1024f64 * 1024f64,
        _ => return Err(anyhow!("invalid chunk size unit: {unit}")),
    };

    let bytes = (value * multiplier) as u64;
    if bytes == 0 {
        return Err(anyhow!("chunk size must be greater than 0"));
    }
    usize::try_from(bytes).context("chunk size is too large for this platform")
}

fn split_num_unit(input: &str) -> (&str, &str) {
    let idx = input
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit() && *ch != '.')
        .map(|(i, _)| i)
        .unwrap_or(input.len());
    input.split_at(idx)
}

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use globset::{Glob, GlobSet, GlobSetBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use opendal::{EntryMode, Metakey, Operator};
use sha2::Digest;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::remote::Location;

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
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub rel_path: String,
    pub size: u64,
    pub mtime_ms: i64,
}

#[derive(Debug, Clone)]
struct Action {
    rel_path: String,
    size: u64,
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
    let filter = build_filter(&options.include, &options.exclude)?;

    info!("scanning source files");
    let source_files = collect_files(&source_op, &source.path, &filter, options.checkers).await?;
    let source_map: HashMap<String, FileEntry> = source_files
        .iter()
        .cloned()
        .map(|f| (f.rel_path.clone(), f))
        .collect();

    info!("scanning target files");
    let target_files = collect_files(&target_op, &target.path, &filter, options.checkers)
        .await
        .unwrap_or_default();
    let target_map: HashMap<String, FileEntry> = target_files
        .iter()
        .cloned()
        .map(|f| (f.rel_path.clone(), f))
        .collect();

    let mut copy_actions = Vec::new();
    for src in &source_files {
        let decision = should_copy(
            src,
            target_map.get(&src.rel_path),
            &source_op,
            &source.path,
            &target_op,
            &target.path,
            &options,
        )
        .await?;
        if decision {
            copy_actions.push(Action {
                rel_path: src.rel_path.clone(),
                size: src.size,
            });
        }
    }

    let mut delete_actions = Vec::new();
    if matches!(mode, SyncMode::Sync) {
        let source_set: HashSet<&String> = source_map.keys().collect();
        for t in target_map.keys() {
            if !source_set.contains(t) {
                delete_actions.push(t.clone());
            }
        }
    }

    let total_size = copy_actions.iter().map(|a| a.size).sum::<u64>();
    info!(
        copy_count = copy_actions.len(),
        delete_count = delete_actions.len(),
        "planned actions"
    );

    if options.dry_run {
        for action in &copy_actions {
            println!("[DRY-RUN] copy {}", action.rel_path);
        }
        for action in &delete_actions {
            println!("[DRY-RUN] delete {}", action);
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

    let failures = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

    stream::iter(copy_actions.clone())
        .for_each_concurrent(options.transfers.max(1), |action| {
            let src = source_op.clone();
            let dst = target_op.clone();
            let src_base = source.path.clone();
            let dst_base = target.path.clone();
            let failures = failures.clone();
            let pb = pb.clone();
            let bw = options.bandwidth_limit;

            async move {
                if let Err(err) =
                    copy_one(&src, &src_base, &dst, &dst_base, &action.rel_path, bw).await
                {
                    error!(path = %action.rel_path, error = %err, "copy failed");
                    failures
                        .lock()
                        .await
                        .push(format!("{}: {}", action.rel_path, err));
                } else {
                    pb.inc(action.size);
                }
            }
        })
        .await;

    pb.finish_with_message("copy done");

    for rel_path in delete_actions {
        let full_path = join_path(&target.path, &rel_path);
        if let Err(err) = retry_async(|| async { target_op.delete(&full_path).await }, 3).await {
            warn!(path = %rel_path, error = %err, "delete failed");
            failures
                .lock()
                .await
                .push(format!("delete {}: {}", rel_path, err));
        }
    }

    if matches!(mode, SyncMode::Move) {
        for action in copy_actions {
            let full_path = join_path(&source.path, &action.rel_path);
            if let Err(err) = retry_async(|| async { source_op.delete(&full_path).await }, 3).await
            {
                warn!(path = %action.rel_path, error = %err, "move delete source failed");
                failures
                    .lock()
                    .await
                    .push(format!("move delete {}: {}", action.rel_path, err));
            }
        }
    }

    let failures = failures.lock().await;
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

async fn collect_files(
    op: &Operator,
    base_path: &str,
    filter: &Filter,
    _checkers: usize,
) -> Result<Vec<FileEntry>> {
    let mut out = Vec::new();
    let base = normalize_list_base(base_path);
    let mut lister = op
        .lister_with(&base)
        .recursive(true)
        .metakey(Metakey::ContentLength | Metakey::LastModified)
        .await?;

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
        let size = m.content_length();
        let mtime_ms = m
            .last_modified()
            .map(|ts| ts.timestamp_millis())
            .unwrap_or_default();

        out.push(FileEntry {
            rel_path: rel,
            size,
            mtime_ms,
        });
    }

    Ok(out)
}

async fn should_copy(
    source: &FileEntry,
    target: Option<&FileEntry>,
    source_op: &Operator,
    source_base: &str,
    target_op: &Operator,
    target_base: &str,
    options: &SyncOptions,
) -> Result<bool> {
    if target.is_none() {
        return Ok(true);
    }
    if options.ignore_existing {
        return Ok(false);
    }

    let target = target.expect("target checked is_some");
    if !options.checksum {
        if source.size == target.size && source.mtime_ms <= target.mtime_ms {
            return Ok(false);
        }
        return Ok(true);
    }

    let src_hash = file_hash(source_op, &join_path(source_base, &source.rel_path)).await?;
    let dst_hash = file_hash(target_op, &join_path(target_base, &source.rel_path)).await?;
    Ok(src_hash != dst_hash)
}

async fn file_hash(op: &Operator, path: &str) -> Result<String> {
    let bytes = retry_async(|| async { op.read(path).await }, 3)
        .await
        .with_context(|| format!("failed to read for hash: {path}"))?;
    let digest = sha2::Sha256::digest(bytes.to_vec());
    Ok(format!("{:x}", digest))
}

async fn copy_one(
    source_op: &Operator,
    source_base: &str,
    target_op: &Operator,
    target_base: &str,
    rel_path: &str,
    bandwidth_limit: Option<u64>,
) -> Result<()> {
    let source_path = join_path(source_base, rel_path);
    let target_path = join_path(target_base, rel_path);

    if let Some(parent) = Path::new(&target_path).parent() {
        let p = parent.to_string_lossy().replace('\\', "/");
        if !p.is_empty() {
            let _ = retry_async(|| async { target_op.create_dir(&p).await }, 2).await;
        }
    }

    let data = retry_async(|| async { source_op.read(&source_path).await }, 3)
        .await
        .with_context(|| format!("failed to read source: {source_path}"))?;

    if let Some(limit) = bandwidth_limit {
        if limit > 0 {
            let seconds = data.len() as f64 / limit as f64;
            if seconds > 0.0 {
                sleep(Duration::from_secs_f64(seconds)).await;
            }
        }
    }

    retry_async(
        || async { target_op.write(&target_path, data.clone()).await },
        3,
    )
    .await
    .with_context(|| format!("failed to write target: {target_path}"))?;

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

fn split_num_unit(input: &str) -> (&str, &str) {
    let idx = input
        .char_indices()
        .find(|(_, ch)| !ch.is_ascii_digit() && *ch != '.')
        .map(|(i, _)| i)
        .unwrap_or(input.len());
    input.split_at(idx)
}

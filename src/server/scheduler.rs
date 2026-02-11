use super::*;
pub(super) async fn load_schedules(state: Arc<AppState>) -> Result<()> {
    let tasks = state.db.list_tasks().await?;
    for task in tasks {
        sync_task_schedule(state.clone(), &task).await?;
    }
    Ok(())
}

pub(super) async fn sync_task_schedule(state: Arc<AppState>, task: &TaskRecord) -> Result<()> {
    unschedule_task(state.clone(), task.id).await?;
    if !task.enabled {
        return Ok(());
    }
    let cron_expr = match task
        .cron_expr
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
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
    .with_context(|| {
        format!(
            "invalid cron expression for task {}: {}",
            task.id, cron_expr
        )
    })?;
    let job_id = job.guid();
    state
        .scheduler
        .add(job)
        .await
        .with_context(|| format!("failed adding scheduler job for task {}", task.id))?;
    state.scheduled_jobs.lock().await.insert(task.id, job_id);
    Ok(())
}

pub(super) async fn unschedule_task(state: Arc<AppState>, task_id: i64) -> Result<()> {
    if let Some(job_id) = state.scheduled_jobs.lock().await.remove(&task_id) {
        if let Err(err) = state.scheduler.remove(&job_id).await {
            warn!(task_id = task_id, error = %err, "failed to remove scheduler job");
        }
    }
    Ok(())
}

pub(super) async fn run_task_by_id(
    state: Arc<AppState>,
    task_id: i64,
    trigger: &'static str,
) -> Result<()> {
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

pub(super) async fn execute_task(
    state: Arc<AppState>,
    task: TaskRecord,
    trigger: &'static str,
) -> Result<()> {
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
            let should_emit =
                completed_bytes >= total_bytes || now_ms.saturating_sub(last_emit) >= 250;
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

    let transfer_result = async {
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
            bandwidth_limit: parse_bandwidth_limit(task.bandwidth_limit.as_deref())?,
            stream_chunk_size: parse_stream_chunk_size(Some(task.chunk_size.as_str()))?,
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

    let finalize_result = async {
        match &transfer_result {
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
                state
                    .db
                    .insert_run_errors(run_id, error_records_vec)
                    .await?;
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
                state
                    .db
                    .insert_run_errors(run_id, error_records_vec)
                    .await?;
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
        Ok::<(), anyhow::Error>(())
    }
    .await;

    state.running_tasks.lock().await.remove(&task.id);
    if let Err(finalize_err) = finalize_result {
        return match transfer_result {
            Ok(_) => Err(finalize_err),
            Err(transfer_err) => Err(anyhow!(
                "task run failed: {}; and failed to persist final run state: {}",
                transfer_err,
                finalize_err
            )),
        };
    }
    transfer_result
}

pub(super) fn parse_mode(mode: &str) -> Result<SyncMode> {
    match mode.to_ascii_lowercase().as_str() {
        "copy" => Ok(SyncMode::Copy),
        "sync" => Ok(SyncMode::Sync),
        "move" => Ok(SyncMode::Move),
        other => Err(anyhow!("invalid mode: {other}, expected copy|sync|move")),
    }
}

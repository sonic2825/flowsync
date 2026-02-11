use super::*;
pub(super) async fn api_health() -> Json<serde_json::Value> {
    Json(json!({ "ok": true, "time": Utc::now().to_rfc3339() }))
}

pub(super) async fn api_list_remotes(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<RemoteRecord>>, ApiError> {
    Ok(Json(state.db.list_remotes().await?))
}

pub(super) async fn api_create_remote(
    State(state): State<Arc<AppState>>,
    Json(mut req): Json<CreateRemoteRequest>,
) -> Result<(StatusCode, Json<RemoteRecord>), ApiError> {
    normalize_create_remote_request(&mut req)?;
    let remote = state.db.create_remote(req).await?;
    Ok((StatusCode::CREATED, Json(remote)))
}

pub(super) async fn api_update_remote(
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

pub(super) async fn api_delete_remote(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let removed = state.db.delete_remote(&name).await?;
    if !removed {
        return Err(ApiError::not_found(format!("remote `{name}` not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn api_list_tasks(
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

pub(super) async fn api_create_task(
    State(state): State<Arc<AppState>>,
    Json(mut req): Json<CreateTaskRequest>,
) -> Result<(StatusCode, Json<TaskRecord>), ApiError> {
    normalize_task_request(&mut req)?;
    let created = state.db.create_task(req).await?;
    sync_task_schedule(state.clone(), &created).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

pub(super) async fn api_update_task(
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

pub(super) async fn api_delete_task(
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

pub(super) async fn api_pause_task(
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

pub(super) async fn api_resume_task(
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

pub(super) async fn api_run_task_now(
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

pub(super) async fn api_list_runs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<Vec<RunRecord>>, ApiError> {
    Ok(Json(state.db.list_runs(query).await?))
}

pub(super) async fn api_list_events(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListTaskEventsQuery>,
) -> Result<Json<Vec<TaskEventRecord>>, ApiError> {
    if query.task_id.is_none() && query.run_id.is_none() {
        return Err(ApiError::bad_request("task_id or run_id is required"));
    }
    Ok(Json(state.db.list_task_events(query).await?))
}

pub(super) async fn api_dashboard(
    State(state): State<Arc<AppState>>,
) -> Result<Json<DashboardResponse>, ApiError> {
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

pub(super) async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_client(socket, state))
}

pub(super) async fn ws_client(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.ws_tx.subscribe();
    while let Ok(payload) = rx.recv().await {
        if socket.send(Message::Text(payload)).await.is_err() {
            break;
        }
    }
}

pub(super) async fn index_handler() -> Html<String> {
    match WebAssets::get("index.html") {
        Some(content) => Html(String::from_utf8_lossy(content.data.as_ref()).to_string()),
        None => Html("<h1>index.html not found</h1>".to_string()),
    }
}

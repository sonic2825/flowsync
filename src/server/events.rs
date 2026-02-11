use super::*;
pub(super) fn broadcast_json(sender: &broadcast::Sender<String>, value: serde_json::Value) {
    let _ = sender.send(value.to_string());
}

pub(super) fn persist_task_event(
    sender: &Sender<TaskEventWrite>,
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
        .unwrap_or("")
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
    match sender.try_send(record) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(full_record)) => {
            if is_critical_task_event(&full_record.event_type) {
                let sender = sender.clone();
                tokio::spawn(async move {
                    if let Err(err) = sender.send(full_record).await {
                        warn!(
                            event_type = %err.0.event_type,
                            "persist critical task event failed: writer channel closed"
                        );
                    }
                });
                return;
            }

            let should_drop = matches!(
                full_record.event_type.as_str(),
                "task_progress" | "task_file_uploaded"
            );
            let dropped = DROPPED_TASK_EVENTS.fetch_add(1, Ordering::Relaxed) + 1;
            if !should_drop || dropped.is_multiple_of(100) {
                warn!(
                    task_id = task_id,
                    run_id = ?run_id,
                    event_type = %full_record.event_type,
                    dropped_total = dropped,
                    "persist task event dropped: queue is full"
                );
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            warn!(task_id = task_id, run_id = ?run_id, "persist task event dropped: writer channel closed");
        }
    }
}

pub(super) fn is_critical_task_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "task_started" | "task_error" | "task_finished_signal" | "task_completed"
    )
}

pub(super) async fn task_event_writer_worker(db: Db, mut rx: Receiver<TaskEventWrite>) {
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

pub(super) async fn persist_task_event_batch_with_retry(
    db: &Db,
    batch: Vec<TaskEventWrite>,
) -> Result<()> {
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

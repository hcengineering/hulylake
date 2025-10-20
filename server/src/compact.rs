use std::collections::HashSet;
use std::sync::Arc;

use size::Size;
use tokio::sync::{RwLock, mpsc};
use tracing::*;
use uuid::Uuid;

use crate::config::CONFIG;
use crate::handlers::{ApiError, PartData};
use crate::merge::{self};
use crate::postgres::{ObjectPart, Pool};
use crate::s3::S3Client;
use crate::{blob, postgres, recovery};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CompactTask {
    pub workspace: Uuid,
    pub key: String,
}

pub struct CompactWorker {
    ingest_tx: mpsc::Sender<CompactTask>,
    ingest_handle: Arc<tokio::task::JoinHandle<()>>,
    compact_handle: Arc<tokio::task::JoinHandle<()>>,
}

impl Clone for CompactWorker {
    fn clone(&self) -> Self {
        CompactWorker {
            ingest_tx: self.ingest_tx.clone(),
            ingest_handle: self.ingest_handle.clone(),
            compact_handle: self.compact_handle.clone(),
        }
    }
}

impl CompactWorker {
    pub fn new(s3: Arc<S3Client>, pool: Pool, buffer_size: usize) -> Self {
        let (ingest_tx, ingest_rx) = mpsc::channel(buffer_size);
        let (compact_tx, compact_rx) = mpsc::channel(buffer_size);

        let pending_tasks = Arc::new(RwLock::new(HashSet::new()));
        let pending_tasks_ingest = pending_tasks.clone();
        let pending_tasks_compact = pending_tasks.clone();

        let ingest_handle = tokio::spawn(async move {
            debug!(buffer_size, "started ingest worker");
            Self::run_ingest_worker(ingest_rx, compact_tx, pending_tasks_ingest).await
        });

        let compact_handle = tokio::spawn(async move {
            debug!(buffer_size, "started compact worker");
            Self::run_compact_worker(compact_rx, s3.clone(), pool, pending_tasks_compact).await;
        });

        Self {
            ingest_tx,
            ingest_handle: Arc::new(ingest_handle),
            compact_handle: Arc::new(compact_handle),
        }
    }

    async fn run_ingest_worker(
        mut ingest_rx: mpsc::Receiver<CompactTask>,
        compact_tx: mpsc::Sender<CompactTask>,
        pending_tasks: Arc<RwLock<HashSet<CompactTask>>>,
    ) {
        loop {
            while let Some(task) = ingest_rx.recv().await {
                let is_new = pending_tasks.write().await.insert(task.clone());
                if !is_new {
                    continue;
                }

                if let Err(err) = compact_tx.send(task.clone()).await {
                    error!(%err, "failed to send compact task");
                    pending_tasks.write().await.remove(&task);
                }
            }
        }
    }

    async fn run_compact_worker(
        mut rx: mpsc::Receiver<CompactTask>,
        s3: Arc<S3Client>,
        pool: Pool,
        pending_tasks: Arc<RwLock<HashSet<CompactTask>>>,
    ) {
        loop {
            while let Some(task) = rx.recv().await {
                debug!(key = %task.key, workspace = %task.workspace, "processing compact task");
                let res = compact(s3.clone(), pool.clone(), task.clone()).await;
                if let Err(err) = res {
                    error!(%err, "failed to compact");
                }
                pending_tasks.write().await.remove(&task);
            }
        }
    }

    pub async fn send(&self, parts: &Vec<ObjectPart<PartData>>) -> Result<(), ApiError> {
        if parts.len() > CONFIG.compact_parts_limit {
            let task = CompactTask {
                workspace: parts[0].data.workspace,
                key: parts[0].data.key.clone(),
            };

            let res = self.ingest_tx.send(task.clone()).await;
            if let Err(err) = res {
                warn!(%err, "failed to schedule compact");
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
        self.ingest_handle.abort();
        self.compact_handle.abort();
    }
}

async fn compact(s3: Arc<S3Client>, pool: Pool, task: CompactTask) -> anyhow::Result<()> {
    let s3 = s3.clone();
    let pool = pool.clone();

    let workspace = task.workspace;
    let key = task.key;

    let parts = postgres::find_parts(&pool, task.workspace, &key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to find parts: {}", e))?;
    let first = &parts.first().unwrap().data;
    let last = &parts.last().unwrap().data;

    debug!(workspace = %workspace, key = %key, parts = %parts.len(), "found parts");

    let stream = merge::stream(s3.clone(), parts.to_vec())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to merge stream: {}", e))?;

    debug!(workspace = %workspace, key = %key, length = %stream.content_length, "merged stream");

    let uploaded = blob::upload(
        &s3,
        &pool,
        Size::from_bytes(stream.content_length),
        stream.stream,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to upload: {}", e))?;

    debug!(workspace = %workspace, key = %key, s3_key = %uploaded.s3_key, "uploaded stream");

    let inline = uploaded.inline.and_then(|inline| {
        if inline.len() < CONFIG.inline_threshold.bytes() as usize {
            Some(inline)
        } else {
            None
        }
    });

    let part_data = PartData {
        workspace,
        key: key.to_owned(),
        part: 0,
        blob: uploaded.s3_key,
        size: uploaded.length,
        etag: last.etag.clone(),
        date: last.date.clone(),

        headers: first.headers.clone(),
        meta: first.meta.clone(),
        merge_strategy: first.merge_strategy,
    };

    postgres::compact(&pool, workspace, &key, inline, &part_data, last.part)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to compact in database: {}", e))?;

    let parts = postgres::find_parts::<PartData>(&pool, workspace, &key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to find parts after compact: {}", e))?;

    let parts_data = parts.iter().map(|p| &p.data).collect::<Vec<&PartData>>();

    recovery::set_object(&s3, workspace, &key, parts_data, None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set object in recovery: {}", e))?;

    Ok(())
}

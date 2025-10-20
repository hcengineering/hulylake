use std::sync::Arc;

use futures::TryFutureExt;
use size::Size;
use tokio::sync::mpsc;
use tracing::*;
use uuid::Uuid;

use crate::config::CONFIG;
use crate::handlers::{ApiError, PartData};
use crate::merge::{self};
use crate::postgres::Pool;
use crate::s3::S3Client;
use crate::{blob, postgres, recovery};

#[derive(Debug)]
pub struct CompactTask {
    pub workspace: Uuid,
    pub key: String,
}

pub struct CompactWorker {
    compact_tx: mpsc::Sender<CompactTask>,
    handle: Arc<tokio::task::JoinHandle<()>>,
}

impl Clone for CompactWorker {
    fn clone(&self) -> Self {
        CompactWorker {
            compact_tx: self.compact_tx.clone(),
            handle: self.handle.clone(),
        }
    }
}

impl CompactWorker {
    pub fn new(s3: Arc<S3Client>, pool: Pool, buffer_size: usize) -> Self {
        let (compact_tx, compact_rx) = mpsc::channel(buffer_size);

        let handle = tokio::spawn(async move {
            debug!(buffer_size, "started compact worker");
            Self::run(compact_rx, s3.clone(), pool).await;
        });

        Self {
            compact_tx,
            handle: Arc::new(handle),
        }
    }

    async fn run(mut rx: mpsc::Receiver<CompactTask>, s3: Arc<S3Client>, pool: Pool) {
        loop {
            while let Some(task) = rx.recv().await {
                debug!(key = %task.key, workspace = %task.workspace, "received compact task");
                let res = compact(s3.clone(), pool.clone(), task).await;
                if let Err(err) = res {
                    error!(%err, "failed to compact");
                }
            }
        }
    }

    pub async fn send(&self, task: CompactTask) -> Result<(), ApiError> {
        let res = self.compact_tx.send(task).await;
        if let Err(err) = res {
            warn!(%err, "failed to schedule compact");
        }

        Ok(())
    }
}

async fn compact(s3: Arc<S3Client>, pool: Pool, task: CompactTask) -> Result<(), ApiError> {
    let s3 = s3.clone();
    let pool = pool.clone();

    let workspace = task.workspace;
    let key = task.key;

    let parts = postgres::find_parts(&pool, task.workspace, &key).await?;
    let first = &parts.first().unwrap().data;
    let last = &parts.last().unwrap().data;

    debug!(workspace = %workspace, key = %key, parts = %parts.len(), "found parts");

    let stream = merge::stream(s3.clone(), parts.to_vec()).await?;

    debug!(workspace = %workspace, key = %key, length = %stream.content_length, "merged stream");

    let uploaded = blob::upload(
        &s3,
        &pool,
        Size::from_bytes(stream.content_length),
        stream.stream,
    )
    .await?;

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

    postgres::compact(&pool, workspace, &key, inline, &part_data, last.part).await?;

    let parts = postgres::find_parts::<PartData>(&pool, workspace, &key)
        .map_err(ApiError::from)
        .await?;

    let parts_data = parts.iter().map(|p| &p.data).collect::<Vec<&PartData>>();

    recovery::set_object(&s3, workspace, &key, parts_data, None).await?;

    Ok(())
}

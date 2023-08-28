// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::{pin, Pin};
use std::str::FromStr;
use std::sync::Arc;

use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::JsonbVal;
use risingwave_connector::source::external::CdcOffset;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_storage::StateStore;
use serde_json::Value;

use crate::executor::backfill::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::upstream_table::snapshot::{
    SnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{
    get_cdc_chunk_last_offset, get_new_pos, mapping_chunk, mapping_message, mark_cdc_chunk,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, Mutation, PkIndices, PkIndicesRef, SourceStateTableHandler,
    StreamExecutorError,
};
use crate::task::{ActorId, CreateMviewProgress};

const BACKFILL_STATE_KEY_SUFFIX: &str = "_backfill";

pub struct CdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Upstream external table
    upstream_table: ExternalStorageTable,

    /// Upstream changelog with the same schema with the external table.
    upstream: BoxedExecutor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    /// User may select a subset of columns from the upstream table.
    output_indices: Vec<usize>,

    actor_id: ActorId,

    info: ExecutorInfo,

    /// Stores the backfill done flag
    source_state_handler: SourceStateTableHandler<S>,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,
}

impl<S: StateStore> CdcBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        upstream_table: ExternalStorageTable,
        upstream: BoxedExecutor,
        output_indices: Vec<usize>,
        _progress: Option<CreateMviewProgress>,
        schema: Schema,
        pk_indices: PkIndices,
        metrics: Arc<StreamingMetrics>,
        source_state_handler: SourceStateTableHandler<S>,
        chunk_size: usize,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "CdcBackfillExecutor".to_owned(),
            },
            upstream_table,
            upstream,
            output_indices,
            actor_id: 0,
            metrics,
            chunk_size,
            actor_ctx,
            source_state_handler,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();

        let pk_order = self.upstream_table.pk_order_types().to_vec();

        let upstream_table_id = self.upstream_table.table_id().table_id;
        let upstream_table_reader = UpstreamTableReader::new(self.upstream_table);

        let mut upstream = self.upstream.execute().peekable();

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pk_pos: Option<OwnedRow>;

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;

        // Check whether this parallelism has been assigned splits,
        // if not, we should bypass the backfill directly.
        let mut invalid_backfill = false;
        let split_id;
        if let Some(mutation) = first_barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add { splits, .. }
                | Mutation::Update {
                    actor_splits: splits,
                    ..
                } => {
                    invalid_backfill = match splits.get(&self.actor_ctx.id) {
                        None => true,
                        Some(splits) => {
                            let is_empty = splits.is_empty();
                            if !is_empty {
                                let split = splits.iter().exactly_one()?;
                                split_id = Some(split.id())
                            }
                            is_empty
                        }
                    }
                }
                _ => {}
            }
        }

        if invalid_backfill {
            // The first barrier message should be propagated.
            yield Message::Barrier(first_barrier);
            #[for_await]
            for msg in upstream {
                if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                    yield msg;
                }
            }
            // exit the executor
            return Ok(());
        }

        tracing::debug!("start cdc backfill: actor {:?}", self.actor_ctx.id);

        // start from the beginning
        // TODO(siyuan): restore backfill offset from state store
        let backfill_offset = None;

        current_pk_pos = backfill_offset;

        let is_finished = {
            // restore backfill done flag from state store
            if let Some(split_id) = split_id {
                let mut key = split_id.as_ref().to_string();
                key.push_str(BACKFILL_STATE_KEY_SUFFIX);
                match self.source_state_handler.lock().get(key.into()).await? {
                    Some(row) => match row.datum_at(1) {
                        Some(ScalarRefImpl::Jsonb(jsonb_ref)) => jsonb_ref.as_bool()?,
                        _ => unreachable!("invalid backfill persistent state"),
                    },
                    None => false,
                }
            } else {
                false
            }
        };

        // If the snapshot is empty, we don't need to backfill.
        let is_snapshot_empty: bool = {
            if is_finished {
                // It is finished, so just assign a value to avoid accessing storage table again.
                false
            } else {
                let args = SnapshotReadArgs::new(init_epoch, None, false, self.chunk_size);
                let snapshot = upstream_table_reader.snapshot_read(args);
                pin_mut!(snapshot);
                snapshot.try_next().await?.unwrap().is_none()
            }
        };

        // | backfill_is_finished | snapshot_empty | need_to_backfill |
        // | t                    | t/f            | f                |
        // | f                    | t              | f                |
        // | f                    | f              | t                |
        let to_backfill = !is_finished && !is_snapshot_empty;

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        // Keep track of rows from the snapshot.
        #[allow(unused_variables)]
        let mut total_snapshot_processed_rows: u64 = 0;

        let mut last_binlog_offset: Option<CdcOffset>;

        let mut consumed_binlog_offset: Option<CdcOffset> = None;

        let mut is_backfill_done = None;

        // CDC Backfill Algorithm:
        //
        // When the first barrier comes from upstream:
        //  - read the current binlog offset as `binlog_low`
        //  - start a snapshot read upon upstream table and iterate over the snapshot read stream
        //  - buffer the changelog event from upstream
        //
        // When a new barrier comes from upstream:
        //  - read the current binlog offset as `binlog_high`
        //  - for each row of the upstream change log, forward it to downstream if it in the range
        //    of [binlog_low, binlog_high] and its pk <= `current_pos`, otherwise ignore it
        //  - reconstruct the whole backfill stream with upstream changelog and a new table snapshot
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if to_backfill {
            last_binlog_offset = upstream_table_reader.current_binlog_offset().await?;
            // drive the upstream changelog first to ensure we can receive timely changelog event,
            // otherwise the upstream changelog may be blocked by the snapshot read stream
            // TODO: we can setup debezium connector starts at latest offset to avoid pulling old
            // events.
            let _ = Pin::new(&mut upstream).peek().await;

            tracing::info!(
                "start the bacfill loop: [initial] binlog offset {:?}",
                last_binlog_offset,
            );

            'backfill_loop: loop {
                let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

                let left_upstream = upstream.by_ref().map(Either::Left);

                let args = SnapshotReadArgs::new_for_cdc(current_pk_pos.clone(), self.chunk_size);
                let right_snapshot =
                    pin!(upstream_table_reader.snapshot_read(args).map(Either::Right));

                // Prefer to select upstream, so we can stop snapshot stream when barrier comes.
                let backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;

                #[for_await]
                for either in backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    // If it is a barrier, switch snapshot and consume buffered
                                    // upstream chunk.
                                    // If no current_pos, means we did not process any snapshot yet.
                                    // In that case we can just ignore the upstream buffer chunk.
                                    if let Some(current_pos) = &current_pk_pos {
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            cur_barrier_upstream_processed_rows +=
                                                chunk.cardinality() as u64;

                                            // record the consumed binlog offset that will be
                                            // persisted later
                                            consumed_binlog_offset = get_cdc_chunk_last_offset(
                                                upstream_table_reader.inner().table_reader(),
                                                &chunk,
                                            )?;
                                            yield Message::Chunk(mapping_chunk(
                                                mark_cdc_chunk(
                                                    upstream_table_reader.inner().table_reader(),
                                                    chunk,
                                                    current_pos,
                                                    &pk_in_output_indices,
                                                    &pk_order,
                                                    last_binlog_offset.clone(),
                                                )?,
                                                &self.output_indices,
                                            ));
                                        }
                                    }

                                    self.metrics
                                        .backfill_snapshot_read_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_snapshot_processed_rows);

                                    self.metrics
                                        .backfill_upstream_output_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_upstream_processed_rows);

                                    // Update last seen binlog offset
                                    if consumed_binlog_offset.is_some() {
                                        last_binlog_offset = consumed_binlog_offset.clone();
                                    }

                                    // TODO(siyuan): Persist state on barrier

                                    yield Message::Barrier(barrier);
                                    // Break the for loop and start a new snapshot read stream.
                                    break;
                                }
                                Message::Chunk(chunk) => {
                                    let chunk_binlog_offset = get_cdc_chunk_last_offset(
                                        upstream_table_reader.inner().table_reader(),
                                        &chunk,
                                    )?;

                                    tracing::trace!(
                                        "recv changelog chunk: bin offset {:?}, capactiy {}",
                                        chunk_binlog_offset,
                                        chunk.capacity()
                                    );

                                    // Since we don't need changelog before the
                                    // `last_binlog_offset`, skip the chunk that *only* contains
                                    // events before `last_binlog_offset`.
                                    if let Some(last_binlog_offset) = &last_binlog_offset {
                                        if let Some(chunk_binlog_offset) = chunk_binlog_offset {
                                            if chunk_binlog_offset < *last_binlog_offset {
                                                tracing::trace!(
                                                    "skip changelog chunk: offset {:?}, capacity {}",
                                                    chunk_binlog_offset,
                                                    chunk.capacity()
                                                );
                                                continue;
                                            }
                                        }
                                    }
                                    // Buffer the upstream chunk.
                                    upstream_chunk_buffer.push(chunk.compact());
                                }
                                Message::Watermark(_) => {
                                    // Ignore watermark during backfill.
                                }
                            }
                        }
                        // Snapshot read
                        Either::Right(msg) => {
                            match msg? {
                                None => {
                                    tracing::debug!(
                                        "snapshot read stream ends: last_binlog_offset {:?}, current_pk_pos {:?}",
                                        last_binlog_offset, current_pk_pos
                                    );
                                    // End of the snapshot read stream.
                                    // We should not mark the chunk anymore,
                                    // otherwise, we will ignore some rows
                                    // in the buffer. Here we choose to never mark the chunk.
                                    // Consume with the renaming stream buffer chunk without mark.
                                    for chunk in upstream_chunk_buffer.drain(..) {
                                        let chunk_cardinality = chunk.cardinality() as u64;
                                        cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                        total_snapshot_processed_rows += chunk_cardinality;
                                        yield Message::Chunk(mapping_chunk(
                                            chunk,
                                            &self.output_indices,
                                        ));
                                    }

                                    // Insert a flag to indicate the backfill has finished,
                                    // which will be persisted upon next barrier ingested to the
                                    // source.
                                    if let Some(split_id) = split_id {
                                        let mut key = split_id.as_ref().to_string();
                                        key.push_str(BACKFILL_STATE_KEY_SUFFIX);
                                        self.source_state_handler
                                            .lock()
                                            .set(key.into(), JsonbVal::from(Value::Bool(true))?)
                                            .await?;
                                    }

                                    break 'backfill_loop;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    current_pk_pos =
                                        Some(get_new_pos(&chunk, &pk_in_output_indices));

                                    tracing::debug!(
                                        "snapshot chunk length: {:?}",
                                        chunk.cardinality()
                                    );

                                    tracing::debug!(
                                        "current backfill progress: {:?}",
                                        current_pk_pos
                                    );
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_processed_rows += chunk_cardinality;
                                    yield Message::Chunk(mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::debug!(
            actor = self.actor_id,
            "CdcBackfill has already finished and forward messages directly to the downstream"
        );

        // After backfill progress finished
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            // upstream offsets will be removed from the message before forwarding to
            // downstream
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                yield msg;
            }
        }
    }
}

impl<S: StateStore> Executor for CdcBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

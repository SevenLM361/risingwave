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

use std::sync::Arc;

use anyhow::Context;
use futures::FutureExt;
use futures_async_stream::try_stream;
use parking_lot::RwLock;
use rand::seq::IteratorRandom;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::{Result, RwError};
use risingwave_connector::source::StreamChunkWithState;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};

pub type TableDmlHandleRef = Arc<TableDmlHandle>;

#[derive(Debug)]
struct TableDmlHandleCore {
    /// The senders of the changes channel.
    ///
    /// When a `StreamReader` is created, a channel will be created and the sender will be
    /// saved here. The insert statement will take one channel randomly.
    changes_txs: Vec<mpsc::Sender<(StreamChunk, oneshot::Sender<usize>)>>,
}

/// The buffer size of the channel between each [`TableDmlHandle`] and the source executors.
// TODO: decide a default value carefully and make this configurable.
const DML_CHUNK_BUFFER_SIZE: usize = 32;

/// [`TableDmlHandle`] is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows will be send to the associated "materialize" streaming task, then be written to the
/// state store. Therefore, [`TableDmlHandle`] can be simply be treated as a channel without side
/// effects.
#[derive(Debug)]
pub struct TableDmlHandle {
    core: RwLock<TableDmlHandleCore>,

    /// All columns in this table.
    column_descs: Vec<ColumnDesc>,
}

impl TableDmlHandle {
    pub fn new(column_descs: Vec<ColumnDesc>) -> Self {
        let core = TableDmlHandleCore {
            changes_txs: vec![],
        };

        Self {
            core: RwLock::new(core),
            column_descs,
        }
    }

    pub fn stream_reader(&self) -> TableStreamReader {
        let mut core = self.core.write();
        let (tx, rx) = mpsc::channel(DML_CHUNK_BUFFER_SIZE);
        core.changes_txs.push(tx);

        TableStreamReader { rx }
    }

    /// Asynchronously write stream chunk into table. Changes written here will be simply passed to
    /// the associated streaming task via channel, and then be materialized to storage there.
    ///
    /// Returns an oneshot channel which will be notified when the chunk is taken by some reader,
    /// and the `usize` represents the cardinality of this chunk.
    pub async fn write_chunk(&self, mut chunk: StreamChunk) -> Result<oneshot::Receiver<usize>> {
        loop {
            // The `changes_txs` should not be empty normally, since we ensured that the channels
            // between the `TableDmlHandle` and the `SourceExecutor`s are ready before we making the
            // table catalog visible to the users. However, when we're recovering, it's possible
            // that the streaming executors are not ready when the frontend is able to schedule DML
            // tasks to the compute nodes, so this'll be temporarily unavailable, so we throw an
            // error instead of asserting here.
            // TODO: may reject DML when streaming executors are not recovered.
            let tx = self
                .core
                .read()
                .changes_txs
                .iter()
                .choose(&mut rand::thread_rng())
                .context("no available table reader in streaming source executors")?
                .clone();

            #[cfg(debug_assertions)]
            risingwave_common::util::schema_check::schema_check(
                self.column_descs
                    .iter()
                    .filter_map(|c| (!c.is_generated()).then_some(&c.data_type)),
                chunk.columns(),
            )
            .expect("table source write chunk schema check failed");

            let (notifier_tx, notifier_rx) = oneshot::channel();

            match tx.send((chunk, notifier_tx)).await {
                Ok(_) => return Ok(notifier_rx),

                // It's possible that the source executor is scaled in or migrated, so the channel
                // is closed. In this case, we should remove the closed channel and retry.
                Err(SendError((chunk_, _))) => {
                    tracing::info!("find one closed table source channel, retry");
                    chunk = chunk_;

                    // Remove all closed channels.
                    self.core.write().changes_txs.retain(|tx| !tx.is_closed());
                }
            }
        }
    }

    /// Get the reference of all columns in this table.
    pub(super) fn column_descs(&self) -> &[ColumnDesc] {
        self.column_descs.as_ref()
    }
}

#[easy_ext::ext(TableDmlHandleTestExt)]
impl TableDmlHandle {
    /// Write a chunk and assert that the chunk channel is not blocking.
    fn write_chunk_ready(&self, chunk: StreamChunk) -> Result<oneshot::Receiver<usize>> {
        self.write_chunk(chunk).now_or_never().unwrap()
    }
}

/// [`TableStreamReader`] reads changes from a certain table continuously.
/// This struct should be only used for associated materialize task, thus the reader should be
/// created only once. Further streaming task relying on this table source should follow the
/// structure of "`MView` on `MView`".
#[derive(Debug)]
pub struct TableStreamReader {
    /// The receiver of the changes channel.
    rx: mpsc::Receiver<(StreamChunk, oneshot::Sender<usize>)>,
}

impl TableStreamReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    pub async fn into_stream(mut self) {
        while let Some((chunk, notifier)) = self.rx.recv().await {
            // Notify about that we've taken the chunk.
            _ = notifier.send(chunk.cardinality());
            yield chunk.into();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::types::DataType;

    use super::*;

    fn new_source() -> TableDmlHandle {
        TableDmlHandle::new(vec![ColumnDesc::unnamed(
            ColumnId::from(0),
            DataType::Int64,
        )])
    }

    #[tokio::test]
    async fn test_table_dml_handle() -> Result<()> {
        let source = Arc::new(new_source());
        let mut reader = source.stream_reader().into_stream();

        macro_rules! write_chunk {
            ($i:expr) => {{
                let source = source.clone();
                let chunk = StreamChunk::new(
                    vec![Op::Insert],
                    vec![I64Array::from_iter([$i]).into_ref()],
                    None,
                );
                source.write_chunk_ready(chunk).unwrap();
            }};
        }

        write_chunk!(0);

        macro_rules! check_next_chunk {
            ($i: expr) => {
                assert_matches!(reader.next().await.unwrap()?, chunk => {
                    assert_eq!(chunk.chunk.columns()[0].as_int64().iter().collect_vec(), vec![Some($i)]);
                });
            }
        }

        check_next_chunk!(0);

        write_chunk!(1);
        check_next_chunk!(1);

        Ok(())
    }
}

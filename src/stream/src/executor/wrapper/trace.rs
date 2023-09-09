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

use await_tree::InstrumentAwait;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use tracing::{Instrument, Span};

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ExecutorInfo, Message, MessageStream};
use crate::task::ActorId;

/// Streams wrapped by `trace` will be traced with `tracing` spans and reported to `opentelemetry`.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn trace(
    enable_executor_row_count: bool,
    info: Arc<ExecutorInfo>,
    _input_pos: usize,
    actor_id: ActorId,
    executor_id: u64,
    metrics: Arc<StreamingMetrics>,
    input: impl MessageStream,
) {
    let actor_id_string = actor_id.to_string();

    let span_name = pretty_identity(&info.identity, actor_id, executor_id);

    let is_sink_or_mv = info.identity.contains("Materialize") || info.identity.contains("Sink");

    let new_span = || tracing::info_span!("executor", "otel.name" = span_name, actor_id);
    let mut span = new_span();

    pin_mut!(input);

    while let Some(message) = input.next().instrument(span.clone()).await.transpose()? {
        // Trace the message in the span's scope.
        span.in_scope(|| match &message {
            Message::Chunk(chunk) => {
                if chunk.cardinality() > 0 {
                    if enable_executor_row_count || is_sink_or_mv {
                        metrics
                            .executor_row_count
                            .with_label_values(&[&actor_id_string, &span_name])
                            .inc_by(chunk.cardinality() as u64);
                    }
                    tracing::trace!(
                        target: "events::stream::message::chunk",
                        cardinality = chunk.cardinality(),
                        capacity = chunk.capacity(),
                        "\n{}\n", chunk.to_pretty_with_schema(&info.schema),
                    );
                }
            }
            Message::Watermark(watermark) => {
                tracing::trace!(
                    target: "events::stream::message::watermark",
                    value = ?watermark.val,
                    col_idx = watermark.col_idx,
                );
            }
            Message::Barrier(barrier) => {
                tracing::trace!(
                    target: "events::stream::message::barrier",
                    prev_epoch = barrier.epoch.prev,
                    curr_epoch = barrier.epoch.curr,
                    kind = ?barrier.kind,
                );
            }
        });

        // Yield the message and update the span.
        match &message {
            Message::Chunk(_) | Message::Watermark(_) => yield message,
            Message::Barrier(_) => {
                // Drop the span as the inner executor has finished processing the barrier (then all
                // data from the previous epoch).
                let _ = std::mem::replace(&mut span, Span::none());

                yield message;

                // Create a new span after we're called again. Now we're in a new epoch and the
                // parent of the span is updated.
                span = new_span();
            }
        }
    }
}

fn pretty_identity(identity: &str, actor_id: ActorId, executor_id: u64) -> String {
    format!(
        "{} (actor {}, operator {})",
        identity,
        actor_id,
        executor_id as u32 // The lower 32 bit is for the operator id.
    )
}

/// Streams wrapped by `instrument_await_tree` will be able to print the spans of the
/// executors in the stack trace through `await-tree`.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn instrument_await_tree(
    info: Arc<ExecutorInfo>,
    actor_id: ActorId,
    executor_id: u64,
    input: impl MessageStream,
) {
    pin_mut!(input);

    let span: await_tree::Span = pretty_identity(&info.identity, actor_id, executor_id).into();

    while let Some(message) = input
        .next()
        .instrument_await(span.clone())
        .await
        .transpose()?
    {
        yield message;
    }
}

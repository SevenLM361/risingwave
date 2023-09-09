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

use futures::StreamExt;
use risingwave_common::catalog::Schema;

use super::monitor::StreamingMetrics;
use super::{
    BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo, MessageStream, PkIndicesRef,
};
use crate::task::ActorId;

mod epoch_check;
mod epoch_provide;
mod schema_check;
mod trace;
mod update_check;

struct ExtraInfo {
    /// Index of input to this operator.
    input_pos: usize,

    actor_id: ActorId,
    executor_id: u64,

    metrics: Arc<StreamingMetrics>,
}

/// [`WrapperExecutor`] will do some sanity checks and logging for the wrapped executor.
pub struct WrapperExecutor {
    input: BoxedExecutor,

    extra: ExtraInfo,

    enable_executor_row_count: bool,
}

impl WrapperExecutor {
    pub fn new(
        input: BoxedExecutor,
        input_pos: usize,
        actor_id: ActorId,
        executor_id: u64,
        metrics: Arc<StreamingMetrics>,
        enable_executor_row_count: bool,
    ) -> Self {
        Self {
            input,
            extra: ExtraInfo {
                input_pos,
                actor_id,
                executor_id,
                metrics,
            },
            enable_executor_row_count,
        }
    }

    #[allow(clippy::let_and_return)]
    fn wrap_debug(
        _enable_executor_row_count: bool,
        info: Arc<ExecutorInfo>,
        _extra: ExtraInfo,
        stream: impl MessageStream + 'static,
    ) -> impl MessageStream + 'static {
        // Update check
        let stream = update_check::update_check(info, stream);

        stream
    }

    fn wrap(
        enable_executor_row_count: bool,
        info: Arc<ExecutorInfo>,
        extra: ExtraInfo,
        stream: impl MessageStream + 'static,
    ) -> BoxedMessageStream {
        // -- Shared wrappers --

        // Await tree
        let stream =
            trace::instrument_await_tree(info.clone(), extra.actor_id, extra.executor_id, stream);

        // Schema check
        let stream = schema_check::schema_check(info.clone(), stream);
        // Epoch check
        let stream = epoch_check::epoch_check(info.clone(), stream);

        // Epoch provide
        let stream = epoch_provide::epoch_provide(stream);

        // Trace
        let stream = trace::trace(
            enable_executor_row_count,
            info.clone(),
            extra.input_pos,
            extra.actor_id,
            extra.executor_id,
            extra.metrics.clone(),
            stream,
        );

        if cfg!(debug_assertions) {
            Self::wrap_debug(enable_executor_row_count, info, extra, stream).boxed()
        } else {
            stream.boxed()
        }
    }
}

impl Executor for WrapperExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let info = Arc::new(self.input.info());
        Self::wrap(
            self.enable_executor_row_count,
            info,
            self.extra,
            self.input.execute(),
        )
        .boxed()
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        let info = Arc::new(self.input.info());
        Self::wrap(
            self.enable_executor_row_count,
            info,
            self.extra,
            self.input.execute_with_epoch(epoch),
        )
        .boxed()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        self.input.identity()
    }
}

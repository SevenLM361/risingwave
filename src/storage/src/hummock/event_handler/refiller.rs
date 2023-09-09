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

use std::collections::{HashSet, VecDeque};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};

use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, HistogramVec,
    IntGauge, Registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

pub static GLOBAL_CACHE_REFILL_METRICS: LazyLock<CacheRefillMetrics> =
    LazyLock::new(|| CacheRefillMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct CacheRefillMetrics {
    pub data_refill_duration: HistogramVec,

    pub data_refill_duration_admitted: Histogram,
    pub data_refill_duration_rejected: Histogram,
    pub data_refill_filtered_total: GenericCounter<AtomicU64>,

    pub meta_refill_duration: Histogram,

    pub refill_queue_total: IntGauge,
}

impl CacheRefillMetrics {
    pub fn new(registry: &Registry) -> Self {
        let data_refill_duration = register_histogram_vec_with_registry!(
            "data_refill_duration",
            "data refill duration",
            &["op"],
            registry,
        )
        .unwrap();
        let data_refill_duration_admitted = data_refill_duration
            .get_metric_with_label_values(&["admitted"])
            .unwrap();
        let data_refill_duration_rejected = data_refill_duration
            .get_metric_with_label_values(&["rejected"])
            .unwrap();

        let data_refill_filtered_total = register_int_counter_with_registry!(
            "data_refill_filtered_total",
            "data refill filtered total",
            registry,
        )
        .unwrap();

        let meta_refill_duration = register_histogram_with_registry!(
            "meta_refill_duration",
            "meta refill duration",
            registry,
        )
        .unwrap();

        let refill_queue_total = register_int_gauge_with_registry!(
            "refill_queue_total",
            "refill queue total",
            registry,
        )
        .unwrap();

        Self {
            data_refill_duration,
            data_refill_duration_admitted,
            data_refill_duration_rejected,
            data_refill_filtered_total,
            meta_refill_duration,
            refill_queue_total,
        }
    }
}

#[derive(Debug)]
pub struct CacheRefillConfig {
    pub timeout: Duration,
    pub data_refill_levels: HashSet<u32>,
    pub concurrency: usize,
}

struct Item {
    handle: JoinHandle<()>,
    event: CacheRefillerEvent,
}

/// A cache refiller for hummock data.
pub struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    context: CacheRefillContext,
}

impl CacheRefiller {
    pub fn new(config: CacheRefillConfig, sstable_store: SstableStoreRef) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        Self {
            queue: VecDeque::new(),
            context: CacheRefillContext {
                config,
                concurrency,
                sstable_store,
            },
        }
    }

    pub fn start_cache_refill(
        &mut self,
        deltas: Vec<SstDeltaInfo>,
        pinned_version: Arc<PinnedVersion>,
        new_pinned_version: PinnedVersion,
    ) {
        let task = CacheRefillTask {
            deltas,
            context: self.context.clone(),
        };
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let handle = tokio::spawn(task.run());
        let item = Item { handle, event };
        self.queue.push_back(item);
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.add(1);
    }

    pub fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }

    pub fn next_event(&mut self) -> NextCacheRefillerEvent<'_> {
        NextCacheRefillerEvent { refiller: self }
    }
}

pub struct NextCacheRefillerEvent<'a> {
    refiller: &'a mut CacheRefiller,
}

impl<'a> Future for NextCacheRefillerEvent<'a> {
    type Output = CacheRefillerEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let refiller = &mut self.deref_mut().refiller;

        if let Some(item) = refiller.queue.front_mut() {
            ready!(item.handle.poll_unpin(cx)).unwrap();
            let item = refiller.queue.pop_front().unwrap();
            GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.sub(1);
            return Poll::Ready(item.event);
        }
        Poll::Pending
    }
}

pub struct CacheRefillerEvent {
    pub pinned_version: Arc<PinnedVersion>,
    pub new_pinned_version: PinnedVersion,
}

#[derive(Clone)]
struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
}

pub struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    async fn run(self) {
        let tasks = self
            .deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!("meta cache refill error: {:?}", e);
                            return;
                        }
                    };
                    Self::data_cache_refill(&context, delta, holders).await;
                }
            })
            .collect_vec();
        let future = join_all(tasks);

        let _ = tokio::time::timeout(self.context.config.timeout, future).await;
    }

    async fn meta_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let stats = StoreLocalStatistic::default();
        let tasks = delta
            .insert_sst_infos
            .iter()
            .map(|info| async {
                let _timer = GLOBAL_CACHE_REFILL_METRICS
                    .meta_refill_duration
                    .start_timer();
                context.sstable_store.sstable_syncable(info, &stats).await
            })
            .collect_vec();
        let res = try_join_all(tasks).await?;
        let holders = res.into_iter().map(|(holder, _, _)| holder).collect_vec();
        Ok(holders)
    }

    async fn data_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let now = Instant::now();

        // return if data file cache is disabled
        let Some(filter) = context.sstable_store.data_file_cache_refill_filter() else {
            return;
        };

        // return if no data to refill
        if delta.insert_sst_infos.is_empty() || delta.delete_sst_object_ids.is_empty() {
            return;
        }

        // return if filtered
        if !context
            .config
            .data_refill_levels
            .contains(&delta.insert_sst_level)
            || !delta
                .delete_sst_object_ids
                .iter()
                .any(|id| filter.contains(id))
        {
            let blocks = holders
                .iter()
                .map(|meta| meta.value().block_count() as u64)
                .sum();
            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_filtered_total
                .inc_by(blocks);
            return;
        }

        let mut tasks = vec![];
        for sst_info in &holders {
            for block_index in 0..sst_info.value().block_count() {
                let meta = sst_info.value();
                let mut stat = StoreLocalStatistic::default();
                let task = async move {
                    let permit = context.concurrency.acquire().await.unwrap();
                    match context
                        .sstable_store
                        .may_fill_data_file_cache(meta, block_index, &mut stat)
                        .await
                    {
                        Ok(true) => GLOBAL_CACHE_REFILL_METRICS
                            .data_refill_duration_admitted
                            .observe(now.elapsed().as_secs_f64()),
                        Ok(false) => GLOBAL_CACHE_REFILL_METRICS
                            .data_refill_duration_rejected
                            .observe(now.elapsed().as_secs_f64()),
                        Err(e) => {
                            tracing::warn!("data cache refill error: {:?}", e);
                        }
                    }
                    drop(permit);
                };
                tasks.push(task);
            }
        }

        join_all(tasks).await;
    }
}

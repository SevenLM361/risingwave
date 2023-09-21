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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc};
use std::time::Instant;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{can_concat, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::{CompactTask, SstableInfo};

use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::compactor::{Compactor, CompactorContext, RemoteBuilderFactory, TaskConfig};
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::sstable_store::{BlockStream, SstableStoreRef};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    Block, BlockBuilder, BlockHolder, BlockIterator, BlockMeta, BlockedXor16FilterBuilder,
    CachePolicy, CompressionAlgorithm, GetObjectId, HummockResult, SstableBuilderOptions,
    StreamingSstableWriterFactory, TableHolder,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

/// Iterates over the KV-pairs of an SST while downloading it.
pub struct BlockStreamIterator {
    /// The downloading stream.
    block_stream: BlockStream,

    next_block_index: usize,

    /// For key sanity check of divided SST and debugging
    sstable: TableHolder,
    iter: Option<BlockIterator>,
    task_progress: Arc<TaskProgress>,
}

impl BlockStreamIterator {
    // We have to handle two internal iterators.
    //   `block_stream`: iterates over the blocks of the table.
    //     `block_iter`: iterates over the KV-pairs of the current block.
    // These iterators work in different ways.

    // BlockIterator works as follows: After new(), we call seek(). That brings us
    // to the first element. Calling next() then brings us to the second element and does not
    // return anything.

    // BlockStream follows a different approach. After new(), we do not seek, instead next()
    // returns the first value.

    /// Initialises a new [`BlockStreamIterator`] which iterates over the given [`BlockStream`].
    /// The iterator reads at most `max_block_count` from the stream.
    pub fn new(
        sstable: TableHolder,
        block_stream: BlockStream,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        Self {
            block_stream,
            next_block_index: 0,
            sstable,
            iter: None,
            task_progress,
        }
    }

    /// Wrapper function for `self.block_stream.next()` which allows us to measure the time needed.
    async fn download_next_block(&mut self) -> HummockResult<Option<(Bytes, Vec<u8>, BlockMeta)>> {
        let (data, meta) = match self.block_stream.next().await? {
            None => return Ok(None),
            Some(ret) => ret,
        };
        let filter_block = self
            .sstable
            .value()
            .filter_reader
            .get_block_raw_filter(self.next_block_index);
        self.next_block_index += 1;
        Ok(Some((data, filter_block, meta)))
    }

    fn init_block_iter(&mut self, buf: Bytes, uncompressed_capacity: usize) -> HummockResult<()> {
        let block = Block::decode(buf, uncompressed_capacity)?;
        let mut iter = BlockIterator::new(BlockHolder::from_owned_block(Box::new(block)));
        iter.seek_to_first();
        self.iter = Some(iter);
        Ok(())
    }

    fn next_block_smallest(&self) -> &[u8] {
        self.sstable.value().meta.block_metas[self.next_block_index]
            .smallest_key
            .as_ref()
    }

    fn next_block_largest(&self) -> &[u8] {
        if self.next_block_index + 1 < self.sstable.value().meta.block_metas.len() {
            self.sstable.value().meta.block_metas[self.next_block_index + 1]
                .smallest_key
                .as_ref()
        } else {
            self.sstable.value().meta.largest_key.as_ref()
        }
    }

    fn current_block_largest(&self) -> Vec<u8> {
        if self.next_block_index < self.sstable.value().meta.block_metas.len() {
            let mut largest_key = FullKey::decode(
                self.sstable.value().meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            );
            // do not include this key because it is the smallest key of next block.
            largest_key.epoch = HummockEpoch::MAX;
            largest_key.encode()
        } else {
            self.sstable.value().meta.largest_key.clone()
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        match self.iter.as_ref() {
            Some(iter) => iter.key(),
            None => FullKey::decode(
                self.sstable.value().meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            ),
        }
    }

    fn is_valid(&self) -> bool {
        self.iter.is_some() || self.next_block_index < self.sstable.value().meta.block_metas.len()
    }
}

impl Drop for BlockStreamIterator {
    fn drop(&mut self) {
        self.task_progress
            .num_pending_read_io
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<BlockStreamIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    sstables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    task_progress: Arc<TaskProgress>,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        sst_infos: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            sstables: sst_infos,
            sstable_store,
            task_progress,
            stats: StoreLocalStatistic::default(),
        }
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0).await
    }

    pub async fn next_sstable(&mut self) -> HummockResult<()> {
        self.seek_idx(self.cur_idx + 1).await
    }

    pub fn current_sstable(&mut self) -> &mut BlockStreamIterator {
        self.sstable_iter.as_mut().unwrap()
    }

    pub fn estimate_key_count(&self, uncompressed_block_size: u64) -> (u64, u64) {
        let total_size = self.sstables[self.cur_idx].uncompressed_file_size;
        if total_size == 0 {
            return (0, 0);
        }
        // use ratio to avoid multiply overflow
        let ratio = uncompressed_block_size * 10000 / total_size;
        (
            self.sstables[self.cur_idx].stale_key_count * ratio / 10000,
            self.sstables[self.cur_idx].total_key_count * ratio / 10000,
        )
    }

    pub async fn init_block_iter(&mut self) -> HummockResult<()> {
        if let Some(sstable) = self.sstable_iter.as_mut() {
            if sstable.iter.is_some() {
                return Ok(());
            }
            let (buf, _, meta) = sstable.download_next_block().await?.unwrap();
            sstable.init_block_iter(buf, meta.uncompressed_size as usize)?;
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.cur_idx < self.sstables.len()
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(&mut self, idx: usize) -> HummockResult<()> {
        self.sstable_iter.take();
        self.cur_idx = idx;
        if self.cur_idx < self.sstables.len() {
            let sstable_info = &self.sstables[self.cur_idx];
            let sstable = self
                .sstable_store
                .sstable(sstable_info, &mut self.stats)
                .verbose_instrument_await("stream_iter_sstable")
                .await?;
            let stats_ptr = self.stats.remote_io_time.clone();
            let now = Instant::now();
            self.task_progress
                .num_pending_read_io
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let block_stream = self
                .sstable_store
                .get_stream_by_position(sstable.value().id, 0, &sstable.value().meta.block_metas)
                .verbose_instrument_await("stream_iter_get_stream")
                .await?;

            // Determine time needed to open stream.
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);

            let sstable_iter =
                BlockStreamIterator::new(sstable, block_stream, self.task_progress.clone());
            self.sstable_iter = Some(sstable_iter);
        }
        Ok(())
    }
}

pub struct CompactorRunner {
    left: Box<ConcatSstableIterator>,
    right: Box<ConcatSstableIterator>,
    task_id: u64,
    executor: CompactTaskExecutor<
        RemoteBuilderFactory<StreamingSstableWriterFactory, BlockedXor16FilterBuilder>,
    >,
    compression_algorithm: CompressionAlgorithm,
    metrics: Arc<CompactorMetrics>,
    task_progress: Arc<TaskProgress>,
}

impl CompactorRunner {
    pub fn new(
        context: CompactorContext,
        task: CompactTask,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        object_id_getter: Box<dyn GetObjectId>,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        let compression_algorithm: CompressionAlgorithm = task.compression_algorithm.into();
        options.compression_algorithm = compression_algorithm;
        options.capacity = task.target_file_size as usize;
        let get_id_time = Arc::new(AtomicU64::new(0));

        let key_range = KeyRange::inf();

        let task_config = TaskConfig {
            key_range,
            cache_policy: CachePolicy::NotFill,
            gc_delete_keys: task.gc_delete_keys,
            watermark: task.watermark,
            stats_target_table_ids: Some(HashSet::from_iter(task.existing_table_ids.clone())),
            task_type: task.task_type(),
            is_target_l0_or_lbase: task.target_level == 0 || task.target_level == task.base_level,
            split_by_table: task.split_by_state_table,
            split_weight_by_vnode: task.split_weight_by_vnode,
            use_block_based_filter: true,
        };
        let factory = StreamingSstableWriterFactory::new(context.sstable_store.clone());
        let builder_factory = RemoteBuilderFactory::<_, BlockedXor16FilterBuilder> {
            object_id_getter,
            limiter: context.memory_limiter.clone(),
            options,
            policy: task_config.cache_policy,
            remote_rpc_cost: get_id_time,
            filter_key_extractor,
            sstable_writer_factory: factory,
            _phantom: PhantomData,
        };
        let sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            context.compactor_metrics.clone(),
            Some(task_progress.clone()),
            task_config.is_target_l0_or_lbase,
            task_config.split_by_table,
            task_config.split_weight_by_vnode,
        );
        assert_eq!(task.input_ssts.len(), 2);
        let left = Box::new(ConcatSstableIterator::new(
            task.input_ssts[0].table_infos.clone(),
            context.sstable_store.clone(),
            task_progress.clone(),
        ));
        let right = Box::new(ConcatSstableIterator::new(
            task.input_ssts[1].table_infos.clone(),
            context.sstable_store,
            task_progress.clone(),
        ));

        Self {
            executor: CompactTaskExecutor::new(sst_builder, task_config),
            left,
            right,
            task_id: task.task_id,
            metrics: context.compactor_metrics.clone(),
            compression_algorithm,
            task_progress,
        }
    }

    pub async fn run(mut self) -> HummockResult<Vec<LocalSstableInfo>> {
        self.left.rewind().await?;
        self.right.rewind().await?;
        let mut skip_raw_block_count = 0;
        let mut skip_raw_block_size = 0;
        while self.left.is_valid() && self.right.is_valid() {
            let ret = self
                .left
                .current_sstable()
                .key()
                .cmp(&self.right.current_sstable().key());
            let (first, second) = if ret == Ordering::Less {
                (&mut self.left, &mut self.right)
            } else {
                (&mut self.right, &mut self.left)
            };
            assert!(ret != Ordering::Equal);
            if first.current_sstable().iter.is_none() {
                let right_key = second.current_sstable().key();
                while first.current_sstable().is_valid() {
                    let full_key = FullKey::decode(first.current_sstable().next_block_largest());
                    // the full key may be either Excluded key or Included key, so we do not allow
                    // they equals.
                    if full_key.user_key.ge(&right_key.user_key) {
                        break;
                    }
                    let smallest_key =
                        FullKey::decode(first.current_sstable().next_block_smallest());
                    if self
                        .executor
                        .last_key
                        .user_key
                        .as_ref()
                        .eq(&smallest_key.user_key)
                    {
                        // If the last key is delete tombstone, we can not append the origin block
                        // because it would cause a deleted key could be see by user again.
                        break;
                    }
                    let smallest_key = smallest_key.to_vec();

                    let (mut block, filter_data, mut meta) = first
                        .current_sstable()
                        .download_next_block()
                        .await?
                        .unwrap();
                    let algorithm = Block::get_algorithm(&block)?;
                    if algorithm == CompressionAlgorithm::None
                        && algorithm != self.compression_algorithm
                    {
                        block = BlockBuilder::compress_block(block, self.compression_algorithm)?;
                        meta.len = block.len() as u32;
                    }

                    let largest_key = first.current_sstable().current_block_largest();
                    let block_len = block.len() as u64;

                    if self
                        .executor
                        .builder
                        .add_raw_block(block, filter_data, smallest_key, largest_key, meta)
                        .await?
                    {
                        skip_raw_block_size += block_len;
                        skip_raw_block_count += 1;
                    }
                    self.executor.clear();
                }
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                    continue;
                }
                first.init_block_iter().await?;
            }

            let target_key = second.current_sstable().key();
            let iter = first.sstable_iter.as_mut().unwrap().iter.as_mut().unwrap();
            self.executor.run(iter, target_key).await?;
            if !iter.is_valid() {
                first.sstable_iter.as_mut().unwrap().iter.take();
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                }
            }
        }
        let rest_data = if !self.left.is_valid() {
            &mut self.right
        } else {
            &mut self.left
        };
        if rest_data.is_valid() {
            // compact rest keys of the current block.
            let sstable_iter = rest_data.sstable_iter.as_mut().unwrap();
            let target_key = FullKey::decode(&sstable_iter.sstable.value().meta.largest_key);
            if let Some(iter) = sstable_iter.iter.as_mut() {
                self.executor.run(iter, target_key).await?;
                assert!(!iter.is_valid());
            }
            sstable_iter.iter.take();
        }

        while rest_data.is_valid() {
            let mut sstable_iter = rest_data.sstable_iter.take().unwrap();
            while sstable_iter.is_valid() {
                let smallest_key = FullKey::decode(sstable_iter.next_block_smallest()).to_vec();
                let (block, filter_data, block_meta) =
                    sstable_iter.download_next_block().await?.unwrap();
                let largest_key = sstable_iter.current_block_largest();
                let block_len = block.len() as u64;
                if self
                    .executor
                    .builder
                    .add_raw_block(block, filter_data, smallest_key, largest_key, block_meta)
                    .await?
                {
                    skip_raw_block_count += 1;
                    skip_raw_block_size += block_len;
                }
            }
            rest_data.next_sstable().await?;
        }
        let mut total_read_bytes = 0;
        for sst in &self.left.sstables {
            total_read_bytes += sst.file_size;
        }
        for sst in &self.right.sstables {
            total_read_bytes += sst.file_size;
        }
        self.metrics
            .write_build_l0_bytes
            .inc_by(skip_raw_block_size);
        tracing::info!(
            "OPTIMIZATION: skip {} blocks for task-{}, optimize {}% data compression",
            skip_raw_block_count,
            self.task_id,
            skip_raw_block_size * 100 / total_read_bytes,
        );

        let outputs = self.executor.builder.finish().await?;
        let ssts = Compactor::report_progress(
            self.metrics.clone(),
            Some(self.task_progress.clone()),
            outputs,
            false,
        )
        .await?;
        let sst_infos = ssts.iter().map(|sst| sst.sst_info.clone()).collect_vec();
        assert!(can_concat(&sst_infos));
        Ok(ssts)
    }
}

pub struct CompactTaskExecutor<F: TableBuilderFactory> {
    last_key: FullKey<Vec<u8>>,
    watermark_can_see_last_key: bool,
    builder: CapacitySplitTableBuilder<F>,
    task_config: TaskConfig,
    last_key_is_delete: bool,
}

impl<F: TableBuilderFactory> CompactTaskExecutor<F> {
    pub fn new(builder: CapacitySplitTableBuilder<F>, task_config: TaskConfig) -> Self {
        Self {
            builder,
            task_config,
            last_key: FullKey::default(),
            watermark_can_see_last_key: false,
            last_key_is_delete: false,
        }
    }

    fn clear(&mut self) {
        if !self.last_key.is_empty() {
            self.last_key = FullKey::default();
        }
        self.watermark_can_see_last_key = false;
        self.last_key_is_delete = false;
    }

    pub async fn run(
        &mut self,
        iter: &mut BlockIterator,
        target_key: FullKey<&[u8]>,
    ) -> HummockResult<()> {
        while iter.is_valid() && iter.key().le(&target_key) {
            let is_new_user_key =
                !self.last_key.is_empty() && iter.key().user_key != self.last_key.user_key.as_ref();
            let mut drop = false;
            let epoch = iter.key().epoch;
            let value = HummockValue::from_slice(iter.value()).unwrap();
            if is_new_user_key || self.last_key.is_empty() {
                self.last_key.set(iter.key());
                self.watermark_can_see_last_key = false;
                self.last_key_is_delete = false;
            }
            if epoch <= self.task_config.watermark
                && self.task_config.gc_delete_keys
                && value.is_delete()
            {
                drop = true;
                self.last_key_is_delete = true;
            } else if epoch < self.task_config.watermark && self.watermark_can_see_last_key {
                drop = true;
            }

            if epoch <= self.task_config.watermark {
                self.watermark_can_see_last_key = true;
            }

            if drop {
                iter.next();
                continue;
            }
            self.builder
                .add_full_key(iter.key(), value, is_new_user_key)
                .await?;
            iter.next();
        }
        Ok(())
    }
}

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

use std::alloc::{Allocator, Global};
use std::borrow::Borrow;
use std::cmp::min;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::{DefaultHasher, IndexedLruCache, KeyRef};
use prometheus::IntGauge;
use risingwave_common::estimate_size::EstimateSize;

use super::{MutGuard, UnsafeMutGuard, REPORT_SIZE_EVERY_N_KB_CHANGE};
use crate::common::metrics::MetricsInfo;

/// The managed cache is a lru cache that bounds the memory usage by epoch.
/// Should be used with `GlobalMemoryManager`.
pub struct ManagedIndexedLruCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    inner: IndexedLruCache<K, V, S, A>,
    /// The entry with epoch less than water should be evicted.
    /// Should only be updated by the `GlobalMemoryManager`.
    watermark_epoch: Arc<AtomicU64>,
    /// The heap size of keys/values
    kv_heap_size: usize,
    /// The metrics of memory usage
    memory_usage_metrics: Option<IntGauge>,
    // Metrics info
    metrics_info: Option<MetricsInfo>,
    /// The size reported last time
    last_reported_size_bytes: usize,
    size_limit: usize,
}

impl<K, V, S, A: Clone + Allocator> Drop for ManagedIndexedLruCache<K, V, S, A> {
    fn drop(&mut self) {
        if let Some(metrics) = &self.memory_usage_metrics {
            metrics.set(0.into());
        }
        if let Some(info) = &self.metrics_info {
            info.metrics
                .stream_memory_usage
                .remove_label_values(&[&info.table_id, &info.actor_id, &info.desc])
                .unwrap();
        }
    }
}

impl<K: Hash + Eq + EstimateSize, V: EstimateSize, S: BuildHasher, A: Clone + Allocator>
    ManagedIndexedLruCache<K, V, S, A>
{
    pub fn new_inner(
        inner: IndexedLruCache<K, V, S, A>,
        watermark_epoch: Arc<AtomicU64>,
        metrics_info: Option<MetricsInfo>,
    ) -> Self {
        let memory_usage_metrics = metrics_info.as_ref().map(|info| {
            info.metrics.stream_memory_usage.with_label_values(&[
                &info.table_id,
                &info.actor_id,
                &info.desc,
            ])
        });

        Self {
            inner,
            watermark_epoch,
            kv_heap_size: 0,
            memory_usage_metrics,
            metrics_info,
            last_reported_size_bytes: 0,
            size_limit: 0,
        }
    }

    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        if self.size_limit == 0 {
            let epoch = self.watermark_epoch.load(Ordering::Relaxed);
            self.evict_by_epoch(epoch);
        } else {
            self.evict_by_size();
        }
    }

    /// Evict epochs lower than the watermark, except those entry which touched in this epoch
    pub fn evict_except_cur_epoch(&mut self) {
        let epoch = self.watermark_epoch.load(Ordering::Relaxed);
        let epoch = min(epoch, self.inner.current_epoch());
        self.evict_by_epoch(epoch);
    }

    /// Evict epochs lower than the watermark
    fn evict_by_epoch(&mut self, epoch: u64) {
        while let Some((key_op, value)) = self.inner.pop_lru_by_epoch(epoch) {
            if let Some(key) = key_op {
                self.kv_heap_size_dec(key.estimated_size() + value.estimated_size());
            } else {
                self.kv_heap_size_dec(value.estimated_size());
            }
        }
    }

    fn evict_by_size(&mut self) {
        while self.kv_heap_size > self.size_limit {
            if let Some((key_op, value)) = self.inner.pop_lru_once() {
                if let Some(key) = key_op {
                    self.kv_heap_size_dec(key.estimated_size() + value.estimated_size());
                } else {
                    self.kv_heap_size_dec(value.estimated_size());
                }
            } else {
                break;
            }
        }
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        self.inner.update_epoch(epoch);
    }

    pub fn update_size_limit(&mut self, size_limit: usize) {
        self.size_limit = size_limit;
        self.evict_by_size();
    }

    pub fn current_epoch(&mut self) -> u64 {
        self.inner.current_epoch()
    }

    pub fn bucket_count(&self) -> usize {
        self.inner.bucket_count()
    }

    pub fn ghost_bucket_count(&self) -> usize {
        self.inner.ghost_bucket_count()
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    // pub fn values(&self) -> impl Iterator<Item = &V> {
    //     self.inner.iter().map(|(_k, v)| v)
    // }

    pub fn put(&mut self, k: K, v: V) {
        let key_size = k.estimated_size();
        self.kv_heap_size_inc(key_size + v.estimated_size());
        let old_val = self.inner.put(k, v);
        if let Some(old_val) = &old_val {
            self.kv_heap_size_dec(key_size + old_val.estimated_size());
        }
    }

    pub fn put_sample(
        &mut self,
        k: K,
        v: V,
        is_update: bool,
        return_distance: bool,
    ) -> Option<(u32, bool)> {
        let key_size = k.estimated_size();
        self.kv_heap_size_inc(key_size + v.estimated_size());
        let (old_val, distance) = self.inner.put_sample(k, v, is_update, return_distance);
        if let Some(old_val) = &old_val {
            self.kv_heap_size_dec(key_size + old_val.estimated_size());
        }
        distance
    }

    pub fn get_mut(&mut self, k: &K, check_ghost: bool) -> Option<MutGuard<'_, V>> {
        let v = self.inner.get_mut(k, check_ghost);
        v.map(|inner| {
            MutGuard::new(
                inner,
                &mut self.kv_heap_size,
                &mut self.last_reported_size_bytes,
                &mut self.memory_usage_metrics,
            )
        })
    }

    pub fn get_mut_unsafe(&mut self, k: &K, check_ghost: bool) -> Option<UnsafeMutGuard<V>> {
        let v = self.inner.get_mut(k, check_ghost);
        v.map(|inner| {
            UnsafeMutGuard::new(
                inner,
                &mut self.kv_heap_size,
                &mut self.last_reported_size_bytes,
                &mut self.memory_usage_metrics,
            )
        })
    }

    // pub fn get<Q>(&mut self, k: &Q, check_ghost: bool) -> Option<&V>
    // where
    //     KeyRef<K>: Borrow<Q>,
    //     Q: Hash + Eq + ?Sized,
    // {
    //     self.inner.get(k, check_ghost)
    // }

    pub fn peek_mut(&mut self, k: &K) -> Option<MutGuard<'_, V>> {
        let v = self.inner.peek_mut(k);
        v.map(|inner| {
            MutGuard::new(
                inner,
                &mut self.kv_heap_size,
                &mut self.last_reported_size_bytes,
                &mut self.memory_usage_metrics,
            )
        })
    }

    // pub fn push(&mut self, k: K, v: V) -> Option<(K, V)> {
    //     self.kv_heap_size_inc(k.estimated_size() + v.estimated_size());

    //     let old_kv = self.inner.push(k, v);

    //     if let Some((old_key, old_val)) = &old_kv {
    //         self.kv_heap_size_dec(old_key.estimated_size() + old_val.estimated_size());
    //     }
    //     old_kv
    // }

    pub fn contains<Q>(&self, k: &Q, check_ghost: bool) -> bool
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains(k, check_ghost)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn len_with_ghost(&self) -> usize {
        self.inner.len() + self.inner.ghost_len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    fn kv_heap_size_inc(&mut self, size: usize) {
        self.kv_heap_size = self.kv_heap_size.saturating_add(size);
        self.report_memory_usage();
    }

    fn kv_heap_size_dec(&mut self, size: usize) {
        self.kv_heap_size = self.kv_heap_size.saturating_sub(size);
        self.report_memory_usage();
    }

    fn report_memory_usage(&mut self) -> bool {
        if self.kv_heap_size.abs_diff(self.last_reported_size_bytes)
            > REPORT_SIZE_EVERY_N_KB_CHANGE << 10
        {
            if let Some(metrics) = self.memory_usage_metrics.as_ref() {
                metrics.set(self.kv_heap_size as _);
            }
            self.last_reported_size_bytes = self.kv_heap_size;
            true
        } else {
            false
        }
    }
}

pub fn new_indexed_with_hasher_in<
    K: Hash + Eq + EstimateSize,
    V: EstimateSize,
    S: BuildHasher,
    A: Clone + Allocator,
>(
    watermark_epoch: Arc<AtomicU64>,
    metrics_info: MetricsInfo,
    hasher: S,
    alloc: A,
    ghost_cap: usize,
    update_interval: u32,
    ghost_bucket_count: usize,
) -> ManagedIndexedLruCache<K, V, S, A> {
    ManagedIndexedLruCache::new_inner(
        IndexedLruCache::unbounded_with_hasher_in(
            hasher,
            alloc,
            ghost_cap,
            update_interval,
            ghost_bucket_count,
        ),
        watermark_epoch,
        Some(metrics_info),
    )
}

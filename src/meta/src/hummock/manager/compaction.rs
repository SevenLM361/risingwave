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

use std::collections::BTreeMap;

use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId};
use risingwave_pb::hummock::{CompactStatus as PbCompactStatus, CompactTaskAssignment};

use crate::hummock::compaction::CompactStatus;
use crate::hummock::manager::read_lock;
use crate::hummock::HummockManager;

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,

    pub deterministic_mode: bool,
}

impl HummockManager {
    #[named]
    pub async fn get_assigned_compact_task_num(&self) -> u64 {
        read_lock!(self, compaction)
            .await
            .compact_task_assignment
            .len() as u64
    }

    #[named]
    pub async fn list_all_tasks_ids(&self) -> Vec<HummockCompactionTaskId> {
        let compaction = read_lock!(self, compaction).await;

        compaction
            .compaction_statuses
            .iter()
            .flat_map(|(_, cs)| {
                cs.level_handlers
                    .iter()
                    .flat_map(|lh| lh.pending_tasks_ids())
            })
            .collect_vec()
    }

    #[named]
    pub async fn list_compaction_status(
        &self,
    ) -> (Vec<PbCompactStatus>, Vec<CompactTaskAssignment>) {
        let compaction = read_lock!(self, compaction).await;
        (
            compaction.compaction_statuses.values().map_into().collect(),
            compaction
                .compact_task_assignment
                .values()
                .cloned()
                .collect(),
        )
    }
}

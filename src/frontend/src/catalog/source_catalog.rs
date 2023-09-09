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

use risingwave_common::catalog::{ColumnCatalog, SourceVersionId};
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::{PbSource, StreamSourceInfo, WatermarkDesc};

use super::{ColumnId, ConnectionId, DatabaseId, OwnedByUserCatalog, SchemaId, SourceId};
use crate::catalog::TableId;
use crate::user::UserId;
use crate::WithOptions;

/// This struct `SourceCatalog` is used in frontend.
/// Compared with `PbSource`, it only maintains information used during optimization.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceCatalog {
    pub id: SourceId,
    pub name: String,
    pub columns: Vec<ColumnCatalog>,
    pub pk_col_ids: Vec<ColumnId>,
    pub append_only: bool,
    pub owner: UserId,
    pub info: StreamSourceInfo,
    pub row_id_index: Option<usize>,
    pub properties: BTreeMap<String, String>,
    pub watermark_descs: Vec<WatermarkDesc>,
    pub associated_table_id: Option<TableId>,
    pub definition: String,
    pub connection_id: Option<ConnectionId>,
    pub created_at_epoch: Option<Epoch>,
    pub initialized_at_epoch: Option<Epoch>,
    pub version: SourceVersionId,
}

impl SourceCatalog {
    /// Returns the SQL statement that can be used to create this source.
    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    pub fn to_prost(&self, schema_id: SchemaId, database_id: DatabaseId) -> PbSource {
        PbSource {
            id: self.id,
            schema_id,
            database_id,
            name: self.name.clone(),
            row_id_index: self.row_id_index.map(|idx| idx as _),
            columns: self.columns.iter().map(|c| c.to_protobuf()).collect(),
            pk_column_ids: self.pk_col_ids.iter().map(Into::into).collect(),
            properties: self.properties.clone().into_iter().collect(),
            owner: self.owner,
            info: Some(self.info.clone()),
            watermark_descs: self.watermark_descs.clone(),
            definition: self.definition.clone(),
            connection_id: self.connection_id,
            initialized_at_epoch: self.initialized_at_epoch.map(|x| x.0),
            created_at_epoch: self.created_at_epoch.map(|x| x.0),
            optional_associated_table_id: self
                .associated_table_id
                .map(|id| OptionalAssociatedTableId::AssociatedTableId(id.table_id)),
            version: self.version,
        }
    }

    /// Get a reference to the source catalog's version.
    pub fn version(&self) -> SourceVersionId {
        self.version
    }
}

impl From<&PbSource> for SourceCatalog {
    fn from(prost: &PbSource) -> Self {
        let id = prost.id;
        let name = prost.name.clone();
        let prost_columns = prost.columns.clone();
        let pk_col_ids = prost
            .pk_column_ids
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        let with_options = WithOptions::new(prost.properties.clone());
        let columns = prost_columns.into_iter().map(ColumnCatalog::from).collect();
        let row_id_index = prost.row_id_index.map(|idx| idx as _);

        let append_only = row_id_index.is_some();
        let owner = prost.owner;
        let watermark_descs = prost.get_watermark_descs().clone();

        let associated_table_id = prost
            .optional_associated_table_id
            .clone()
            .map(|id| match id {
                OptionalAssociatedTableId::AssociatedTableId(id) => id,
            });
        let version = prost.version;

        let connection_id = prost.connection_id;

        Self {
            id,
            name,
            columns,
            pk_col_ids,
            append_only,
            owner,
            info: prost.info.clone().unwrap(),
            row_id_index,
            properties: with_options.into_inner(),
            watermark_descs,
            associated_table_id: associated_table_id.map(|x| x.into()),
            definition: prost.definition.clone(),
            connection_id,
            created_at_epoch: prost.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: prost.initialized_at_epoch.map(Epoch::from),
            version,
        }
    }
}

impl OwnedByUserCatalog for SourceCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
